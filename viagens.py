# viagens.py - viagens a servico e diarias do Executivo federal (resumos gerados do arquivo da CGU)
# ============================================================
# Fonte: arquivo oficial "Viagens a Servico" da CGU / Portal da Transparencia
#   https://portaldatransparencia.gov.br/download-de-dados/viagens/AAAA  (zip de ~125 a 157 MB por ano, com
#   AAAA_Viagem.csv, AAAA_Passagem.csv, AAAA_Pagamento.csv e AAAA_Trecho.csv; cerca de 650 mil viagens por ano).
# O arquivo e grande demais para o app: scripts/atualizar_viagens.py roda no GitHub Actions, calcula os resumos
# abaixo e grava data/viagens_AAAA_*.csv (poucos MB). O app so le os resumos.
#
# Resumos por ano: orgao (por orgao solicitante), mes, viajantes (mais viagens e maior gasto), destinos e
# alertas (as viagens com mais pontos de atencao).
#
# PONTOS DE ATENCAO sao regras explicaveis que indicam onde olhar primeiro. Viagem a servico e gasto legitimo e
# previsto; nenhuma regra aqui prova irregularidade. CPF aparece como publicado pela fonte (mascarado).
# ============================================================

import io
import json
import os
import re
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

SIGILO = "Informações protegidas por sigilo"
RECURSOS = ["orgao", "mes", "viajantes", "destinos", "alertas"]
TOP_VIAJANTES = 1500
TOP_DESTINOS = 300
TOP_ALERTAS = 1000
LIMITE_FREQUENTE = 60       # viagens no ano
LIMITE_VALOR_ALTO = 30_000.0
LIMITE_VALOR_MUITO_ALTO = 100_000.0

COLS_VIAGEM = {
    "Identificador do processo de viagem": "id", "Número da Proposta (PCDP)": "pcdp", "Situação": "situacao",
    "Viagem Urgente": "urgente", "Justificativa Urgência Viagem": "just", "Código do órgão superior": "cod_sup",
    "Nome do órgão superior": "sup", "Código órgão solicitante": "cod_org", "Nome órgão solicitante": "org",
    "CPF viajante": "cpf", "Nome": "nome", "Cargo": "cargo", "Descrição Função": "funcao",
    "Período - Data de início": "ini", "Período - Data de fim": "fim", "Destinos": "destinos", "Motivo": "motivo",
    "Valor diárias": "diarias", "Valor passagens": "passagens", "Valor devolução": "devolucao",
    "Valor outros gastos": "outros",
}
COLS_TRECHO = {"Identificador do processo de viagem": "id", "Destino - País": "pais_destino"}
COLS_PASSAGEM = {"Identificador do processo de viagem": "id", "Data da emissão/compra": "compra"}


# ------------------------------------------------------------
# Leitura
# ------------------------------------------------------------

def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
                         errors="coerce").fillna(0.0)


def ler_csv(caminho, mapa: Dict[str, str]) -> pd.DataFrame:
    """Le so as colunas de `mapa` (nomes do arquivo, com espacos soltos) e renomeia."""
    cab = pd.read_csv(caminho, sep=";", encoding="latin-1", dtype=str, nrows=0).columns
    limpo = {c.strip(): c for c in cab}
    faltam = [c for c in mapa if c not in limpo]
    if faltam:
        raise ValueError(f"{os.path.basename(str(caminho))}: faltam colunas {faltam[:3]}")
    df = pd.read_csv(caminho, sep=";", encoding="latin-1", dtype=str, usecols=[limpo[c] for c in mapa])
    df.columns = [c.strip() for c in df.columns]
    return df.rename(columns=mapa)


def preparar_viagens(v: pd.DataFrame) -> pd.DataFrame:
    v = v.copy()
    for c in ("diarias", "passagens", "devolucao", "outros"):
        v[c] = _num(v[c])
    v["total"] = v["diarias"] + v["passagens"] + v["outros"] - v["devolucao"]
    v["dt_ini"] = pd.to_datetime(v["ini"], format="%d/%m/%Y", errors="coerce")
    v["dt_fim"] = pd.to_datetime(v["fim"], format="%d/%m/%Y", errors="coerce")
    v["dias"] = ((v["dt_fim"] - v["dt_ini"]).dt.days + 1).clip(lower=0).fillna(0).astype(int)
    v["mes"] = v["dt_ini"].dt.month.fillna(0).astype(int)
    v["eh_urgente"] = v["urgente"].str.upper().eq("SIM")
    v["realizada"] = v["situacao"].str.lower().eq("realizada")
    v["sigilo"] = v["nome"].fillna("").str.contains("sigilo", case=False)
    for c in ("just", "cpf", "nome", "cargo", "funcao", "destinos", "motivo", "sup", "org"):
        v[c] = v[c].fillna("")
    return v


def viagens_internacionais(trechos: pd.DataFrame) -> set:
    d = trechos[trechos["pais_destino"].fillna("Brasil").str.strip().ne("Brasil")
                & trechos["pais_destino"].fillna("").str.strip().ne("")
                & ~trechos["pais_destino"].fillna("").str.contains("sem informa", case=False)]
    return set(d["id"])


def antecedencia_compra(passagens: pd.DataFrame, viagens: pd.DataFrame) -> pd.Series:
    """Menor antecedencia (dias) entre a compra e o inicio da viagem, por viagem (id). Negativo = comprada depois."""
    p = passagens.copy()
    p["dt_compra"] = pd.to_datetime(p["compra"], format="%d/%m/%Y", errors="coerce")
    m = p.merge(viagens[["id", "dt_ini"]], on="id", how="inner")
    m["antec"] = (m["dt_ini"] - m["dt_compra"]).dt.days
    return m.dropna(subset=["antec"]).groupby("id")["antec"].min()


# ------------------------------------------------------------
# Pontos de atencao
# ------------------------------------------------------------

def avaliar(v: pd.DataFrame, intl: set, antec: pd.Series) -> pd.DataFrame:
    """Adiciona 'score' (0-100) e 'pontos' (texto). v ja passou por preparar_viagens."""
    v = v.copy()
    v["internacional"] = v["id"].isin(intl)
    v["antec_min"] = v["id"].map(antec)
    n_viag = v[~v["sigilo"]].groupby(["cpf", "nome"])["id"].transform("size")
    v["viagens_do_viajante"] = n_viag.reindex(v.index).fillna(0).astype(int)

    regras = [
        (v["total"] >= LIMITE_VALOR_MUITO_ALTO, 45, "valor total muito alto (≥ R$ 100 mil)"),
        ((v["total"] >= LIMITE_VALOR_ALTO) & (v["total"] < LIMITE_VALOR_MUITO_ALTO), 25, "valor total alto (≥ R$ 30 mil)"),
        (v["internacional"], 10, "viagem internacional"),
        (v["internacional"] & (v["total"] >= 20_000), 10, "internacional de valor alto (≥ R$ 20 mil)"),
        (~v["realizada"] & (v["total"] >= 1000) & (v["devolucao"] <= 0), 25,
         "viagem não realizada com valor pago e sem devolução registrada"),
        (v["eh_urgente"] & v["just"].str.strip().str.lower().isin(["", "sem informação", "sem informacao"]), 15,
         "viagem urgente sem justificativa"),
        (v["antec_min"] < 0, 15, "passagem emitida depois do início da viagem"),
        ((v["antec_min"] >= 0) & (v["antec_min"] < 3), 10, "passagem comprada com menos de 3 dias de antecedência"),
        (v["dias"] > 30, 10, "viagem com mais de 30 dias"),
        (v["viagens_do_viajante"] >= LIMITE_FREQUENTE, 10, f"viajante com {LIMITE_FREQUENTE} ou mais viagens no ano"),
    ]
    score = pd.Series(0, index=v.index)
    for cond, pts, _ in regras:
        score = score + cond.fillna(False).astype(int) * pts
    v["score"] = score.clip(upper=100)
    # texto so para quem pontuou (mais barato que montar para as 650 mil)
    pontos = pd.Series("", index=v.index)
    idx = v.index[v["score"] > 0]
    partes = [(cond.fillna(False), txt) for cond, _, txt in regras]
    pontos.loc[idx] = [" | ".join(t for c, t in partes if c.at[i]) for i in idx]
    v["pontos"] = pontos
    return v


# ------------------------------------------------------------
# Resumos
# ------------------------------------------------------------

def _soma(g) -> pd.Series:
    return pd.Series({
        "viagens": len(g), "realizadas": int(g["realizada"].sum()), "urgentes": int(g["eh_urgente"].sum()),
        "internacionais": int(g["internacional"].sum()), "diarias": g["diarias"].sum(),
        "passagens": g["passagens"].sum(), "outros": g["outros"].sum(), "devolucao": g["devolucao"].sum(),
        "total": g["total"].sum(),
        "viajantes": g.loc[~g["sigilo"], ["cpf", "nome"]].drop_duplicates().shape[0],
        "compra_tardia": int((g["antec_min"] < 5).sum()), "com_passagem": int(g["antec_min"].notna().sum()),
    })


def resumir(v: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """v avaliada -> dict com os 5 resumos."""
    orgao = (v.groupby(["cod_sup", "sup", "cod_org", "org"]).apply(_soma, include_groups=False)
             .reset_index().sort_values("total", ascending=False))
    mes = (v[v["mes"] > 0].groupby(["mes", "cod_sup", "sup"])[["diarias", "passagens", "outros", "devolucao", "total"]]
           .sum().join(v[v["mes"] > 0].groupby(["mes", "cod_sup", "sup"]).size().rename("viagens")).reset_index())

    ind = v[~v["sigilo"] & v["cpf"].ne("")]
    vi = ind.groupby(["cpf", "nome"]).agg(
        cargo=("cargo", lambda s: s.mode().iat[0] if len(s.mode()) else ""),
        funcao=("funcao", lambda s: s.mode().iat[0] if len(s.mode()) else ""),
        sup=("sup", lambda s: s.mode().iat[0] if len(s.mode()) else ""),
        viagens=("id", "size"), dias=("dias", "sum"), urgentes=("eh_urgente", "sum"),
        internacionais=("internacional", "sum"), diarias=("diarias", "sum"), passagens=("passagens", "sum"),
        total=("total", "sum")).reset_index()
    viajantes = pd.concat([vi.nlargest(TOP_VIAJANTES, "total"), vi.nlargest(TOP_VIAJANTES // 3, "viagens")]) \
        .drop_duplicates(["cpf", "nome"]).sort_values("total", ascending=False)

    dest = v[v["destinos"].ne("")].groupby("destinos").agg(
        viagens=("id", "size"), total=("total", "sum"), internacionais=("internacional", "sum")).reset_index()
    destinos = dest.nlargest(TOP_DESTINOS, "viagens")

    cols = ["id", "pcdp", "situacao", "eh_urgente", "sup", "org", "cpf", "nome", "cargo", "funcao", "ini", "fim", "dias",
            "destinos", "motivo", "diarias", "passagens", "outros", "devolucao", "total", "internacional", "antec_min",
            "viagens_do_viajante", "score", "pontos"]
    alertas = v[v["score"] > 0].nlargest(TOP_ALERTAS, ["score", "total"])[cols].copy()
    alertas["motivo"] = alertas["motivo"].str.slice(0, 200)
    return {"orgao": orgao, "mes": mes, "viajantes": viajantes, "destinos": destinos, "alertas": alertas}


def processar(pasta_csv: str, ano: int) -> Dict[str, pd.DataFrame]:
    """Pasta com os 4 CSVs do ano -> resumos. (Roda no GitHub Actions; usa ~1 GB de memoria.)"""
    base = os.path.join(pasta_csv, str(ano))
    if not os.path.isdir(base):
        base = pasta_csv
    v = preparar_viagens(ler_csv(os.path.join(base, f"{ano}_Viagem.csv"), COLS_VIAGEM))
    intl = viagens_internacionais(ler_csv(os.path.join(base, f"{ano}_Trecho.csv"), COLS_TRECHO))
    antec = antecedencia_compra(ler_csv(os.path.join(base, f"{ano}_Passagem.csv"), COLS_PASSAGEM), v)
    return resumir(avaliar(v, intl, antec))


# ------------------------------------------------------------
# Snapshot (data/viagens_AAAA_*.csv)
# ------------------------------------------------------------
PASTA_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
URL_DADOS_PADRAO = "https://raw.githubusercontent.com/LucasPin45/FiscalizaGov/main/data"
_TIPOS = {"cod_sup": str, "cod_org": str, "cpf": str, "id": str, "pcdp": str}


def salvar(resumos: Dict[str, pd.DataFrame], ano: int, pasta: str = PASTA_DADOS, agora: str = "",
           fonte: str = "") -> None:
    os.makedirs(pasta, exist_ok=True)
    for nome, df in resumos.items():
        df.to_csv(os.path.join(pasta, f"viagens_{ano}_{nome}.csv"), index=False, encoding="utf-8")
    caminho = os.path.join(pasta, "viagens_meta.json")
    try:
        with open(caminho, encoding="utf-8") as f:
            meta = json.load(f)
    except (OSError, ValueError):
        meta = {}
    meta[str(ano)] = {"gerado_em": agora, "fonte": fonte, "viagens": int(resumos["orgao"]["viagens"].sum())}
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1, sort_keys=True)


def ler(ano: int, base: str = "", timeout: int = 40) -> Optional[Tuple[Dict[str, pd.DataFrame], dict]]:
    """Le os resumos do ano (pasta local ou URL). None se algum arquivo faltar ou vier quebrado."""
    base = base or os.getenv("FISCALIZAGOV_DATA_URL", URL_DADOS_PADRAO)
    out: Dict[str, pd.DataFrame] = {}
    try:
        for nome in RECURSOS:
            if base.startswith("http"):
                r = requests.get(f"{base}/viagens_{ano}_{nome}.csv", timeout=timeout)
                if r.status_code != 200:
                    return None
                out[nome] = pd.read_csv(io.StringIO(r.text), dtype=_TIPOS, keep_default_na=False)
            else:
                out[nome] = pd.read_csv(os.path.join(base, f"viagens_{ano}_{nome}.csv"), dtype=_TIPOS,
                                        keep_default_na=False)
        if base.startswith("http"):
            m = requests.get(f"{base}/viagens_meta.json", timeout=timeout)
            meta = m.json() if m.status_code == 200 else {}
        else:
            with open(os.path.join(base, "viagens_meta.json"), encoding="utf-8") as f:
                meta = json.load(f)
    except (OSError, ValueError, requests.RequestException):
        return None
    if out["orgao"].empty or "viagens" not in out["orgao"].columns:
        return None
    return out, meta.get(str(ano), {})


def fmt_brl(v) -> str:
    try:
        return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return ""
