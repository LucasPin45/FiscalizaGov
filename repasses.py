# repasses.py - obras e programas financiados pela Uniao: convenios, contratos de repasse e similares
# ============================================================
# Fonte: Portal da Transparencia, /convenios (chave TRANSPARENCIA_API_KEY). Cada registro e um instrumento de
# repasse da Uniao a estado, municipio ou entidade (convenio, contrato de repasse, termo de compromisso,
# transferencia legal...), com valor, valor liberado, contrapartida, vigencia e situacao.
# Contratos de repasse (operados pela Caixa) e objetos de construcao/reforma/pavimentacao formam as "obras".
#
# Recorte: instrumentos cuja vigencia termina de 01/01 do ano anterior em diante (os em andamento e os
# encerrados recentemente). A API NAO tem total nem execucao fisica: o que medimos e o dinheiro (liberado x
# valor) e o prazo (vigencia x situacao). Atraso de obra de verdade exige execucao fisica (Obrasgov), cuja API
# antiga parou em julho de 2026.
#
# Pontos de atencao sao regras explicaveis para indicar onde olhar primeiro; nao provam irregularidade.
# Falha de coleta vira erro, nunca "UF sem repasse".
# ============================================================

import datetime as dt
import io
import json
import os
import re
import time
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from dou import ErroColeta

BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
POR_PAGINA = 15
PAUSA_S = 0.4
UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
       "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO"]
UF_DESTAQUE = "SC"
RECURSOS = ["uf", "sc", "municipios_sc", "alertas"]

COLUNAS = ["id", "numero", "objeto", "situacao", "grupo", "instrumento", "obra", "uf", "cod_ibge", "municipio",
           "convenente", "tipo_convenente", "cnpj", "orgao_superior", "orgao", "subfuncao", "valor", "liberado",
           "contrapartida", "inicio", "fim", "ultima_liberacao", "conclusao"]

_OBRA_PALAVRAS = re.compile(
    r"\b(obra|obras|construcao|construir|pavimenta\w*|reforma|ampliacao|drenagem|saneamento|esgot\w*|ponte|pontes|"
    r"escola|creche|quadra|ginasio|ubs|habitac\w*|moradia|calcamento|asfalt\w*|terraplen\w*|abastecimento de agua|"
    r"urbaniza\w*|recuperacao de|revitaliza\w*|reconstrucao|restauro|cobertura|muro de contencao|encosta|"
    r"unidade basica|hospital|aeroporto|rodovia|estrada|barragem|dragagem)\b")

ANDAMENTO = {"EM EXECUÇÃO", "ADIMPLENTE", "NORMAL", "INADIMPLÊNCIA SUSPENSA"}
PROBLEMA = {"INADIMPLENTE", "PRESTAÇÃO DE CONTAS REJEITADA", "PRESTAÇÃO DE CONTAS REJEITADA COM INSTAURAÇÃO DE TCE"}
ENCERRADO = {"CONCLUÍDO", "PRESTAÇÃO DE CONTAS APROVADA", "ARQUIVADO", "EXCLUÍDO", "CONVÊNIO ANULADO", "RESCINDIDO",
             "BAIXADO", "CANCELADO", "ANULADO"}


# ------------------------------------------------------------
# Utilitarios
# ------------------------------------------------------------

def _sem_acento(s: str) -> str:
    return unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode("ascii").lower()


def _num(v) -> float:
    try:
        return float(v) if v is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def grupo_situacao(situacao: str) -> str:
    s = (situacao or "").strip().upper()
    if s in PROBLEMA or "REJEITAD" in s:
        return "problema"
    if s in ENCERRADO:
        return "encerrado"
    if "PRESTAÇÃO DE CONTAS" in s or "PRESTACAO DE CONTAS" in s:
        return "prestação de contas"
    return "andamento"


def eh_obra(instrumento: str, objeto: str) -> bool:
    return "repasse" in _sem_acento(instrumento) or bool(_OBRA_PALAVRAS.search(_sem_acento(objeto)))


def fmt_brl(v) -> str:
    try:
        return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return ""


# ------------------------------------------------------------
# Coleta
# ------------------------------------------------------------

def _get(session, chave: str, params: dict, tentativas: int = 4) -> list:
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        espera = 2.0 * (i + 1)
        try:
            r = session.get(f"{BASE}/convenios", params=params,
                            headers={"chave-api-dados": chave, "Accept": "application/json"}, timeout=90)
            if r.status_code == 200:
                d = r.json()
                return d if isinstance(d, list) else []
            ultimo = f"HTTP {r.status_code}"
            if r.status_code in (401, 403):
                raise ErroColeta(f"chave da API recusada ({r.status_code})")
            if r.status_code == 429:
                espera = 20.0 * (i + 1)
        except (requests.RequestException, ValueError) as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(espera)
    raise ErroColeta(ultimo)


def janela(hoje: Optional[dt.date] = None) -> Tuple[str, str]:
    hoje = hoje or dt.date.today()
    return f"01/01/{hoje.year - 1}", "31/12/2040"


def coletar_uf(uf: str, chave: str, session=None, hoje: Optional[dt.date] = None, pausa: float = PAUSA_S,
               max_paginas: int = 1500) -> List[dict]:
    """Todos os instrumentos da UF com vigencia terminando na janela. Levanta ErroColeta se falhar."""
    session = session or requests.Session()
    ini, fim = janela(hoje)
    itens: List[dict] = []
    for pag in range(1, max_paginas + 1):
        d = _get(session, chave, {"uf": uf, "dataVigenciaInicial": ini, "dataVigenciaFinal": fim, "pagina": pag})
        itens += d
        if pausa:
            time.sleep(pausa)
        if len(d) < POR_PAGINA:
            return itens
    raise ErroColeta(f"{uf}: mais de {max_paginas} páginas (corte de segurança)")


def normalizar(itens: List[dict], uf: str) -> pd.DataFrame:
    linhas = []
    for x in itens:
        dim = x.get("dimConvenio") or {}
        conv = x.get("convenente") or {}
        mun = x.get("municipioConvenente") or {}
        org = x.get("orgao") or {}
        sub = x.get("subfuncao") or {}
        instr = (x.get("tipoInstrumento") or {}).get("descricao") or ""
        objeto = " ".join(str(dim.get("objeto") or "").split())
        situacao = str(x.get("situacao") or "").strip()
        linhas.append({
            "id": x.get("id"), "numero": dim.get("numero") or "", "objeto": objeto, "situacao": situacao,
            "grupo": grupo_situacao(situacao), "instrumento": instr if instr != "Não se aplica" else "",
            "obra": eh_obra(instr, objeto), "uf": uf, "cod_ibge": str(mun.get("codigoIBGE") or ""),
            "municipio": str(mun.get("nomeIBGE") or "").title(), "convenente": conv.get("nome") or "",
            "tipo_convenente": conv.get("tipo") or "", "cnpj": conv.get("cnpjFormatado") or "",
            "orgao_superior": (org.get("orgaoMaximo") or {}).get("nome") or org.get("nome") or "",
            "orgao": org.get("nome") or "", "subfuncao": sub.get("descricaoSubfuncap") or sub.get("descricaoSubfuncao") or "",
            "valor": _num(x.get("valor")), "liberado": _num(x.get("valorLiberado")),
            "contrapartida": _num(x.get("valorContrapartida")),
            "inicio": x.get("dataInicioVigencia") or "", "fim": x.get("dataFinalVigencia") or "",
            "ultima_liberacao": x.get("dataUltimaLiberacao") or "", "conclusao": x.get("dataConclusao") or "",
        })
    return pd.DataFrame(linhas, columns=COLUNAS)


# ------------------------------------------------------------
# Pontos de atencao
# ------------------------------------------------------------
LIM_VALOR_ALTO = 10_000_000.0


def avaliar(df: pd.DataFrame, hoje: Optional[dt.date] = None) -> pd.DataFrame:
    """Adiciona dias_vencido, pct_liberado, pct_prazo, score e pontos."""
    hoje_ts = pd.Timestamp(hoje or dt.date.today())
    d = df.copy()
    ini = pd.to_datetime(d["inicio"], errors="coerce")
    fim = pd.to_datetime(d["fim"], errors="coerce")
    ult = pd.to_datetime(d["ultima_liberacao"], errors="coerce")
    d["dias_vencido"] = (hoje_ts - fim).dt.days.where(fim.notna()).fillna(0).astype(int).clip(lower=0)
    prazo = (fim - ini).dt.days.where((fim - ini).dt.days > 0)
    d["pct_prazo"] = ((hoje_ts - ini).dt.days / prazo * 100).clip(0, 999).fillna(0)
    d["pct_liberado"] = (d["liberado"] / d["valor"].where(d["valor"] > 0) * 100).fillna(0)
    d["meses_sem_liberacao"] = ((hoje_ts - ult).dt.days / 30.4).where(ult.notna())
    ativo = d["grupo"].isin(["andamento", "problema"])
    pc = d["grupo"].eq("prestação de contas")
    sit = d["situacao"].str.upper()

    regras = [
        (ativo & (d["dias_vencido"] > 365), 40, lambda r: f"vigência encerrada há {r.dias_vencido} dias e o instrumento segue sem conclusão"),
        (ativo & (d["dias_vencido"] > 0) & (d["dias_vencido"] <= 365), 25, lambda r: f"vigência encerrada há {r.dias_vencido} dias e o instrumento segue sem conclusão"),
        (ativo & (d["pct_prazo"] >= 70) & (d["pct_liberado"] < 30) & (d["dias_vencido"] == 0) & (d["valor"] > 0), 25,
         lambda r: f"só {r.pct_liberado:.0f}% do valor liberado com {min(r.pct_prazo, 100):.0f}% do prazo decorrido"),
        (ativo & (d["liberado"] <= 0) & (d["valor"] > 0) & (d["pct_prazo"] >= 50), 15, lambda r: "nenhuma liberação com metade do prazo já decorrido"),
        (ativo & (d["meses_sem_liberacao"] > 12) & (d["dias_vencido"] == 0), 10, lambda r: f"sem liberação há {r.meses_sem_liberacao:.0f} meses"),
        (sit.eq("INADIMPLENTE"), 30, lambda r: "convenente inadimplente"),
        (sit.str.contains("REJEITAD"), 30, lambda r: "prestação de contas rejeitada"),
        (pc & sit.str.contains("AGUARDANDO") & (d["dias_vencido"] > 120), 25,
         lambda r: f"prestação de contas ainda não enviada, {r.dias_vencido} dias após o fim da vigência"),
        (ativo & (d["valor"] >= LIM_VALOR_ALTO), 10, lambda r: "valor alto (≥ R$ 10 mi)"),
    ]
    score = pd.Series(0, index=d.index)
    for cond, pts, _ in regras:
        score = score + cond.fillna(False).astype(int) * pts
    d["score"] = score.clip(upper=100)
    pontos = pd.Series("", index=d.index)
    for i in d.index[d["score"] > 0]:
        r = d.loc[i]
        pontos.at[i] = " | ".join(f(r) for c, _, f in regras if bool(c.at[i]))
    d["pontos"] = pontos
    # remove duplicidade: as duas regras de vigencia sao excludentes por construcao
    return d


# ------------------------------------------------------------
# Resumos
# ------------------------------------------------------------

def resumo_uf(d: pd.DataFrame) -> pd.DataFrame:
    """Por UF: carteira em andamento, liberado, vencidos e valor em atraso."""
    ativo = d[d["grupo"].isin(["andamento", "problema"])]
    base = d.groupby("uf").agg(instrumentos=("id", "size"), valor=("valor", "sum"), liberado=("liberado", "sum"))
    a = ativo.groupby("uf").agg(ativos=("id", "size"), valor_ativo=("valor", "sum"), liberado_ativo=("liberado", "sum"),
                                obras_ativas=("obra", "sum"))
    v = ativo[ativo["dias_vencido"] > 0].groupby("uf").agg(vencidos=("id", "size"), valor_vencido=("valor", "sum"))
    p = d[d["score"] >= 60].groupby("uf").size().rename("alertas_altos")
    out = base.join([a, v, p]).fillna(0).reset_index()
    out["pct_liberado_ativo"] = (out["liberado_ativo"] / out["valor_ativo"].where(out["valor_ativo"] > 0) * 100).fillna(0)
    out["pct_vencidos"] = (out["vencidos"] / out["ativos"].where(out["ativos"] > 0) * 100).fillna(0)
    return out.sort_values("valor_ativo", ascending=False)


def resumo_municipios(d: pd.DataFrame) -> pd.DataFrame:
    ativo = d[d["grupo"].isin(["andamento", "problema"])]
    g = ativo.groupby(["cod_ibge", "municipio"]).agg(
        ativos=("id", "size"), valor=("valor", "sum"), liberado=("liberado", "sum"), obras=("obra", "sum"),
        vencidos=("dias_vencido", lambda s: int((s > 0).sum()))).reset_index()
    g["valor_vencido"] = g["cod_ibge"].map(ativo[ativo["dias_vencido"] > 0].groupby("cod_ibge")["valor"].sum()).fillna(0)
    g["pct_liberado"] = (g["liberado"] / g["valor"].where(g["valor"] > 0) * 100).fillna(0)
    return g.sort_values("valor", ascending=False)


TOP_ALERTAS = 1500
ALERTA_COLS = ["id", "numero", "objeto", "situacao", "instrumento", "obra", "uf", "municipio", "convenente", "orgao_superior",
               "valor", "liberado", "contrapartida", "inicio", "fim", "ultima_liberacao", "dias_vencido", "pct_liberado",
               "pct_prazo", "score", "pontos"]


def resumir(d: pd.DataFrame, ufs_completas: List[str]) -> Dict[str, pd.DataFrame]:
    """d = todos os instrumentos avaliados (de todas as UFs coletadas)."""
    uf = resumo_uf(d)
    uf["completa"] = uf["uf"].isin(ufs_completas)
    sc = d[d["uf"] == UF_DESTAQUE].sort_values(["score", "valor"], ascending=[False, False])
    alertas = d[d["score"] > 0].sort_values(["score", "valor"], ascending=[False, False]).head(TOP_ALERTAS)[ALERTA_COLS]
    return {"uf": uf, "sc": sc[COLUNAS + ["dias_vencido", "pct_liberado", "pct_prazo", "score", "pontos"]],
            "municipios_sc": resumo_municipios(sc), "alertas": alertas}


# ------------------------------------------------------------
# Snapshot (data/repasses_*.csv)
# ------------------------------------------------------------
PASTA_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
URL_DADOS_PADRAO = "https://raw.githubusercontent.com/LucasPin45/FiscalizaGov/main/data"
_TIPOS = {"id": str, "cod_ibge": str, "numero": str, "cnpj": str}


def salvar(resumos: Dict[str, pd.DataFrame], pasta: str = PASTA_DADOS, agora: str = "", ufs_completas=(),
           janela_txt: str = "") -> None:
    os.makedirs(pasta, exist_ok=True)
    for nome, df in resumos.items():
        df.to_csv(os.path.join(pasta, f"repasses_{nome}.csv"), index=False, encoding="utf-8")
    meta = {"gerado_em": agora, "ufs_completas": sorted(ufs_completas), "janela": janela_txt}
    with open(os.path.join(pasta, "repasses_meta.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=1, sort_keys=True)


def ler(base: str = "", timeout: int = 40) -> Optional[Tuple[Dict[str, pd.DataFrame], dict]]:
    base = base or os.getenv("FISCALIZAGOV_DATA_URL", URL_DADOS_PADRAO)
    out: Dict[str, pd.DataFrame] = {}
    try:
        for nome in RECURSOS:
            if base.startswith("http"):
                r = requests.get(f"{base}/repasses_{nome}.csv", timeout=timeout)
                if r.status_code != 200:
                    return None
                out[nome] = pd.read_csv(io.StringIO(r.text), dtype=_TIPOS, keep_default_na=False)
            else:
                out[nome] = pd.read_csv(os.path.join(base, f"repasses_{nome}.csv"), dtype=_TIPOS, keep_default_na=False)
        if base.startswith("http"):
            m = requests.get(f"{base}/repasses_meta.json", timeout=timeout)
            meta = m.json() if m.status_code == 200 else {}
        else:
            with open(os.path.join(base, "repasses_meta.json"), encoding="utf-8") as f:
                meta = json.load(f)
    except (OSError, ValueError, requests.RequestException):
        return None
    if out["uf"].empty or "instrumentos" not in out["uf"].columns:
        return None
    return out, meta
