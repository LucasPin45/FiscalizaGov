# painel.py - quanto o Governo Federal arrecadou e gastou no ano (alimenta o relogio da Apresentacao)
# ============================================================
# Fontes oficiais da CGU / Portal da Transparencia (arquivos pequenos, atualizados todo dia util):
#   Receitas:   .../saida/receitas/AAAA_Receitas.zip        (~1,7 MB, uma linha por lancamento, com DATA)
#   Despesas:   .../saida/orcamento-despesa/AAAA_OrcamentoDespesa.zip  (~0,8 MB; empenhado e liquidado acumulados)
#
# DEFINICOES (importantes; a tela repete o aviso):
#  - ARRECADACAO = receita realizada (correntes + de capital), SEM operacoes de credito e SEM as receitas
#    intra-orcamentarias (repasses entre orgaos do proprio governo, que contariam duas vezes).
#  - DIVIDA CONTRATADA = operacoes de credito (inclui o refinanciamento da divida, que e rolagem, nao dinheiro novo).
#  - GASTO = despesa liquidada, SEM amortizacao e refinanciamento da divida (rolagem) e sem reserva de contingencia;
#    os juros da divida ficam DENTRO do gasto.
# O contador do relogio e uma ESTIMATIVA: acumulado ate a data do arquivo + ritmo medio do ano. Nao e dado em tempo real.
# ============================================================

import datetime as dt
import io
import json
import os
import time
import zipfile
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

import orcamento
from dou import ErroColeta

URL_RECEITAS = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/receitas/{ano}_Receitas.zip"
URL_DESPESAS = orcamento.URL_ZIP
LIMITE_CSV = 200 * 1024 * 1024
BRT = dt.timezone(dt.timedelta(hours=-3))

COLS_RECEITA = {"CATEGORIA ECONÔMICA": "cat", "ORIGEM RECEITA": "orig", "DETALHAMENTO": "det",
                "VALOR REALIZADO": "realizado", "DATA LANÇAMENTO": "data"}


# ------------------------------------------------------------
# Download
# ------------------------------------------------------------

def baixar(url: str, session=None, tentativas: int = 3) -> Tuple[bytes, Optional[dt.date]]:
    """Baixa o zip e devolve (bytes, data do Last-Modified em Brasilia). Levanta ErroColeta se falhar."""
    session = session or requests.Session()
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        try:
            r = session.get(url, timeout=120, headers={"User-Agent": "Mozilla/5.0 (FiscalizaGov)"})
            if r.status_code == 200 and len(r.content) > 50_000:
                mod = None
                try:
                    from email.utils import parsedate_to_datetime
                    mod = parsedate_to_datetime(r.headers["Last-Modified"]).astimezone(BRT).date()
                except (KeyError, TypeError, ValueError):
                    pass
                return r.content, mod
            ultimo = f"HTTP {r.status_code}" if r.status_code != 200 else "arquivo pequeno demais"
        except requests.RequestException as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(2.0 * (i + 1))
    raise ErroColeta(f"{url.rsplit('/', 1)[-1]}: {ultimo}")


# ------------------------------------------------------------
# Receitas
# ------------------------------------------------------------

def ler_receitas(conteudo: bytes, ano: int) -> pd.DataFrame:
    """Zip de receitas -> DataFrame agregado por data x categoria x origem x refinanciamento (poucas centenas de linhas)."""
    try:
        z = zipfile.ZipFile(io.BytesIO(conteudo))
    except zipfile.BadZipFile:
        raise ErroColeta(f"receitas {ano}: arquivo zip inválido")
    membros = [i for i in z.infolist() if i.filename.lower().endswith("_receitas.csv")]
    if not membros or membros[0].file_size > LIMITE_CSV:
        raise ErroColeta(f"receitas {ano}: CSV não encontrado ou grande demais")
    with z.open(membros[0]) as f:
        bruto = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str)
    faltam = [c for c in COLS_RECEITA if c not in bruto.columns]
    if faltam or len(bruto) < 1000:
        raise ErroColeta(f"receitas {ano}: formato inesperado (faltam {faltam[:3]}, {len(bruto)} linhas)")
    df = bruto[list(COLS_RECEITA)].rename(columns=COLS_RECEITA)
    df["realizado"] = pd.to_numeric(df["realizado"].str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
                                    errors="coerce").fillna(0.0)
    df["dt"] = pd.to_datetime(df["data"], format="%d/%m/%Y", errors="coerce")
    df = df.dropna(subset=["dt"])
    df["intra"] = df["cat"].fillna("").str.contains("intra", case=False)
    df["credito"] = df["orig"].fillna("").str.contains("Operações de Crédito|Operacoes de Credito", case=False)
    df["refin"] = df["credito"] & df["det"].fillna("").str.contains("REFIN", case=False)
    return df.groupby(["dt", "intra", "credito", "refin"], as_index=False)["realizado"].sum()


JANELA_DIAS = 30  # o ritmo "recente" olha os ultimos 30 dias, como o relogio da divida dos EUA olha a variacao recente


def ritmo_recente_receita(df: pd.DataFrame, ultimo: pd.Timestamp, dias: int = JANELA_DIAS) -> float:
    """Arrecadacao media dos ultimos `dias` dias corridos ate `ultimo` (inclusive), em R$ por segundo."""
    ini = ultimo - pd.Timedelta(days=dias)
    base = df[(df["dt"] > ini) & (df["dt"] <= ultimo) & ~df["intra"] & ~df["credito"]]
    return float(base["realizado"].sum()) / (dias * 86400.0)


def somar_receitas(df: pd.DataFrame, ate: Optional[pd.Timestamp] = None) -> Dict[str, float]:
    d = df if ate is None else df[df["dt"] <= ate]
    base = d[~d["intra"]]
    return {
        "arrecadacao": float(base[~base["credito"]]["realizado"].sum()),
        "credito": float(base[base["credito"]]["realizado"].sum()),
        "refinanciamento": float(base[base["refin"]]["realizado"].sum()),
    }


# ------------------------------------------------------------
# Despesas
# ------------------------------------------------------------

def somar_gastos(df_orc: pd.DataFrame) -> Dict[str, float]:
    """df_orc = orcamento.ler_zip(...). Gasto = liquidado sem amortizacao/refinanciamento e sem reserva."""
    d = df_orc[df_orc["Tipo de gasto"] != orcamento.T_RESERVA]
    gasto = d[d["Tipo de gasto"] != orcamento.T_AMORT]
    return {
        "gasto_liquidado": float(gasto["Liquidado"].sum()),
        "gasto_empenhado": float(gasto["Empenhado"].sum()),
        "juros_liquidado": float(d[d["Tipo de gasto"] == orcamento.T_JUROS]["Liquidado"].sum()),
        "amortizacao_liquidada": float(d[d["Tipo de gasto"] == orcamento.T_AMORT]["Liquidado"].sum()),
    }


# ------------------------------------------------------------
# Montagem e ritmo
# ------------------------------------------------------------

def _ms(d: dt.datetime) -> int:
    return int(d.timestamp() * 1000)


def inicio_do_ano_ms(ano: int) -> int:
    return _ms(dt.datetime(ano, 1, 1, tzinfo=BRT))


def montar(ano: int, rec_ano: pd.DataFrame, rec_ant: Optional[pd.DataFrame], df_orc: pd.DataFrame,
           data_despesa: Optional[dt.date], agora: str = "") -> dict:
    """Junta tudo num dicionario simples (vira JSON). As-of das receitas = fim do ultimo dia lancado."""
    ultimo = rec_ano["dt"].max()
    asof_rec = dt.datetime(ultimo.year, ultimo.month, ultimo.day, tzinfo=BRT) + dt.timedelta(days=1)
    rec = somar_receitas(rec_ano)
    anterior = None
    if rec_ant is not None and not rec_ant.empty:
        corte = pd.Timestamp(year=ano - 1, month=ultimo.month, day=min(ultimo.day, 28 if ultimo.month == 2 else ultimo.day))
        anterior = somar_receitas(rec_ant, corte)["arrecadacao"]
    gas = somar_gastos(df_orc)
    asof_gasto = dt.datetime(*(data_despesa or ultimo.date()).timetuple()[:3], tzinfo=BRT)
    if asof_gasto <= dt.datetime(ano, 1, 1, tzinfo=BRT):
        asof_gasto = asof_rec
    inicio = inicio_do_ano_ms(ano)
    rit_ano_r = ritmo(rec["arrecadacao"], inicio, _ms(asof_rec))
    rit_30_r = ritmo_recente_receita(rec_ano, ultimo)
    rit_ano_g = ritmo(gas["gasto_liquidado"], inicio, _ms(asof_gasto))
    seg_g = max((_ms(asof_gasto) - inicio) / 1000.0, 1.0)
    return {
        "ano": ano, "gerado_em": agora,
        "inicio_ano_ms": inicio,
        "receita": {**rec, "asof_ms": _ms(asof_rec), "asof": asof_rec.date().isoformat(),
                    "mesmo_periodo_ano_anterior": anterior,
                    # ritmo usado pelo contador: ultimos 30 dias (a data de cada lancamento esta no arquivo)
                    "ritmo": rit_30_r if rit_30_r > 0 else rit_ano_r,
                    "ritmo_base": f"últimos {JANELA_DIAS} dias" if rit_30_r > 0 else "média do ano",
                    "ritmo_ano": rit_ano_r},
        "gasto": {**gas, "asof_ms": _ms(asof_gasto), "asof": asof_gasto.date().isoformat(),
                  # o arquivo de despesas e acumulado e sem data por lancamento: comeca na media do ano e passa
                  # ao ritmo recente quando o historico diario (aplicar_historico) tiver pelo menos dois pontos
                  "ritmo": rit_ano_g, "ritmo_base": "média do ano", "ritmo_ano": rit_ano_g,
                  "juros_por_segundo": gas["juros_liquidado"] / seg_g},
    }


# ------------------------------------------------------------
# Historico diario do gasto (a rotina noturna acrescenta uma linha por dia)
# ------------------------------------------------------------
COLUNAS_HIST = ["asof", "asof_ms", "gasto_liquidado", "gasto_empenhado", "juros_liquidado", "gerado_em"]


def ler_historico(pasta: str = "") -> pd.DataFrame:
    pasta = pasta or PASTA_DADOS
    try:
        return pd.read_csv(os.path.join(pasta, "painel_historico.csv"))[COLUNAS_HIST]
    except (OSError, ValueError, KeyError):
        return pd.DataFrame(columns=COLUNAS_HIST)


def atualizar_historico(p: dict, pasta: str = "", max_linhas: int = 800) -> pd.DataFrame:
    """Acrescenta (ou substitui) a linha da data do arquivo de despesas e grava data/painel_historico.csv."""
    pasta = pasta or PASTA_DADOS
    g = p["gasto"]
    nova = pd.DataFrame([{"asof": g["asof"], "asof_ms": g["asof_ms"], "gasto_liquidado": g["gasto_liquidado"],
                          "gasto_empenhado": g["gasto_empenhado"], "juros_liquidado": g["juros_liquidado"],
                          "gerado_em": p.get("gerado_em", "")}])
    h = ler_historico(pasta)
    h = h[h["asof"] != g["asof"]]
    h = pd.concat([h, nova], ignore_index=True).sort_values("asof_ms").tail(max_linhas).reset_index(drop=True)
    os.makedirs(pasta, exist_ok=True)
    h.to_csv(os.path.join(pasta, "painel_historico.csv"), index=False)
    return h


def ritmo_gasto_recente(h: pd.DataFrame, janela: int = JANELA_DIAS) -> Optional[Tuple[float, float]]:
    """(R$ por segundo, dias usados) entre o ponto mais recente e o mais antigo dentro da janela. None se faltar historico."""
    if h is None or len(h) < 2:
        return None
    h = h.sort_values("asof_ms")
    ult = h.iloc[-1]
    # referencia entre 3 e 30 dias antes: com menos de 3 dias o ritmo oscila demais (fim de semana, lotes)
    dentro = h[(h["asof_ms"] >= ult["asof_ms"] - janela * 86400 * 1000) & (h["asof_ms"] <= ult["asof_ms"] - 3 * 86400 * 1000)]
    if dentro.empty:
        return None
    ref = dentro.iloc[0]
    dseg = (ult["asof_ms"] - ref["asof_ms"]) / 1000.0
    delta = float(ult["gasto_liquidado"] - ref["gasto_liquidado"])
    if dseg <= 0 or delta <= 0:
        return None
    return delta / dseg, dseg / 86400.0


def aplicar_historico(p: dict, h: pd.DataFrame) -> dict:
    """Troca o ritmo do gasto pelo recente quando ha historico suficiente."""
    r = ritmo_gasto_recente(h)
    if r:
        p["gasto"]["ritmo"] = r[0]
        p["gasto"]["ritmo_base"] = f"últimos {r[1]:.0f} dias"
    return p


def ritmo(valor_acumulado: float, inicio_ms: int, asof_ms: int) -> float:
    """Reais por segundo (media do ano ate a data do arquivo)."""
    seg = max((asof_ms - inicio_ms) / 1000.0, 1.0)
    return valor_acumulado / seg


def estimar(valor_acumulado: float, inicio_ms: int, asof_ms: int, agora_ms: int) -> float:
    """Valor estimado em `agora_ms`: acumulado + ritmo medio * tempo desde a data do arquivo."""
    return valor_acumulado + ritmo(valor_acumulado, inicio_ms, asof_ms) * max(agora_ms - asof_ms, 0) / 1000.0


# ------------------------------------------------------------
# Snapshot (data/painel.json)
# ------------------------------------------------------------
PASTA_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
URL_DADOS_PADRAO = "https://raw.githubusercontent.com/LucasPin45/FiscalizaGov/main/data"


def salvar(painel: dict, pasta: str = PASTA_DADOS) -> None:
    os.makedirs(pasta, exist_ok=True)
    with open(os.path.join(pasta, "painel.json"), "w", encoding="utf-8") as f:
        json.dump(painel, f, ensure_ascii=False, indent=1, sort_keys=True)


def ler(base: str = "", timeout: int = 30) -> Optional[dict]:
    base = base or os.getenv("FISCALIZAGOV_DATA_URL", URL_DADOS_PADRAO)
    try:
        if base.startswith("http"):
            r = requests.get(f"{base}/painel.json", timeout=timeout)
            if r.status_code != 200:
                return None
            p = r.json()
        else:
            with open(os.path.join(base, "painel.json"), encoding="utf-8") as f:
                p = json.load(f)
    except (OSError, ValueError, requests.RequestException):
        return None
    ok = isinstance(p, dict) and all(k in p for k in ("ano", "inicio_ano_ms", "receita", "gasto"))
    return p if ok else None


def calcular_ao_vivo(ano: int, session=None) -> dict:
    """Baixa os arquivos da CGU e monta o painel (usado pela rotina noturna e como reserva do app)."""
    session = session or requests.Session()
    zr, _ = baixar(URL_RECEITAS.format(ano=ano), session)
    rec_ano = ler_receitas(zr, ano)
    try:
        za, _ = baixar(URL_RECEITAS.format(ano=ano - 1), session)
        rec_ant = ler_receitas(za, ano - 1)
    except ErroColeta:
        rec_ant = None  # sem o ano anterior so perdemos a comparacao
    zd, data_desp = baixar(URL_DESPESAS.format(ano=ano), session)
    df_orc = orcamento.ler_zip(zd, ano)
    return montar(ano, rec_ano, rec_ant, df_orc, data_desp)
