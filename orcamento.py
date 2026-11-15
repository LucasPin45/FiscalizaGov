# orcamento.py - orcamento da despesa (dotacao x empenhado), por tipo de gasto e por orgao
# ============================================================
# Fonte: arquivo oficial "Orcamento da Despesa" da CGU (Portal da Transparencia):
#   https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/orcamento-despesa/AAAA_OrcamentoDespesa.zip
# (cerca de 0,8 MB por ano; o CSV tem ~25 mil linhas). Traz, por orgao, funcao, programa, acao, grupo e elemento
# de despesa: orcamento inicial, orcamento ATUALIZADO (dotacao), EMPENHADO e REALIZADO (= liquidado).
# Conferido contra a API de execucao: FNDE 2026, empenhado 126,88 bi (arquivo) x 126,80 bi (API).
#
# Duas leituras:
#  - "Tipo de gasto": agrupamento didatico sobre a classificacao oficial (grupo de despesa + elemento):
#    pessoal, aposentadorias de servidores, beneficios do INSS, beneficios sociais, transferencias...
#  - "Quem menos usa o orcamento": empenhado / dotacao atualizada, por orgao. Baixa execucao NAO e prova de
#    ma gestao: pode vir de bloqueio/contingenciamento por decreto, dependencia de transferencias ou do
#    calendario (no inicio do ano e normal estar baixo; o ano em curso e parcial).
# ============================================================

import datetime as dt
import io
import time
import zipfile
from typing import Optional

import pandas as pd
import requests

from dou import ErroColeta

URL_ZIP = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/orcamento-despesa/{ano}_OrcamentoDespesa.zip"
LIMITE_CSV = 200 * 1024 * 1024  # protecao contra zip gigante
MIN_LINHAS = 1000               # abaixo disso o arquivo e considerado quebrado

ORIG = {
    "CÓDIGO ÓRGÃO SUPERIOR": "Cód. superior", "NOME ÓRGÃO SUPERIOR": "Órgão superior",
    "CÓDIGO ÓRGÃO SUBORDINADO": "Cód. órgão", "NOME ÓRGÃO SUBORDINADO": "Órgão",
    "NOME FUNÇÃO": "Função", "CÓDIGO GRUPO DE DESPESA": "Cód. GND", "NOME GRUPO DE DESPESA": "Grupo de despesa",
    "CÓDIGO ELEMENTO DE DESPESA": "Cód. elemento", "NOME ELEMENTO DE DESPESA": "Elemento",
}
VALORES = {"ORÇAMENTO INICIAL (R$)": "Dotação inicial", "ORÇAMENTO ATUALIZADO (R$)": "Dotação atualizada",
           "ORÇAMENTO EMPENHADO (R$)": "Empenhado", "ORÇAMENTO REALIZADO (R$)": "Liquidado"}
COLUNAS = ["Ano", "Cód. superior", "Órgão superior", "Cód. órgão", "Órgão", "Função", "Cód. GND", "Grupo de despesa",
           "Cód. elemento", "Elemento", "Tipo de gasto", "Dotação inicial", "Dotação atualizada", "Empenhado", "Liquidado"]
METRICAS = ["Empenhado", "Liquidado", "Dotação atualizada"]

# ------------------------------------------------------------
# Tipo de gasto (visao popular da classificacao oficial)
# ------------------------------------------------------------
T_PESSOAL = "Pessoal ativo e encargos"
T_APOSENT_SERV = "Aposentadorias e pensões de servidores e militares"
T_INSS = "Benefícios previdenciários do INSS (RGPS)"
T_SOCIAL = "Benefícios sociais (BPC, Bolsa Família, seguro-desemprego, abono)"
T_TRANSF = "Transferências, contribuições e subvenções"
T_CUSTEIO = "Custeio da máquina e serviços"
T_INVEST = "Investimentos (obras e equipamentos)"
T_INVERSOES = "Inversões financeiras (empréstimos e participações)"
T_JUROS = "Juros da dívida"
T_AMORT = "Amortização e refinanciamento da dívida"
T_RESERVA = "Reserva de contingência"
ORDEM_TIPOS = [T_PESSOAL, T_APOSENT_SERV, T_INSS, T_SOCIAL, T_TRANSF, T_CUSTEIO, T_INVEST, T_INVERSOES, T_JUROS,
               T_AMORT, T_RESERVA]
TIPOS_DIVIDA = {T_JUROS, T_AMORT}

_INSS = {"53", "54", "55", "56", "57", "58"}
_SOCIAL = {"06", "10", "18", "48", "59"}
_TRANSF = {"41", "42", "43", "45", "81", "85"}
_APOSENT_SERV = {"01", "03", "05"}


def tipo_de_gasto(cod_gnd: str, cod_elemento: str) -> str:
    g, e = str(cod_gnd).strip(), str(cod_elemento).strip().zfill(2)
    if g == "1":
        return T_APOSENT_SERV if e in _APOSENT_SERV else T_PESSOAL
    if g == "2":
        return T_JUROS
    if g == "4":
        return T_INVEST
    if g == "5":
        return T_INVERSOES
    if g == "6":
        return T_AMORT
    if g == "9":
        return T_RESERVA
    if e in _INSS:
        return T_INSS
    if e in _SOCIAL:
        return T_SOCIAL
    if e in _TRANSF:
        return T_TRANSF
    return T_CUSTEIO


# ------------------------------------------------------------
# Download e leitura
# ------------------------------------------------------------

def baixar_zip(ano: int, session=None, tentativas: int = 3) -> bytes:
    session = session or requests.Session()
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        try:
            r = session.get(URL_ZIP.format(ano=ano), timeout=120, headers={"User-Agent": "Mozilla/5.0 (FiscalizaGov)"})
            if r.status_code == 200 and len(r.content) > 50_000:
                return r.content
            ultimo = f"HTTP {r.status_code}" if r.status_code != 200 else "arquivo pequeno demais"
        except requests.RequestException as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(2.0 * (i + 1))
    raise ErroColeta(f"orçamento {ano}: {ultimo}")


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False), errors="coerce").fillna(0.0)


def ler_zip(conteudo: bytes, ano: int) -> pd.DataFrame:
    """CSV do zip -> DataFrame agregado (orgao x funcao x grupo x elemento). Levanta ErroColeta se vier errado."""
    try:
        z = zipfile.ZipFile(io.BytesIO(conteudo))
    except zipfile.BadZipFile:
        raise ErroColeta(f"orçamento {ano}: arquivo zip inválido")
    membros = [i for i in z.infolist() if i.filename.lower().endswith("_orcamentodespesa.csv")]
    if not membros or membros[0].file_size > LIMITE_CSV:
        raise ErroColeta(f"orçamento {ano}: CSV não encontrado ou grande demais")
    with z.open(membros[0]) as f:
        bruto = pd.read_csv(f, sep=";", encoding="latin-1", dtype=str)
    faltam = [c for c in list(ORIG) + list(VALORES) if c not in bruto.columns]
    if faltam or len(bruto) < MIN_LINHAS:
        raise ErroColeta(f"orçamento {ano}: formato inesperado (faltam {faltam[:3]}, {len(bruto)} linhas)")
    df = bruto.rename(columns={**ORIG, **VALORES})
    for c in VALORES.values():
        df[c] = _num(df[c])
    df["Ano"] = ano
    df["Tipo de gasto"] = [tipo_de_gasto(g, e) for g, e in zip(df["Cód. GND"], df["Cód. elemento"])]
    chaves = ["Ano", "Cód. superior", "Órgão superior", "Cód. órgão", "Órgão", "Função", "Cód. GND", "Grupo de despesa",
              "Cód. elemento", "Elemento", "Tipo de gasto"]
    df[chaves] = df[chaves].fillna("")
    out = df.groupby(chaves, as_index=False)[list(VALORES.values())].sum()
    return out[COLUNAS]


def coletar(ano: int, session=None) -> pd.DataFrame:
    return ler_zip(baixar_zip(ano, session), ano)


# ------------------------------------------------------------
# Analises
# ------------------------------------------------------------

def sem_divida(df: pd.DataFrame, incluir_divida: bool) -> pd.DataFrame:
    """Reserva de contingencia sai sempre (nao e gasto de ninguem); juros e amortizacao saem por padrao."""
    d = df[df["Tipo de gasto"] != T_RESERVA]
    return d if incluir_divida else d[~d["Tipo de gasto"].isin(TIPOS_DIVIDA)]


def por_tipo(df: pd.DataFrame, metrica: str, oficial: bool = False) -> pd.DataFrame:
    col = "Grupo de despesa" if oficial else "Tipo de gasto"
    g = df.groupby(col, as_index=False)[metrica].sum()
    total = g[metrica].sum()
    g["% do total"] = g[metrica] / total * 100 if total else 0.0
    return g.sort_values(metrica, ascending=False).reset_index(drop=True)


def por_funcao(df: pd.DataFrame, metrica: str) -> pd.DataFrame:
    g = df.groupby("Função", as_index=False)[metrica].sum()
    total = g[metrica].sum()
    g["% do total"] = g[metrica] / total * 100 if total else 0.0
    return g.sort_values(metrica, ascending=False).reset_index(drop=True)


def detalhar(df: pd.DataFrame, tipo: str, metrica: str, oficial: bool = False, top: int = 12) -> pd.DataFrame:
    col = "Grupo de despesa" if oficial else "Tipo de gasto"
    g = df[df[col] == tipo].groupby("Elemento", as_index=False)[metrica].sum()
    total = g[metrica].sum()
    g["% do tipo"] = g[metrica] / total * 100 if total else 0.0
    return g.sort_values(metrica, ascending=False).head(top).reset_index(drop=True)


ESCOPOS = {
    "Todas as despesas": None,
    "Só investimentos (obras e equipamentos)": {T_INVEST},
    "Só transferências e subvenções": {T_TRANSF},
    "Só custeio da máquina e serviços": {T_CUSTEIO},
}


def execucao(df: pd.DataFrame, nivel: str = "Órgão", escopo: str = "Todas as despesas",
             dotacao_min: float = 100e6, incluir_divida: bool = False) -> pd.DataFrame:
    """Ranking de execucao: % empenhado e % liquidado da dotacao atualizada, do pior para o melhor."""
    d = sem_divida(df, incluir_divida)
    tipos = ESCOPOS.get(escopo)
    if tipos:
        d = d[d["Tipo de gasto"].isin(tipos)]
    chaves = ["Órgão superior", "Órgão"] if nivel == "Órgão" else ["Órgão superior"]
    g = d.groupby(chaves, as_index=False)[["Dotação atualizada", "Empenhado", "Liquidado"]].sum()
    g = g[g["Dotação atualizada"] > max(dotacao_min, 0)]
    g["% empenhado"] = g["Empenhado"] / g["Dotação atualizada"] * 100
    g["% liquidado"] = g["Liquidado"] / g["Dotação atualizada"] * 100
    g["Não empenhado (R$)"] = (g["Dotação atualizada"] - g["Empenhado"]).clip(lower=0)
    return g.sort_values(["% empenhado", "Dotação atualizada"], ascending=[True, False]).reset_index(drop=True)


def fracao_ano(ano: int, hoje: Optional[dt.date] = None) -> float:
    """Quanto do ano ja passou (0-100). Anos encerrados = 100."""
    hoje = hoje or dt.date.today()
    if ano < hoje.year:
        return 100.0
    if ano > hoje.year:
        return 0.0
    return hoje.timetuple().tm_yday / (366 if hoje.year % 4 == 0 else 365) * 100
