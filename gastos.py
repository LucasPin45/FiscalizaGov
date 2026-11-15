# gastos.py - gastos do governo federal por orgao (Portal da Transparencia)
# ============================================================
# Fonte: /despesas/por-orgao?ano=AAAA&orgaoSuperior=COD  (execucao orcamentaria)
#   Empenhado  = compromisso assumido (reserva do orcamento)
#   Liquidado  = servico/bem entregue e reconhecido
#   Pago       = dinheiro que efetivamente saiu
# A API exige um orgao superior por consulta; ha cerca de 60 deles, entao a coleta
# de um ano leva 1 a 2 minutos (limite de ~90 requisicoes/minuto). Chave gratuita
# no cabecalho `chave-api-dados` (secret TRANSPARENCIA_API_KEY).
#
# Cuidado de leitura: encargos da divida, refinanciamento e transferencias a
# estados e municipios nao sao "gasto de um ministerio"; ficam fora do ranking
# por padrao para nao distorcer a comparacao entre orgaos.
#
# Falha de consulta vira erro; nunca "orgao sem gasto".
# ============================================================

import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from dou import ErroColeta

BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
POR_PAGINA = 15
PAUSA_S = 0.75

# Orgaos superiores (codigo SIAFI) levantados no proprio Portal da Transparencia.
ORGAOS_SUPERIORES: Dict[str, str] = {
    "01000": "Câmara dos Deputados", "02000": "Senado Federal", "03000": "Tribunal de Contas da União",
    "10000": "Supremo Tribunal Federal", "11000": "Superior Tribunal de Justiça", "12000": "Justiça Federal",
    "13000": "Justiça Militar", "14000": "Justiça Eleitoral", "15000": "Justiça do Trabalho",
    "16000": "Justiça do DF e Territórios", "17000": "Conselho Nacional de Justiça",
    "20000": "Presidência da República", "22000": "Ministério da Agricultura e Pecuária",
    "23000": "Ministério do Bem Estar Social (em extinção)", "24000": "Ministério da Ciência, Tecnologia e Inovação",
    "25000": "Ministério da Fazenda", "26000": "Ministério da Educação",
    "28000": "Ministério do Desenvolvimento, Indústria, Comércio e Serviços", "29000": "Defensoria Pública da União",
    "30000": "Ministério da Justiça e Segurança Pública", "32000": "Ministério de Minas e Energia",
    "33000": "Ministério da Previdência Social", "34000": "Ministério Público da União",
    "35000": "Ministério das Relações Exteriores", "36000": "Ministério da Saúde", "37000": "Controladoria-Geral da União",
    "38000": "Ministério do Trabalho e Emprego", "39000": "Ministério dos Transportes",
    "40000": "Ministério do Trabalho e Emprego", "41000": "Ministério das Comunicações", "42000": "Ministério da Cultura",
    "44000": "Ministério do Meio Ambiente e Mudança do Clima",
    "46000": "Ministério da Gestão e da Inovação em Serviços Públicos", "47000": "Ministério do Planejamento e Orçamento",
    "49000": "Ministério do Desenvolvimento Agrário e Agricultura Familiar", "50000": "Gabinete do Ministro Extraordinário de Projetos Especiais",
    "51000": "Ministério do Esporte", "52000": "Ministério da Defesa",
    "53000": "Ministério da Integração e do Desenvolvimento Regional", "54000": "Ministério do Turismo",
    "55000": "Ministério do Desenvolvimento e Assistência Social, Família e Combate à Fome",
    "56000": "Ministério das Cidades", "57000": "Ministério das Mulheres, Igualdade Racial, da Juventude e dos Direitos Humanos",
    "58000": "Ministério da Pesca e Aquicultura", "59000": "Conselho Nacional do Ministério Público",
    "60000": "Gabinete da Vice-Presidência da República", "61000": "Secretaria de Assuntos Estratégicos",
    "62000": "Secretaria de Aviação Civil", "63000": "Advocacia-Geral da União",
    "64000": "Ministério dos Direitos Humanos e Cidadania", "65000": "Ministério das Mulheres",
    "66000": "Controladoria-Geral da União", "67000": "Ministério da Igualdade Racial",
    "68000": "Ministério de Portos e Aeroportos", "69000": "Ministério do Empreendedorismo, da Microempresa e da Empresa de Pequeno Porte",
    "71000": "Encargos Financeiros da União", "73000": "Transferências a Estados, DF e Municípios",
    "74000": "Operações Oficiais de Crédito", "75000": "Refinanciamento da Dívida Pública Mobiliária Federal",
    "80000": "Entidades em extinção ou privatização (L. 8.029/90)", "81000": "Ministério dos Direitos Humanos e Cidadania",
    "82000": "Ministério da Segurança Pública", "83000": "Ministério dos Povos Indígenas",
    "84000": "Ministério dos Povos Indígenas", "91000": "Entidades que utilizam conta única (TCT)",
}
# Fora do ranking por padrao: nao sao gasto proprio de um ministerio.
NAO_FINALISTICOS = {"71000", "73000", "74000", "75000", "90000"}
# Administracao direta da Fazenda (Tesouro Nacional): concentra, entre outros, o servico da divida
# publica (juros e amortizacao), que nao e gasto de custeio do ministerio. Em 2026 ela sozinha
# passa de R$ 2 tri e esconderia todos os outros orgaos no ranking.
NAO_FINALISTICOS_ORGAOS = {"25000"}

COLUNAS = ["Ano", "Órgão superior", "Cód. superior", "Órgão", "Cód. órgão", "Empenhado", "Liquidado", "Pago"]
METRICAS = ["Pago", "Liquidado", "Empenhado"]


def valor_br(txt) -> float:
    """'126.801.192.978,82' -> 126801192978.82"""
    try:
        return float(str(txt).replace(".", "").replace(",", "."))
    except ValueError:
        return 0.0


def fmt_brl_curto(v: float) -> str:
    """R$ 1,23 bi / R$ 456,7 mi / R$ 12 mil"""
    v = float(v or 0)
    for lim, suf in ((1e12, "tri"), (1e9, "bi"), (1e6, "mi"), (1e3, "mil")):
        if abs(v) >= lim:
            return "R$ " + f"{v / lim:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") + f" {suf}"
    return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _get(session, chave: str, params: dict, tentativas: int = 4) -> list:
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        espera = 2.0 * (i + 1)
        try:
            r = session.get(f"{BASE}/despesas/por-orgao", params=params,
                            headers={"chave-api-dados": chave, "Accept": "application/json"}, timeout=60)
            if r.status_code == 200:
                d = r.json()
                return d if isinstance(d, list) else []
            ultimo = f"HTTP {r.status_code}"
            if r.status_code in (401, 403):
                raise ErroColeta(f"chave da API recusada ({r.status_code})")
            if r.status_code == 429:
                espera = 15.0 * (i + 1)
        except (requests.RequestException, ValueError) as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(espera)
    raise ErroColeta(ultimo)


def coletar_superior(ano: int, codigo: str, chave: str, session=None, pausa: float = PAUSA_S) -> List[dict]:
    session = session or requests.Session()
    linhas: List[dict] = []
    for pag in range(1, 40):
        itens = _get(session, chave, {"ano": ano, "orgaoSuperior": codigo, "pagina": pag})
        linhas += itens
        if pausa:
            time.sleep(pausa)
        if len(itens) < POR_PAGINA:
            break
    return linhas


def normalizar(linhas: List[dict]) -> pd.DataFrame:
    rows = [{
        "Ano": int(x.get("ano") or 0),
        "Órgão superior": str(x.get("orgaoSuperior", "")).strip(),
        "Cód. superior": str(x.get("codigoOrgaoSuperior", "")),
        "Órgão": str(x.get("orgao", "")).strip(),
        "Cód. órgão": str(x.get("codigoOrgao", "")),
        "Empenhado": valor_br(x.get("empenhado")),
        "Liquidado": valor_br(x.get("liquidado")),
        "Pago": valor_br(x.get("pago")),
    } for x in linhas]
    return pd.DataFrame(rows, columns=COLUNAS)


def coletar_ano(ano: int, chave: str, session=None, progresso=None, codigos: Optional[List[str]] = None,
                pausa: float = PAUSA_S) -> Tuple[pd.DataFrame, List[str]]:
    """Coleta todos os orgaos superiores de um ano. Retorna (df, erros). Para se a chave for recusada."""
    session = session or requests.Session()
    codigos = codigos or list(ORGAOS_SUPERIORES)
    todas: List[dict] = []
    erros: List[str] = []
    for i, cod in enumerate(codigos, 1):
        try:
            todas += coletar_superior(ano, cod, chave, session, pausa)
        except ErroColeta as e:
            erros.append(f"{ORGAOS_SUPERIORES.get(cod, cod)}: {e}")
            if "chave" in str(e):
                break
        if progresso:
            progresso(i, len(codigos))
    df = normalizar(todas)
    # o mesmo orgao pode vir sob dois superiores com codigos antigos: soma duplicatas exatas
    df = df.drop_duplicates(subset=["Ano", "Cód. superior", "Cód. órgão"])
    return df, erros


# ------------------------------------------------------------
# Rankings
# ------------------------------------------------------------

def filtrar_finalisticos(df: pd.DataFrame, incluir_encargos: bool) -> pd.DataFrame:
    if df.empty or incluir_encargos:
        return df
    return df[~df["Cód. superior"].isin(NAO_FINALISTICOS) & ~df["Cód. órgão"].isin(NAO_FINALISTICOS_ORGAOS)]


def ranking_superiores(df: pd.DataFrame, metrica: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["Órgão superior", "Empenhado", "Liquidado", "Pago", "% do total", "% pago do empenhado"])
    g = df.groupby("Órgão superior")[["Empenhado", "Liquidado", "Pago"]].sum()
    total = g[metrica].sum()
    g["% do total"] = (g[metrica] / total * 100) if total else 0.0
    g["% pago do empenhado"] = (g["Pago"] / g["Empenhado"] * 100).where(g["Empenhado"] > 0, 0.0)
    return g.sort_values(metrica, ascending=False).reset_index()


def ranking_orgaos(df: pd.DataFrame, metrica: str) -> pd.DataFrame:
    if df.empty:
        return df
    total = df[metrica].sum()
    d = df.copy()
    d["% do total"] = (d[metrica] / total * 100) if total else 0.0
    d["% pago do empenhado"] = (d["Pago"] / d["Empenhado"] * 100).where(d["Empenhado"] > 0, 0.0)
    return d.sort_values(metrica, ascending=False).reset_index(drop=True)


def concentracao(ranking: pd.DataFrame, n: int) -> float:
    """% do total concentrado nos n maiores."""
    return float(ranking["% do total"].head(n).sum()) if not ranking.empty else 0.0
