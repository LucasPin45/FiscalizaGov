# pncp.py - licitacoes e contratos do Executivo federal (PNCP)
# ============================================================
# Fonte: API de consulta do PNCP (https://pncp.gov.br/api/consulta/v1), sem chave.
# - /contratos: contratos, termos aditivos e empenhos publicados no dia
# - /contratacoes/publicacao: processos de contratacao (dispensa e inexigibilidade)
# A API nao filtra por esfera: baixamos o dia inteiro (milhares de registros,
# paginas em paralelo) e ficamos so com esfera Federal e poder Executivo.
#
# Os "pontos de atencao" sao REGRAS EXPLICAVEIS que sinalizam onde olhar primeiro.
# Nao sao conclusao de irregularidade.
#
# Mesma regra de ouro do dou.py: falha de coleta vira erro, nunca "sem resultados".
# ============================================================

import datetime as dt
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from dou import ErroColeta, normalize_text

BASE = "https://pncp.gov.br/api/consulta/v1"
LINK_CONTRATO = "https://pncp.gov.br/app/contratos/{cnpj}/{ano}/{seq}"
LINK_EDITAL = "https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"

MODALIDADES = {8: "Dispensa", 9: "Inexigibilidade"}
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; FiscalizaGov/0.4)", "Accept": "application/json"}

COLUNAS_CONTRATOS = [
    "Data", "Órgão", "Unidade", "UF", "Fornecedor", "Documento", "Tipo", "Categoria", "Objeto",
    "Valor", "Valor inicial", "Valor acumulado", "Início vigência", "Fim vigência",
    "Processo", "Nº PNCP", "Link",
]
COLUNAS_DISPENSAS = [
    "Data", "Modalidade", "Órgão", "Unidade", "UF", "Objeto", "Amparo legal", "Situação",
    "Valor estimado", "Valor homologado", "Nº PNCP", "Link",
]

PALAVRAS_EMERGENCIA = ("emergencial", "emergência", "emergencia", "calamidade")


# ------------------------------------------------------------
# Utilitarios
# ------------------------------------------------------------

def ultimo_dia_util(hoje: dt.date) -> dt.date:
    """Ultimo dia de semana ANTES de hoje (o PNCP publica com atraso)."""
    d = hoje - dt.timedelta(days=1)
    while d.weekday() >= 5:
        d -= dt.timedelta(days=1)
    return d


def mascarar_documento(doc: str) -> str:
    """CPF aparece mascarado (LGPD); CNPJ fica inteiro."""
    d = re.sub(r"\D", "", str(doc or ""))
    if len(d) == 11:
        return f"***.{d[3:6]}.{d[6:9]}-**"
    return d


def _data_br(iso: str) -> str:
    try:
        return dt.date.fromisoformat(str(iso)[:10]).strftime("%d/%m/%Y")
    except ValueError:
        return ""


def _num(v) -> Optional[float]:
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def _federal_executivo(item: dict) -> bool:
    o = item.get("orgaoEntidade") or {}
    return o.get("esferaId") == "F" and o.get("poderId") == "E"


def fmt_brl(v) -> str:
    try:
        return "R$ " + f"{float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return ""


# ------------------------------------------------------------
# Coleta
# ------------------------------------------------------------

def _get(session, path: str, params: dict, tentativas: int = 5) -> dict:
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        espera = 1.5 * (i + 1)
        try:
            r = session.get(f"{BASE}{path}", params=params, headers=HEADERS, timeout=60)
            if r.status_code == 204:
                return {"data": [], "totalPaginas": 0}
            if r.status_code == 200:
                return r.json()
            ultimo = f"HTTP {r.status_code}"
            if r.status_code == 429:  # limite de requisicoes: espera mais
                espera = 5.0 * (i + 1)
        except (requests.RequestException, ValueError) as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(espera)
    raise ErroColeta(ultimo)


def _todas_paginas(session, path: str, params: dict, workers: int = 3) -> Tuple[List[dict], List[str]]:
    primeira = _get(session, path, {**params, "pagina": 1})
    itens = list(primeira.get("data") or [])
    total = int(primeira.get("totalPaginas") or 0)
    erros: List[str] = []
    if total > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_get, session, path, {**params, "pagina": p}): p for p in range(2, total + 1)}
            for f in as_completed(futs):
                try:
                    itens += f.result().get("data") or []
                except ErroColeta as e:
                    erros.append(f"página {futs[f]}: {e}")
    return itens, erros


def coletar_contratos(data: dt.date, session=None) -> Tuple[pd.DataFrame, List[str]]:
    session = session or requests.Session()
    d = data.strftime("%Y%m%d")
    try:
        itens, erros = _todas_paginas(session, "/contratos",
                                      {"dataInicial": d, "dataFinal": d, "tamanhoPagina": 500})
    except ErroColeta as e:
        return pd.DataFrame(columns=COLUNAS_CONTRATOS), [f"Contratos: {e}"]
    df = normalizar_contratos([x for x in itens if _federal_executivo(x)])
    return df, [f"Contratos: {x}" for x in erros]


def coletar_dispensas(data: dt.date, session=None) -> Tuple[pd.DataFrame, List[str]]:
    session = session or requests.Session()
    d = data.strftime("%Y%m%d")
    todos: List[dict] = []
    erros: List[str] = []
    for cod, nome in MODALIDADES.items():
        try:
            itens, er = _todas_paginas(session, "/contratacoes/publicacao",
                                       {"dataInicial": d, "dataFinal": d,
                                        "codigoModalidadeContratacao": cod, "tamanhoPagina": 50})
            todos += [x for x in itens if _federal_executivo(x)]
            erros += [f"{nome}: {x}" for x in er]
        except ErroColeta as e:
            erros.append(f"{nome}: {e}")
    return normalizar_dispensas(todos), erros


# ------------------------------------------------------------
# Normalizacao
# ------------------------------------------------------------

def _partes_controle(item: dict, ano_key: str, seq_key: str) -> Tuple[str, str, str]:
    cnpj = (item.get("orgaoEntidade") or {}).get("cnpj", "")
    return cnpj, str(item.get(ano_key, "")), str(item.get(seq_key, ""))


def normalizar_contratos(itens: List[dict]) -> pd.DataFrame:
    linhas = []
    for x in itens:
        cnpj, ano, seq = _partes_controle(x, "anoContrato", "sequencialContrato")
        un = x.get("unidadeOrgao") or {}
        linhas.append({
            "Data": _data_br(x.get("dataPublicacaoPncp")),
            "Órgão": normalize_text((x.get("orgaoEntidade") or {}).get("razaoSocial")),
            "Unidade": normalize_text(un.get("nomeUnidade")),
            "UF": un.get("ufSigla") or "",
            "Fornecedor": normalize_text(x.get("nomeRazaoSocialFornecedor")),
            "Documento": mascarar_documento(x.get("niFornecedor")),
            "Tipo": normalize_text((x.get("tipoContrato") or {}).get("nome")),
            "Categoria": normalize_text((x.get("categoriaProcesso") or {}).get("nome")),
            "Objeto": normalize_text(x.get("objetoContrato")),
            "Valor": _num(x.get("valorGlobal")),
            "Valor inicial": _num(x.get("valorInicial")),
            "Valor acumulado": _num(x.get("valorAcumulado")),
            "Início vigência": _data_br(x.get("dataVigenciaInicio")),
            "Fim vigência": _data_br(x.get("dataVigenciaFim")),
            "Processo": normalize_text(x.get("processo")),
            "Nº PNCP": x.get("numeroControlePNCP") or "",
            "Link": LINK_CONTRATO.format(cnpj=cnpj, ano=ano, seq=seq) if cnpj and ano and seq else "",
        })
    return pd.DataFrame(linhas, columns=COLUNAS_CONTRATOS)


def normalizar_dispensas(itens: List[dict]) -> pd.DataFrame:
    linhas = []
    for x in itens:
        cnpj, ano, seq = _partes_controle(x, "anoCompra", "sequencialCompra")
        un = x.get("unidadeOrgao") or {}
        ap = x.get("amparoLegal") or {}
        linhas.append({
            "Data": _data_br(x.get("dataPublicacaoPncp")),
            "Modalidade": normalize_text(x.get("modalidadeNome")),
            "Órgão": normalize_text((x.get("orgaoEntidade") or {}).get("razaoSocial")),
            "Unidade": normalize_text(un.get("nomeUnidade")),
            "UF": un.get("ufSigla") or "",
            "Objeto": normalize_text(x.get("objetoCompra")),
            "Amparo legal": normalize_text(f'{ap.get("nome", "")} — {ap.get("descricao", "")}') if ap else "",
            "Situação": normalize_text(x.get("situacaoCompraNome")),
            "Valor estimado": _num(x.get("valorTotalEstimado")),
            "Valor homologado": _num(x.get("valorTotalHomologado")),
            "Nº PNCP": x.get("numeroControlePNCP") or "",
            "Link": LINK_EDITAL.format(cnpj=cnpj, ano=ano, seq=seq) if cnpj and ano and seq else "",
        })
    return pd.DataFrame(linhas, columns=COLUNAS_DISPENSAS)


# ------------------------------------------------------------
# Pontos de atencao (regras explicaveis)
# ------------------------------------------------------------

def _tem_emergencia(texto: str) -> bool:
    t = (texto or "").lower()
    return any(p in t for p in PALAVRAS_EMERGENCIA)


def _fim_antes_do_inicio_ou_longo(inicio: str, fim: str, anos: int = 5) -> bool:
    try:
        i = dt.datetime.strptime(inicio, "%d/%m/%Y").date()
        f = dt.datetime.strptime(fim, "%d/%m/%Y").date()
    except ValueError:
        return False
    return (f - i).days > 365 * anos


def avaliar_contratos(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona Score (0-100) e 'Pontos de atenção'. Ordena do maior para o menor."""
    if df.empty:
        return df.assign(**{"Score": pd.Series(dtype=int), "Pontos de atenção": pd.Series(dtype=str)})
    d = df.copy()
    rec = d.groupby(["Órgão", "Documento"])["Nº PNCP"].transform("size")
    dia = d.groupby(["Órgão", "Documento", "Data"])["Nº PNCP"].transform("size")
    scores, pontos = [], []
    for i, r in d.iterrows():
        s, p = 0, []
        v = r["Valor"] or 0
        if v >= 10_000_000:
            s += 45; p.append("valor muito alto (≥ R$ 10 mi)")
        elif v >= 1_000_000:
            s += 20; p.append("valor alto (≥ R$ 1 mi)")
        if r["Tipo"] != "Empenho" and 0 < v <= 1:
            s += 10; p.append("valor simbólico (≤ R$ 1): possível erro de cadastro")
        if _tem_emergencia(r["Objeto"]):
            s += 25; p.append("menciona emergência/calamidade")
        if "aditivo" in r["Tipo"].lower():
            s += 10; p.append("termo aditivo")
        ini, acu = r["Valor inicial"], r["Valor acumulado"]
        if ini and acu and ini > 0 and acu / ini > 1.25:
            s += 30; p.append(f"acréscimo de {round((acu / ini - 1) * 100)}% sobre o valor inicial (limite usual: 25%)")
        if dia[i] >= 3:
            s += 25; p.append(f"{dia[i]} contratos do mesmo fornecedor com o mesmo órgão no dia (possível fracionamento)")
        elif rec[i] >= 3:
            s += 15; p.append(f"fornecedor recorrente: {rec[i]} contratos neste órgão no período")
        if _fim_antes_do_inicio_ou_longo(r["Início vigência"], r["Fim vigência"]):
            s += 10; p.append("vigência acima de 5 anos")
        scores.append(min(100, s))
        pontos.append(" | ".join(p))
    d["Score"] = scores
    d["Pontos de atenção"] = pontos
    return d.sort_values(["Score", "Valor"], ascending=[False, False], kind="stable").reset_index(drop=True)


def avaliar_dispensas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.assign(**{"Score": pd.Series(dtype=int), "Pontos de atenção": pd.Series(dtype=str)})
    d = df.copy()
    scores, pontos = [], []
    for _, r in d.iterrows():
        s, p = 0, []
        v = r["Valor estimado"] or 0
        amparo = r["Amparo legal"].lower()
        if v >= 10_000_000:
            s += 45; p.append("valor estimado muito alto (≥ R$ 10 mi)")
        elif v >= 1_000_000:
            s += 25; p.append("valor estimado alto (≥ R$ 1 mi)")
        if "art. 75, viii" in amparo or "emerg" in amparo or _tem_emergencia(r["Objeto"]):
            s += 30; p.append("dispensa por emergência/calamidade")
        if r["Modalidade"].lower().startswith("inexig"):
            s += 10; p.append("inexigibilidade (sem competição)")
            if "art. 74, iii" in amparo and v >= 500_000:
                s += 20; p.append("serviço técnico especializado de alto valor (art. 74, III)")
        hom = r["Valor homologado"]
        if hom and v and hom > v * 1.01:
            s += 20; p.append("valor homologado acima do estimado")
        scores.append(min(100, s))
        pontos.append(" | ".join(p))
    d["Score"] = scores
    d["Pontos de atenção"] = pontos
    return d.sort_values(["Score", "Valor estimado"], ascending=[False, False], kind="stable").reset_index(drop=True)
