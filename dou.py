# dou.py - coleta, filtro e score do Diario Oficial da Uniao (compartilhado)
# ============================================================
# Usado pelo app (fiscalizagov.py) e pelo notificador (notificar_fiscalizagov.py)
# para que os dois enxerguem exatamente os mesmos dados.
#
# Fonte: https://www.in.gov.br/leiturajornal?data=DD-MM-AAAA&secao=do1
# A pagina HTML traz o JSON completo do dia em <script id="params">.
# Cada ato vem com urlTitle, o que permite o link direto para a materia.
#
# Regra de ouro: FALHA DE COLETA NUNCA PODE VIRAR "SEM ACHADOS".
# coletar_dou() devolve (DataFrame, erros). Lista de erros vazia + DataFrame
# vazio significa "dia sem publicacao"; erros preenchidos significam "nao deu
# para ler", e quem chama deve avisar.
# ============================================================

import datetime as dt
import json
import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

LEITURAJORNAL_URL = "https://www.in.gov.br/leiturajornal"
MATERIA_URL = "https://www.in.gov.br/web/dou/-/{url_title}"

DEFAULT_SECOES_DOU = ["do1", "do2", "do3"]
SECOES_DISPONIVEIS = ["do1", "do2", "do3", "do1e", "do2e", "do3e"]
ROTULOS_SECAO = {
    "do1": "DOU 1",
    "do2": "DOU 2",
    "do3": "DOU 3",
    "do1e": "DOU 1 Extra",
    "do2e": "DOU 2 Extra",
    "do3e": "DOU 3 Extra",
}

COLUNAS = [
    "Data", "Seção", "Órgão", "Tipo", "Título", "Ementa/Resumo",
    "Link", "Página", "Edição",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FiscalizaGov/0.2)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

_PARAMS_RE = re.compile(
    r'<script id="params" type="application/json">(.*?)</script>', re.DOTALL
)


# ------------------------------------------------------------
# Utilitarios
# ------------------------------------------------------------

def normalize_text(s) -> str:
    if s is None:
        return ""
    return re.sub(r"\s+", " ", str(s)).strip()


def agora_brasilia() -> dt.datetime:
    """Data/hora de Brasilia (sem DST desde 2019). Funciona sem tzdata."""
    try:
        from zoneinfo import ZoneInfo
        return dt.datetime.now(ZoneInfo("America/Sao_Paulo"))
    except Exception:
        utc = dt.datetime.now(dt.timezone.utc)
        return utc.astimezone(dt.timezone(dt.timedelta(hours=-3)))


def agora_brasilia_str() -> str:
    return agora_brasilia().strftime("%d/%m/%Y %H:%M")


def hoje_brasilia() -> dt.date:
    return agora_brasilia().date()


# ------------------------------------------------------------
# Coleta
# ------------------------------------------------------------

class ErroColeta(Exception):
    pass


def extrair_json_params(html: str) -> dict:
    """Extrai o JSON embutido na pagina do leiturajornal."""
    m = _PARAMS_RE.search(html or "")
    if not m:
        raise ErroColeta("bloco <script id=\"params\"> nao encontrado (layout mudou ou acesso bloqueado)")
    try:
        payload = json.loads(m.group(1))
    except ValueError as e:
        raise ErroColeta(f"JSON invalido na pagina: {e}")
    if not isinstance(payload, dict) or "jsonArray" not in payload:
        raise ErroColeta("JSON sem a chave 'jsonArray'")
    return payload


def normalizar_itens(payload: dict, secao: str) -> List[Dict]:
    """Converte jsonArray do portal para o schema do FiscalizaGov."""
    itens: List[Dict] = []
    rotulo = ROTULOS_SECAO.get(secao, secao.upper())
    for raw in payload.get("jsonArray") or []:
        if not isinstance(raw, dict):
            continue
        titulo = normalize_text(raw.get("title") or raw.get("titulo"))
        tipo = normalize_text(raw.get("artType"))
        if not titulo:
            titulo = tipo
        url_title = normalize_text(raw.get("urlTitle"))
        link = MATERIA_URL.format(url_title=url_title) if url_title else ""
        itens.append({
            "Data": normalize_text(raw.get("pubDate")),
            "Seção": rotulo,
            "Órgão": normalize_text(raw.get("hierarchyStr")),
            "Tipo": tipo,
            "Título": titulo,
            "Ementa/Resumo": normalize_text(raw.get("content")),
            "Link": link,
            "Página": normalize_text(raw.get("numberPage")),
            "Edição": normalize_text(raw.get("editionNumber")),
        })
    return [x for x in itens if x["Título"] or x["Link"]]


def _baixar_secao(data: dt.date, secao: str, tentativas: int, session) -> dict:
    params = {"data": data.strftime("%d-%m-%Y"), "secao": secao}
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        try:
            r = session.get(LEITURAJORNAL_URL, params=params, headers=HEADERS, timeout=40)
            if r.status_code == 200:
                return extrair_json_params(r.text)
            ultimo = f"HTTP {r.status_code}"
        except ErroColeta as e:
            ultimo = str(e)
        except requests.RequestException as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(1.5 * (i + 1))
    raise ErroColeta(ultimo)


def coletar_dou(
    data: dt.date,
    secoes: Optional[List[str]] = None,
    tentativas: int = 3,
    session=None,
) -> Tuple[pd.DataFrame, List[str]]:
    """Coleta o DOU de uma data. Retorna (df, erros). Ver regra de ouro no topo."""
    secoes = secoes or DEFAULT_SECOES_DOU
    session = session or requests.Session()
    todos: List[Dict] = []
    erros: List[str] = []

    for secao in secoes:
        if secao not in ROTULOS_SECAO:
            erros.append(f"{secao}: seção desconhecida")
            continue
        try:
            payload = _baixar_secao(data, secao, tentativas, session)
            todos.extend(normalizar_itens(payload, secao))
        except ErroColeta as e:
            erros.append(f"{ROTULOS_SECAO[secao]}: {e}")

    df = pd.DataFrame(todos, columns=COLUNAS)
    return df, erros


# ------------------------------------------------------------
# Filtro e score
# ------------------------------------------------------------

def filtrar(df: pd.DataFrame, termos: List[str]) -> pd.DataFrame:
    termos = [t.strip().lower() for t in (termos or []) if t and t.strip()]
    if df.empty or not termos:
        return df
    blob = (
        df["Título"].fillna("").astype(str) + " "
        + df["Ementa/Resumo"].fillna("").astype(str) + " "
        + df["Órgão"].fillna("").astype(str)
    ).str.lower()
    mask = pd.Series(False, index=df.index)
    for t in termos:
        mask = mask | blob.str.contains(re.escape(t), na=False)
    return df[mask].copy()


GATILHOS = {
    "imposto": 25,
    "tribut": 25,
    "contribui": 18,
    "taxa": 18,
    "benefício": 18,
    "programa": 12,
    "fica instituído": 22,
    "fica criado": 22,
    "regulamenta": 18,
    "dispõe sobre": 12,
    "autoriza": 15,
    "estabelece": 10,
    "prorroga": 8,
    "excepcional": 10,
    "em caráter": 10,
}


def score_row(row, termos_alerta: List[str]) -> Tuple[int, List[str]]:
    """Score 0-100 por relevancia tematica (nao e indicador de irregularidade)."""
    texto = f"{row.get('Título','')} {row.get('Ementa/Resumo','')} {row.get('Órgão','')}".lower()
    score = 10
    motivos: List[str] = []
    for k, pts in GATILHOS.items():
        if k in texto:
            score += pts
            motivos.append(k)
    for t in termos_alerta or []:
        if t and t.lower() in texto:
            score += 12
            motivos.append(f"match:{t.lower()}")
    return max(0, min(100, score)), motivos[:8]


def rankear(df: pd.DataFrame, termos_alerta: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    scores, motivos = [], []
    for _, row in df.iterrows():
        sc, mot = score_row(row, termos_alerta)
        scores.append(sc)
        motivos.append(", ".join(mot))
    out = df.copy()
    out["Score"] = scores
    out["Motivos"] = motivos
    out = out.sort_values(["Score"], ascending=[False], kind="stable")
    cols = ["Score", "Motivos"] + [c for c in out.columns if c not in ("Score", "Motivos")]
    return out[cols]


# ------------------------------------------------------------
# Apresentacao: orgao, nivel de relevancia, selecao diversificada
# ------------------------------------------------------------

def orgao_raiz(hierarquia: str) -> str:
    """Primeiro nivel da hierarquia (ex.: 'Ministerio da Fazenda')."""
    return normalize_text(str(hierarquia).split("/")[0]) if hierarquia else ""


def orgao_curto(hierarquia: str, limite: int = 90) -> str:
    """'Ministério X/Secretaria Y/Coordenação Z -> Ministério X › Coordenação Z."""
    partes = [normalize_text(p) for p in str(hierarquia or "").split("/") if p.strip()]
    if not partes:
        return ""
    txt = partes[0] if len(partes) == 1 else f"{partes[0]} › {partes[-1]}"
    return txt if len(txt) <= limite else txt[: limite - 1] + "…"


NIVEL_ALTO = 60
NIVEL_MEDIO = 35


def nivel_relevancia(score) -> str:
    try:
        s = int(score)
    except (TypeError, ValueError):
        return "Baixa"
    return "Alta" if s >= NIVEL_ALTO else "Média" if s >= NIVEL_MEDIO else "Baixa"


def top_diverso(df: pd.DataFrame, n: int = 5, max_por_raiz: int = 2) -> pd.DataFrame:
    """Top n por Score, no maximo `max_por_raiz` por orgao raiz (evita 5 atos do mesmo orgao)."""
    if df.empty:
        return df
    escolhidos, contagem = [], {}
    for idx, row in df.iterrows():
        raiz = orgao_raiz(row.get("Órgão", ""))
        if contagem.get(raiz, 0) >= max_por_raiz:
            continue
        contagem[raiz] = contagem.get(raiz, 0) + 1
        escolhidos.append(idx)
        if len(escolhidos) >= n:
            break
    return df.loc[escolhidos]


def ultimo_dia_com_edicao(coletar, hoje: dt.date, max_voltas: int = 6):
    """Volta de `hoje` ate achar um dia com atos. `coletar(d)` -> (df, erros).
    Retorna (data, df, erros). Para no primeiro erro (nao mascara falha como dia vazio)."""
    d = hoje
    for _ in range(max_voltas + 1):
        df, erros = coletar(d)
        if erros or not df.empty:
            return d, df, erros
        d -= dt.timedelta(days=1)
    return hoje, pd.DataFrame(columns=COLUNAS), []
