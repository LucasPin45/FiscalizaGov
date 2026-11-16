# servidores.py - cargo e funcao dos viajantes, pelo cadastro de servidores do Portal da Transparencia
# ============================================================
# Fonte: arquivo mensal "Servidores" da CGU (Executivo civil, SIAPE: ~83 MB; Banco Central: 0,26 MB)
#   https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores/AAAAMM_Servidores_SIAPE.zip
# A API de consulta individual exige o CPF inteiro; o arquivo traz nome e CPF MASCARADO (***.999.999-**), no mesmo formato do
# arquivo de viagens. O cruzamento e por NOME COMPLETO (sem acento, maiusculas) + os 6 digitos visiveis do CPF.
#
# LIMITES: militares das Forcas Armadas e parte da seguranca publica nao tem cadastro publicado (na amostra de 2026, 65% dos
# viajantes foram localizados; dos nao localizados, 68% do Ministerio da Justica e 17% da Defesa). Quem nao e servidor
# (consultor, colaborador eventual) tambem nao aparece. O cadastro e do mes de referencia: a pessoa pode ter mudado de posto.
# Falha de download vira erro (o arquivo anterior fica); nunca vira "cargo inexistente".
# ============================================================

import datetime as dt
import json
import os
import re
import unicodedata
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd
import requests

BASE_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores"
FONTES = {"siape": "{ym}_Servidores_SIAPE.zip", "bacen": "{ym}_Servidores_BACEN.zip"}
COLS_CADASTRO = ["NOME", "CPF", "DESCRICAO_CARGO", "SIGLA_FUNCAO", "NIVEL_FUNCAO", "FUNCAO", "ATIVIDADE", "ORG_LOTACAO",
                 "ORGSUP_LOTACAO", "SITUACAO_VINCULO", "TIPO_VINCULO"]
SEM_INFO = ("sem informa", "-1", "")
COLUNAS = ["chave", "cpf", "nome", "status", "cargo", "atividade", "funcao_codigo", "funcao_desc", "lotacao", "orgao_superior",
           "situacao", "fonte", "referencia"]

_FUNCAO_RE = re.compile(r"^(CCX|FEX|CCE|FCE)[- ]?(\d{2})(\d{2})$", re.I)


# ------------------------------------------------------------
# Normalizacao e chaves
# ------------------------------------------------------------

def normalizar_nome(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii").upper()
    return re.sub(r"\s+", " ", s).strip()


def chave(nome, cpf) -> str:
    return f"{normalizar_nome(nome)}|{str(cpf or '').strip()}"


def _sem_info(v) -> bool:
    return str(v or "").strip().lower().startswith(SEM_INFO[0]) or str(v or "").strip() in SEM_INFO


def decodificar_funcao(codigo: str) -> str:
    """'CCX-0115' -> 'CCE-1.15 (DAS 5)'; 'FEX-0113' -> 'FCE-1.13 (DAS 4)'. Outros codigos voltam como vieram."""
    import cargos
    cod = str(codigo or "").strip()
    m = _FUNCAO_RE.match(cod)
    if not m:
        return "" if _sem_info(cod) else cod
    sigla = {"CCX": "CCE", "FEX": "FCE"}.get(m.group(1).upper(), m.group(1).upper())
    return cargos.rotulo_codigo(f"{sigla}-{int(m.group(2))}.{int(m.group(3)):02d}")


# ------------------------------------------------------------
# Leitura e consolidacao do cadastro
# ------------------------------------------------------------

def ler_cadastro(caminho: str, fonte: str, chaves: Optional[Iterable[str]] = None, tamanho: int = 200_000) -> pd.DataFrame:
    """Le o CSV do cadastro em blocos; se `chaves` vier, guarda so as linhas desses viajantes (poupa memoria)."""
    alvo = set(chaves) if chaves is not None else None
    partes = []
    for bloco in pd.read_csv(caminho, sep=";", encoding="latin-1", dtype=str, usecols=lambda c: c in COLS_CADASTRO,
                             chunksize=tamanho):
        bloco = bloco.fillna("")
        bloco["chave"] = [chave(n, c) for n, c in zip(bloco["NOME"], bloco["CPF"])]
        if alvo is not None:
            bloco = bloco[bloco["chave"].isin(alvo)]
        if not bloco.empty:
            bloco["fonte"] = fonte
            partes.append(bloco)
    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame(columns=COLS_CADASTRO + ["chave", "fonte"])


def consolidar(cad: pd.DataFrame, referencia: str) -> pd.DataFrame:
    """Uma linha por pessoa: cargo (carreira), atividade, funcao (codigo e descricao), lotacao e orgao superior."""
    linhas = []
    for k, g in cad.groupby("chave", sort=False):
        cargos_ = [c for c in g["DESCRICAO_CARGO"] if not _sem_info(c)]
        ativ = [a for a in g["ATIVIDADE"] if not _sem_info(a)]
        com_funcao = g[~g["SIGLA_FUNCAO"].map(_sem_info) & ~g["NIVEL_FUNCAO"].map(_sem_info)]
        cod, desc = "", ""
        if not com_funcao.empty:
            # a funcao de maior nivel (numero depois da sigla) manda
            ordem = com_funcao.assign(_n=pd.to_numeric(com_funcao["NIVEL_FUNCAO"], errors="coerce").fillna(0)) \
                .sort_values("_n", ascending=False).iloc[0]
            cod = f'{ordem["SIGLA_FUNCAO"]}-{ordem["NIVEL_FUNCAO"]}'
            desc = ordem["FUNCAO"]
        ativos = g[g["SITUACAO_VINCULO"].str.upper().str.contains("ATIVO", na=False)]
        base = (ativos if not ativos.empty else g).iloc[0]
        linhas.append({
            "chave": k, "cargo": cargos_[0] if cargos_ else "", "atividade": ativ[0] if ativ else "",
            "funcao_codigo": cod, "funcao_desc": "" if _sem_info(desc) else desc,
            "lotacao": "" if _sem_info(base["ORG_LOTACAO"]) else base["ORG_LOTACAO"],
            "orgao_superior": "" if _sem_info(base["ORGSUP_LOTACAO"]) else base["ORGSUP_LOTACAO"],
            "situacao": base["SITUACAO_VINCULO"], "fonte": base["fonte"],
        })
    out = pd.DataFrame(linhas, columns=["chave", "cargo", "atividade", "funcao_codigo", "funcao_desc", "lotacao",
                                        "orgao_superior", "situacao", "fonte"])
    out["status"] = "localizado"
    out["referencia"] = referencia
    return out


def montar_tabela(viajantes: pd.DataFrame, consolidado: pd.DataFrame, referencia: str) -> pd.DataFrame:
    """Uma linha por viajante (cpf + nome): localizado ou nao localizado no cadastro."""
    v = viajantes[["cpf", "nome"]].drop_duplicates().copy()
    v["chave"] = [chave(n, c) for n, c in zip(v["nome"], v["cpf"])]
    m = v.merge(consolidado, on="chave", how="left")
    m["status"] = m["status"].fillna("nao_localizado")
    m["referencia"] = m["referencia"].fillna(referencia)
    for c in ("cargo", "atividade", "funcao_codigo", "funcao_desc", "lotacao", "orgao_superior", "situacao", "fonte"):
        m[c] = m[c].fillna("")
    return m[COLUNAS]


# ------------------------------------------------------------
# Descoberta do mes mais recente e download
# ------------------------------------------------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (FiscalizaGov)", "Referer": "https://portaldatransparencia.gov.br/"}


def meses_candidatos(hoje: Optional[dt.date] = None, n: int = 8):
    d = (hoje or dt.date.today()).replace(day=1)
    for _ in range(n):
        d = (d - dt.timedelta(days=1)).replace(day=1)
        yield d.strftime("%Y%m")


def mes_mais_recente(session=None, hoje: Optional[dt.date] = None) -> Optional[str]:
    """Primeiro mes (do mais novo para o mais antigo) cujo zip do SIAPE existe. O site responde 403 ao que ainda nao foi publicado."""
    session = session or requests.Session()
    for ym in meses_candidatos(hoje):
        try:
            r = session.get(f"{BASE_URL}/{FONTES['siape'].format(ym=ym)}", headers={**HEADERS, "Range": "bytes=0-0"}, timeout=40)
        except requests.RequestException:
            continue
        if r.status_code in (200, 206):
            return ym
    return None


# ------------------------------------------------------------
# Tabela guardada (data/viagens_cargos.csv)
# ------------------------------------------------------------
PASTA_DADOS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
URL_DADOS_PADRAO = "https://raw.githubusercontent.com/LucasPin45/FiscalizaGov/main/data"
_TIPOS = {c: str for c in COLUNAS}


def salvar(tabela: pd.DataFrame, referencia: str, pasta: str = "") -> None:
    pasta = pasta or PASTA_DADOS
    os.makedirs(pasta, exist_ok=True)
    tabela.to_csv(os.path.join(pasta, "viagens_cargos.csv"), index=False, encoding="utf-8")
    with open(os.path.join(pasta, "viagens_cargos_meta.json"), "w", encoding="utf-8") as f:
        json.dump({"referencia": referencia, "pessoas": int(len(tabela)),
                   "localizadas": int((tabela["status"] == "localizado").sum())}, f, indent=1)


def ler(base: str = "", timeout: int = 40) -> Optional[Tuple[pd.DataFrame, dict]]:
    base = base or os.getenv("FISCALIZAGOV_DATA_URL", URL_DADOS_PADRAO)
    try:
        if base.startswith("http"):
            r = requests.get(f"{base}/viagens_cargos.csv", timeout=timeout)
            if r.status_code != 200:
                return None
            import io
            df = pd.read_csv(io.StringIO(r.text), dtype=_TIPOS, keep_default_na=False)
            m = requests.get(f"{base}/viagens_cargos_meta.json", timeout=timeout)
            meta = m.json() if m.status_code == 200 else {}
        else:
            df = pd.read_csv(os.path.join(base, "viagens_cargos.csv"), dtype=_TIPOS, keep_default_na=False)
            with open(os.path.join(base, "viagens_cargos_meta.json"), encoding="utf-8") as f:
                meta = json.load(f)
    except (OSError, ValueError, requests.RequestException):
        return None
    return (df, meta) if set(COLUNAS).issubset(df.columns) else None


def indice(tabela: pd.DataFrame) -> Dict[str, dict]:
    """chave (nome normalizado + cpf parcial) -> linha, para consulta rapida na tela."""
    return {r["chave"]: r for r in tabela.to_dict("records")}
