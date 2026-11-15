# sancoes.py - cruzamento de fornecedores com CEIS e CNEP (CGU / Portal da Transparencia)
# ============================================================
# Fonte: https://api.portaldatransparencia.gov.br/api-de-dados/{ceis,cnep}
# Exige chave gratuita no cabecalho `chave-api-dados` (secret TRANSPARENCIA_API_KEY).
# Limite da API: ~90 requisicoes/minuto de dia, entao a consulta e sequencial e
# limitada a um numero de CNPJs por vez.
#
# ATENCAO (leitura de controle): estar no CEIS/CNEP nao significa, por si so, que o
# contrato seja irregular. O alcance da sancao varia: impedimento (Lei 14.133, art.
# 156, III) vale so para o ente sancionador; inidoneidade (art. 156, IV) vale para
# toda a Administracao. Por isso mostramos tipo, orgao sancionador e abrangencia.
#
# Falha de consulta vira erro; nunca "empresa sem sancao".
# ============================================================

import datetime as dt
import re
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

from dou import ErroColeta

BASE = "https://api.portaldatransparencia.gov.br/api-de-dados"
CADASTROS = ("ceis", "cnep")
POR_PAGINA = 15
MAX_PAGINAS = 5
PAUSA_S = 0.75  # ~80 req/min, abaixo do limite de 90


def so_digitos(doc) -> str:
    return re.sub(r"\D", "", str(doc or ""))


def _data(txt) -> Optional[dt.date]:
    try:
        return dt.datetime.strptime(str(txt).strip(), "%d/%m/%Y").date()
    except ValueError:
        return None


def _get(session, chave: str, cadastro: str, cnpj: str, pagina: int, tentativas: int = 4) -> list:
    ultimo = "erro desconhecido"
    for i in range(tentativas):
        espera = 2.0 * (i + 1)
        try:
            r = session.get(f"{BASE}/{cadastro}",
                            params={"pagina": pagina, "codigoSancionado": cnpj},
                            headers={"chave-api-dados": chave, "Accept": "application/json"}, timeout=60)
            if r.status_code == 200:
                dados = r.json()
                return dados if isinstance(dados, list) else []
            ultimo = f"HTTP {r.status_code}"
            if r.status_code in (401, 403):
                raise ErroColeta(f"{cadastro.upper()}: chave da API recusada ({r.status_code})")
            if r.status_code == 429:
                espera = 15.0 * (i + 1)
        except (requests.RequestException, ValueError) as e:
            ultimo = type(e).__name__
        if i < tentativas - 1:
            time.sleep(espera)
    raise ErroColeta(f"{cadastro.upper()}: {ultimo}")


def _simplificar(x: dict, cadastro: str) -> dict:
    fund = (x.get("fundamentacao") or [{}])[0].get("codigo", "")
    org = x.get("orgaoSancionador") or {}
    return {
        "cadastro": cadastro.upper(),
        "tipo": (x.get("tipoSancao") or {}).get("descricaoResumida", ""),
        "inicio": x.get("dataInicioSancao", ""),
        "fim": x.get("dataFimSancao", ""),
        "orgao": org.get("nome", "") or (x.get("fonteSancao") or {}).get("nomeExibicao", ""),
        "esfera": org.get("esfera", ""),
        "uf": org.get("siglaUf", ""),
        "fundamentacao": fund,
        "abrangencia": x.get("abrangenciaDefinidaDecisaoJudicial", ""),
        "link": x.get("linkPublicacao", ""),
    }


def consultar_cnpj(cnpj: str, chave: str, session=None, pausa: float = PAUSA_S) -> List[dict]:
    """Todas as sancoes (CEIS + CNEP) de um CNPJ. Levanta ErroColeta se a consulta falhar."""
    session = session or requests.Session()
    cnpj = so_digitos(cnpj)
    achadas: List[dict] = []
    for cad in CADASTROS:
        for pag in range(1, MAX_PAGINAS + 1):
            itens = _get(session, chave, cad, cnpj, pag)
            achadas += [_simplificar(x, cad) for x in itens]
            if pausa:
                time.sleep(pausa)
            if len(itens) < POR_PAGINA:
                break
    return achadas


def status_na_data(sancao: dict, data: dt.date) -> str:
    """'vigente', 'encerrada', 'futura' ou 'indeterminada' em relacao a `data`."""
    ini, fim = _data(sancao.get("inicio")), _data(sancao.get("fim"))
    if ini and ini > data:
        return "futura"
    if fim and fim < data:
        return "encerrada"
    if ini or fim:
        return "vigente"
    return "indeterminada"


def verificar(cnpjs: List[str], chave: str, session=None, progresso=None,
              pausa: float = PAUSA_S, consultar=None) -> Tuple[Dict[str, List[dict]], List[str]]:
    """Consulta cada CNPJ (14 digitos). Retorna ({cnpj: sancoes}, erros). Para ao ver chave recusada.
    `consultar(cnpj)` permite injetar uma versao com cache (a do app)."""
    session = session or requests.Session()
    consultar = consultar or (lambda c: consultar_cnpj(c, chave, session, pausa))
    mapa: Dict[str, List[dict]] = {}
    erros: List[str] = []
    unicos = [c for c in dict.fromkeys(so_digitos(c) for c in cnpjs) if len(c) == 14]
    for i, c in enumerate(unicos, 1):
        try:
            mapa[c] = consultar(c)
        except ErroColeta as e:
            erros.append(f"{c}: {e}")
            if "chave" in str(e):
                break
        if progresso:
            progresso(i, len(unicos))
    return mapa, erros


def descrever(s: dict, status: str) -> str:
    base = f'{s["cadastro"]}: {s["tipo"]} ({s["orgao"]}{" - " + s["uf"] if s["uf"] else ""}), {s["inicio"]} a {s["fim"] or "sem data final"}'
    if status == "vigente" and s.get("abrangencia"):
        base += f' · alcance: {s["abrangencia"]}'
    return base


def aplicar_sancoes(df: pd.DataFrame, mapa: Dict[str, List[dict]]) -> pd.DataFrame:
    """Soma pontos de atencao no df de contratos (coluna Documento = CNPJ do fornecedor)."""
    if df.empty:
        return df
    d = df.copy()
    novos_scores, novos_pontos = [], []
    for _, r in d.iterrows():
        score, pontos = int(r["Score"]), [p for p in str(r["Pontos de atenção"]).split(" | ") if p]
        sancoes = mapa.get(so_digitos(r["Documento"]), [])
        data = _data(r["Data"]) or dt.date.today()
        vig = [(s, status_na_data(s, data)) for s in sancoes]
        for s, st_ in vig:
            if st_ == "vigente":
                score += 50
                pontos.append("🚫 fornecedor com sanção VIGENTE na publicação — " + descrever(s, st_))
                break
        else:
            hist = [s for s, st_ in vig if st_ in ("encerrada", "indeterminada")]
            if hist:
                score += 10
                pontos.append("fornecedor com histórico de sanção (já encerrada ou sem datas) — " + descrever(hist[0], "encerrada"))
        novos_scores.append(min(100, score))
        novos_pontos.append(" | ".join(pontos))
    d["Score"], d["Pontos de atenção"] = novos_scores, novos_pontos
    return d.sort_values(["Score", "Valor"], ascending=[False, False], kind="stable").reset_index(drop=True)
