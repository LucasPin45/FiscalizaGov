# cargos.py - nomeacoes, exoneracoes e designacoes no DOU, com foco em alta gestao e Presidencia
# ============================================================
# Fonte: os proprios atos do DOU ja coletados por dou.py (portarias e decretos de pessoal).
# O DOU entrega so ~400 caracteres de cada ato; para os atos relevantes o texto completo e
# baixado da pagina da materia (texto_completo).
#
# Classificacao (explicavel, por regras):
#   Alta direcao       - decreto presidencial; Ministro de Estado; Secretario-Executivo; Natureza
#                        Especial (NE/CNE); CCE/FCE 1.17-1.18 e DAS 5-6; presidente, diretor-presidente
#                        ou diretor-geral de entidade; comandantes.
#   Direcao intermedia - CCE/FCE 1.15-1.16 e DAS 4; diretor; secretario; superintendente; chefe de gabinete.
#   Demais             - o resto (assessores, funcoes gratificadas, servidores de carreira...).
# A equivalencia CCE/FCE x DAS segue a tabela de transformacao dos cargos (1.18=DAS 6, 1.17=DAS 5,
# 1.15=DAS 4).
#
# Limites: a leitura de nomes e cargos e por padroes de texto; sempre mostramos o trecho original
# do ato e o link. Conflito de interesses e nepotismo (Sumula Vinculante 13) NAO sao detectaveis
# so com o DOU: exigem dados de parentesco.
# ============================================================

import html
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import pandas as pd
import requests

import dou

PRESIDENCIA = "Presidência da República"
ATOS_PRESIDENTE = "Atos do Poder Executivo"
RAIZES_NAO_EXECUTIVO = ("Poder Judiciário", "Poder Legislativo", "Ministério Público da União",
                        "Entidades de Fiscalização do Exercício das Profissões Liberais", "Prefeituras")

ACOES = {"NOMEAR": "Nomeação", "EXONERAR": "Exoneração", "DESIGNAR": "Designação",
         "DISPENSAR": "Dispensa", "TORNAR SEM EFEITO": "Sem efeito"}

_PART = r"(?:d[aeo]s?|e)"
_TOK = r"[A-ZÀ-Ý][A-Za-zÀ-ÿ'’\.\-]*"
_NOME = rf"{_TOK}(?:\s+(?:{_PART}\s+)?{_TOK}){{1,9}}"
# O verbo vale em maiúsculas ou minúsculas; o NOME só conta se começar com maiúscula.
RE_ACAO = re.compile(
    rf"\b((?i:nomear|exonerar|designar|dispensar|tornar\s+sem\s+efeito))\b"
    rf"(?:(?i:\s*,\s*a\s+pedido\s*,|\s*,\s*a\s+partir\s+de\s+[^,]{{3,40}},))?[\s,:]*(?P<nome>{_NOME})?")
RE_ACAO_PESSOAL = re.compile(
    r"\b(?i:nomear|exonerar|designar|dispensar)\b\s*(?:,|:|[A-ZÀ-Ý])")
RE_CARGO = re.compile(
    r"(?:cargo|fun[cç][aã]o|encargo)\s+(?:em\s+comiss[aã]o\s+|comissionad[oa]\s+(?:executiv[oa]\s+)?|"
    r"de\s+natureza\s+especial\s+)?(?:de|do|da)\s+(?P<cargo>[^,;\.\(]{3,100})", re.I)
RE_CODIGO = re.compile(r"\b(CCE|FCE|DAS|CNE|FCPE)[\s\-]*(\d{1,3}(?:[\.,]\d{1,3})?)", re.I)
RE_CORPO = re.compile(r"\bresolv[ea]\s*:?", re.I)

PALAVRAS_ALTA = ("ministro de estado", "secretário-executivo", "secretário executivo", "secretária-executiva",
                 "secretário especial", "secretária especial", "natureza especial", "diretor-presidente",
                 "diretora-presidente", "diretor-geral", "diretora-geral", "presidente d", "comandante")
PALAVRAS_INTERMEDIARIA = ("diretor", "diretora", "secretário", "secretária", "superintendente",
                          "chefe de gabinete", "subchefe", "consultor jurídico", "consultora jurídica",
                          "coordenador-geral", "coordenadora-geral", "chefe de assessoria")


def codigo_norm(serie: str, num: str) -> str:
    return f"{serie.upper()}-{num.replace(',', '.')}"


def nivel_codigo(serie: str, num: str) -> int:
    """2 = alta direcao, 1 = intermediaria, 0 = demais. CCE/FCE 1.x e DAS 101.x/N."""
    serie = serie.upper()
    n = num.replace(",", ".")
    try:
        if serie in ("CCE", "FCE"):
            if not n.startswith("1."):
                return 0  # series 2.x e 3.x: assessoramento e chefias de nivel menor
            g = int(n.split(".")[1])
            return 2 if g >= 17 else 1 if g >= 15 else 0
        if serie == "DAS":
            g = int(n.split(".")[-1]) if "." in n else int(n)
            return 2 if g >= 5 else 1 if g == 4 else 0
        if serie == "CNE":
            return 2
    except (ValueError, IndexError):
        return 0
    return 0


# ------------------------------------------------------------
# Selecao dos atos de pessoal
# ------------------------------------------------------------

def eh_ato_pessoal(texto: str) -> bool:
    return bool(RE_ACAO_PESSOAL.search(texto or ""))


def eh_executivo(orgao: str) -> bool:
    return dou.orgao_raiz(orgao) not in RAIZES_NAO_EXECUTIVO


def eh_presidencia(orgao: str) -> bool:
    return dou.orgao_raiz(orgao) == PRESIDENCIA


def eh_decreto_presidencial(orgao: str, titulo: str) -> bool:
    return dou.orgao_raiz(orgao) == ATOS_PRESIDENTE and str(titulo).upper().startswith("DECRETO")


def unidade_presidencia(orgao: str) -> str:
    """Segundo nivel da hierarquia dentro da Presidencia (ex.: Casa Civil)."""
    partes = [p.strip() for p in str(orgao).split("/")]
    if partes and partes[0] == PRESIDENCIA:
        return partes[1] if len(partes) > 1 else "Gabinete e demais"
    if partes and partes[0] == ATOS_PRESIDENTE:
        return "Atos do Presidente (decretos)"
    return ""


# ------------------------------------------------------------
# Texto completo
# ------------------------------------------------------------

_RE_TEXTO = re.compile(r'<div class="texto-dou">(.*?)</div>\s*<div', re.S)


def extrair_texto(pagina_html: str) -> str:
    m = _RE_TEXTO.search(pagina_html or "")
    if not m:
        return ""
    t = re.sub(r"<[^>]+>", " ", m.group(1))
    return re.sub(r"\s+", " ", html.unescape(t)).strip()


def texto_completo(link: str, session=None, tentativas: int = 3) -> str:
    """Texto integral da materia no in.gov.br. '' se nao conseguir (nunca levanta)."""
    session = session or requests.Session()
    for i in range(tentativas):
        try:
            r = session.get(link, headers=dou.HEADERS, timeout=40)
            if r.status_code == 200:
                return extrair_texto(r.text)
        except requests.RequestException:
            pass
        if i < tentativas - 1:
            time.sleep(1.5 * (i + 1))
    return ""


def baixar_textos(links: List[str], workers: int = 4, session=None) -> Dict[str, str]:
    session = session or requests.Session()
    unicos = list(dict.fromkeys(l for l in links if l))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        textos = list(ex.map(lambda l: texto_completo(l, session), unicos))
    return dict(zip(unicos, textos))


# ------------------------------------------------------------
# Movimentacoes (quem entrou/saiu, em que cargo)
# ------------------------------------------------------------

def _janela_cargo(texto: str, ini: int) -> str:
    fim = ini + 420
    prox = RE_ACAO.search(texto, ini + 8)
    if prox and prox.start() < fim:
        fim = prox.start()
    return texto[ini:fim]


PALAVRAS_NAO_NOME = {
    "Lei", "Decreto", "Portaria", "Art", "Artigo", "Siape", "SIAPE", "DOU", "CPF", "Edital", "Universidade",
    "Instituto", "Federal", "Fundação", "Ministério", "Gerência", "Superintendência", "Secretaria", "Departamento",
    "Coordenação", "Reitoria", "Diretoria", "Campus", "Resolução", "Instrução", "Normativa", "Constituição",
    "Processo", "Seção", "Seçao", "Concurso", "Magistério", "Superior", "Ensino", "Brasil", "República",
    "Presidência", "Casa", "Civil", "Servidor", "Servidora", "Procuradoria", "Agência", "Nacional", "Conselho",
    "Escola", "Hospital", "Núcleo", "Setor", "Divisão", "Unidade", "Comissão", "Grupo", "Banca", "Anexo",
}


def _achar_nome(trecho: str) -> str:
    """Primeira sequencia de nomes proprios depois do verbo, pulando referencias legais e orgaos."""
    for m in re.finditer(_NOME, trecho[:300]):
        tokens = m.group(0).split()
        if any(t.strip(".,") in PALAVRAS_NAO_NOME for t in tokens):
            continue
        return m.group(0).strip(" ,.-")
    return ""


def extrair_movimentos(texto: str) -> List[dict]:
    """Lista de {acao, nome, cargo, codigos} lidos do texto do ato (so apos 'resolve')."""
    m = RE_CORPO.search(texto or "")
    corpo = texto[m.end():] if m else (texto or "")
    movs: List[dict] = []
    for a in RE_ACAO.finditer(corpo):
        verbo = re.sub(r"\s+", " ", a.group(1).upper())
        janela = _janela_cargo(corpo, a.start())
        nome = _achar_nome(janela[a.end(1) - a.start():]) if verbo != "TORNAR SEM EFEITO" else ""
        cm = RE_CARGO.search(janela)
        codigos = [codigo_norm(s, n) for s, n in RE_CODIGO.findall(janela)]
        movs.append({"acao": ACOES.get(verbo, verbo.title()), "nome": nome,
                     "cargo": re.sub(r"\s+", " ", cm.group("cargo")).strip() if cm else "",
                     "codigos": codigos, "trecho": janela[:260]})
    return [x for x in movs if x["nome"] or x["cargo"]]


ALTA_RE = [
    (r"\bministr[oa] de estado\b", "Ministro de Estado"),
    # "secretaria" (órgão) não pode casar: exige secretário/secretária (com acento) ou secretario
    (r"\bsecret(?:ário|ária|ario)[- ]executiv[oa]\b", "Secretário-Executivo"),
    (r"\bsecret(?:ário|ária|ario) especial\b", "Secretário Especial"),
    (r"\bsecret(?:ário|ária|ario) nacional\b", "Secretário Nacional"),
    (r"\bnatureza especial\b", "Natureza Especial"),
    (r"\bdiretor[a]?[- ]presidente\b", "Diretor-Presidente"),
    (r"\bdiretor[a]?[- ]geral\b", "Diretor-Geral"),
    (r"\bpresidente d[aeo]s? (?!comiss|comit|conselh|banca|grupo|colegiad|n[úu]cleo|junta)", "Presidente de entidade"),
    (r"(?<!vice-)(?<!pró-)(?<!pro-)(?<!pró )(?<!pro )\breitor[a]?\b", "Reitor"),
    (r"\bcomandante d[aoe] (?:marinha|ex[ée]rcito|aeron[áa]utica)\b", "Comandante de Força"),
]
INTERMEDIARIA_RE = [
    (r"\bdiretor[a]?\b", "Diretor"), (r"\bsecret(?:ário|ária|ario)\b", "Secretário"), (r"\bsuperintendente\b", "Superintendente"),
    (r"\bchefe de gabinete\b", "Chefe de Gabinete"), (r"\bsubchefe\b", "Subchefe"),
    (r"\bconsultor[a]? jur[íi]dic[oa]\b", "Consultor Jurídico"), (r"\bcoordenador[a]?[- ]geral\b", "Coordenador-Geral"),
    (r"\bvice-reitor[a]?\b", "Vice-Reitor"), (r"\bcomandante\b", "Comandante"),
]
RE_INSTITUICAO_ENSINO = re.compile(r"universidade|instituto federal|col[ée]gio pedro|hospital universit", re.I)


def nivel_texto(corpo: str, movs: List[dict], orgao: str = "") -> (int, List[str]):
    """Nivel (0-2) e motivos. Olha so os CARGOS e codigos das movimentacoes, nunca o preambulo nem o
    signatario (que cita 'Ministro de Estado' em quase todo ato)."""
    substituicao = False
    if movs:
        cabecas = []
        for m in movs:
            cargo = m["cargo"]
            substituicao = substituicao or bool(re.search(r"\bsubstitut[oa]s?\b|\bsubstitui[cç][aã]o\b|\binterin[oa]\b", cargo, re.I))
            nucleo = re.split(r"(?:do|da)\s+(?:cargo|fun[cç][aã]o)\s+(?:de|do|da)\s+", cargo, flags=re.I)[-1]
            # só o núcleo do cargo: "Assessor da Secretaria Nacional" não é Secretário Nacional
            cabecas.append(re.split(r"\s+(?:d[aeo]s?|n[aeo]s?|em|junto)\s+", nucleo, maxsplit=1)[0])
        cargos_txt = " ; ".join(cabecas).lower()
        codigos = [c for m in movs for c in m["codigos"]]
    else:
        m = RE_CORPO.search(corpo or "")
        cargos_txt = (corpo[m.end():] if m else corpo or "")[:600].lower()
        codigos = [codigo_norm(s, n) for s, n in RE_CODIGO.findall(cargos_txt)]
    nivel, motivos = 0, []

    def sobe(n, motivo):
        nonlocal nivel
        nivel = max(nivel, n)
        motivos.append(motivo)

    for c in dict.fromkeys(codigos):
        serie, num = c.split("-", 1)
        g = nivel_codigo(serie, num)
        if g:
            sobe(g, f"{c} ({'alta direção' if g == 2 else 'direção intermediária'})")
    ensino = bool(RE_INSTITUICAO_ENSINO.search(orgao))
    for rx, nome in ALTA_RE:
        if re.search(rx, cargos_txt):
            # direcao-geral de campus/universidade nao e alta direcao do Governo: so Reitor
            if ensino and nome not in ("Reitor",):
                sobe(1, f"cargo: {nome} (instituição de ensino)")
            else:
                sobe(2, f"cargo: {nome}")
            break
    if nivel < 1:
        for rx, nome in INTERMEDIARIA_RE:
            if re.search(rx, cargos_txt):
                sobe(1, f"cargo: {nome}")
                break
    # Designacao como substituto eventual nao e nomeacao: no maximo direcao intermediaria
    if nivel == 2 and substituicao:
        nivel = 1
        motivos.append("substituição eventual (não é nomeação)")
    return nivel, list(dict.fromkeys(motivos))


NIVEIS = {2: "Alta direção", 1: "Direção intermediária", 0: "Demais"}
NIVEL_NUM = {v: k for k, v in NIVEIS.items()}


def movimentos_df(analisado: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por movimentação (quem entrou ou saiu de qual cargo), pronta para cards e tabelas."""
    cols = ["Data", "Órgão", "Unidade da Presidência", "Presidência", "Decreto presidencial", "Ação", "Nome",
            "Cargo", "Códigos", "Nível", "Motivos", "Score", "Substituição", "Título do ato", "Link"]
    linhas = []
    for _, r in analisado.iterrows():
        for m in r["Movimentações"]:
            linhas.append({
                "Data": r["Data"], "Órgão": r["Órgão"], "Unidade da Presidência": r["Unidade da Presidência"],
                "Presidência": r["Presidência"], "Decreto presidencial": r["Decreto presidencial"],
                "Ação": m["acao"], "Nome": m["nome"], "Cargo": m["cargo"], "Códigos": ", ".join(m["codigos"]),
                "Nível": m.get("nivel", r["Nível"]), "Motivos": " | ".join(m.get("motivos", [])),
                "Score": m.get("score", r["Score"]),
                "Substituição": bool(re.search(r"\bsubstitut[oa]s?\b|\binterin[oa]\b", m["cargo"], re.I)),
                "Título do ato": r["Título"], "Link": r["Link"],
            })
    out = pd.DataFrame(linhas, columns=cols)
    if out.empty:
        return out
    return out.sort_values(["Score", "Data"], ascending=[False, False], kind="stable").reset_index(drop=True)


def analisar(df: pd.DataFrame, textos: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """Filtra atos de pessoal do Executivo e adiciona classificacao.

    `textos` ({link: texto completo}) refina a leitura; sem ele usa o trecho de ~400 caracteres.
    """
    textos = textos or {}
    if df.empty:
        return pd.DataFrame(columns=list(df.columns) + COLUNAS_ANALISE)
    base = df[df.apply(lambda r: eh_executivo(r["Órgão"]) and
                       eh_ato_pessoal(f'{r["Título"]} {r["Ementa/Resumo"]}'), axis=1)].copy()
    if base.empty:
        return pd.DataFrame(columns=list(df.columns) + COLUNAS_ANALISE)
    linhas = []
    for _, r in base.iterrows():
        texto = textos.get(r["Link"]) or f'{r["Título"]} {r["Ementa/Resumo"]}'
        movs = extrair_movimentos(texto)
        presid = eh_presidencia(r["Órgão"])
        decreto = eh_decreto_presidencial(r["Órgão"], r["Título"])
        # classifica cada movimentação; o ato fica com o maior nível entre elas
        for m in movs:
            n, mot = nivel_texto(texto, [m], r["Órgão"])
            if decreto:
                # decreto do Presidente: sempre visível; só é alta direção se o cargo/código for de alta direção
                # (decretos também nomeiam juízes, procuradores etc.)
                n = max(n, 1)
                mot = ["decreto do Presidente da República"] + mot
            m["nivel"], m["motivos"] = NIVEIS[n], mot
            m["score"] = min(100, {2: 70, 1: 40, 0: 10}[n] + (15 if presid else 0) + (15 if decreto else 0))
        nivel, motivos = nivel_texto(texto, movs, r["Órgão"])
        if decreto:
            nivel = max(nivel, 1)
            motivos.insert(0, "decreto do Presidente da República")
        if movs:
            nivel = max(NIVEL_NUM[m["nivel"]] for m in movs)
        score = {2: 70, 1: 40, 0: 10}[nivel] + (15 if presid else 0) + (15 if decreto else 0)
        acoes = list(dict.fromkeys(m["acao"] for m in movs))
        linhas.append({
            "Nível": NIVEIS[nivel], "Presidência": presid, "Decreto presidencial": decreto,
            "Unidade da Presidência": unidade_presidencia(r["Órgão"]) if presid or decreto else "",
            "Ações": ", ".join(acoes), "Movimentações": movs,
            "Motivos": " | ".join(motivos), "Score": min(100, score),
            "Texto completo": bool(textos.get(r["Link"])),
        })
    out = pd.concat([base.reset_index(drop=True), pd.DataFrame(linhas)], axis=1)
    return out.sort_values(["Score", "Data"], ascending=[False, False], kind="stable").reset_index(drop=True)


COLUNAS_ANALISE = ["Nível", "Presidência", "Decreto presidencial", "Unidade da Presidência", "Ações",
                   "Movimentações", "Motivos", "Score", "Texto completo"]


def prioritarios(df_analisado: pd.DataFrame, maximo: int = 80) -> List[str]:
    """Links cujo texto completo vale baixar: Presidencia, decretos e alta/intermediaria pelo trecho."""
    if df_analisado.empty:
        return []
    d = df_analisado[(df_analisado["Presidência"]) | (df_analisado["Decreto presidencial"]) |
                     (df_analisado["Nível"] != "Demais")]
    return d.sort_values("Score", ascending=False)["Link"].head(maximo).tolist()
