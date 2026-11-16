# fiscalizagov.py - FiscalizaGov: painel de fiscalização do Poder Executivo federal
# ============================================================
# Gastos e execução do orçamento, licitações e contratos, viagens, obras e repasses, cargos e nomeações e o Diário Oficial
# (uma das fontes, não a única). Padrão visual do Monitor Zanatta, agora em verde.
# - Falha de coleta é sempre sinalizada, nunca vira "sem resultados"
# - As quatro abas ligadas ao DOU ficam lado a lado (posições 6 a 9)
# ============================================================

import datetime as dt
import hmac
import os
from typing import List

import pandas as pd
import streamlit as st

import apresentacao
import dou
import tab_cargos
import tab_contratos
import tab_gastos
import tab_repasses
import tab_viagens
import ui

st.set_page_config(
    page_title="FiscalizaGov — Painel de fiscalização do Executivo",
    page_icon="🔎",
    layout="wide",
    initial_sidebar_state="collapsed",
)

BUSCA_PADRAO = "imposto\ntribut\ncontribui\nbenefício\nprograma\nportaria\ndecreto\ninstrução normativa"
ALERTA_PADRAO = "imposto\nbenefício\nregulamenta\nfica instituído\nfica criado"

# ============================================================
# ACESSO RESTRITO
# ============================================================

if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    st.markdown("""
    <style>
    .stApp{background:linear-gradient(135deg,#0b3d2e 0%,#146b46 55%,#27a06b 100%)}
    .login-icon{text-align:center;font-size:4rem;margin-top:3rem}
    .login-title{text-align:center;color:#fff;font-size:2.1rem;font-weight:800}
    .login-sub{text-align:center;color:#d8f3e3;margin-bottom:1.5rem}
    </style>
    <div class="login-icon">🔎</div>
    <div class="login-title">FiscalizaGov</div>
    <div class="login-sub">Painel de fiscalização do Executivo federal — acesso restrito</div>
    """, unsafe_allow_html=True)

    with st.form("login_form"):
        senha = st.text_input("🔒 Senha", type="password", placeholder="Digite a senha de acesso")
        entrar = st.form_submit_button("🚀 Entrar", use_container_width=True)

    if entrar:
        try:
            senha_correta = st.secrets["auth"]["senha"]
        except Exception:
            senha_correta = os.getenv("FISCALIZAGOV_SENHA", "")
        # sem senha configurada, ninguem entra
        if senha_correta and hmac.compare_digest(senha.encode("utf-8"), str(senha_correta).encode("utf-8")):
            st.session_state.autenticado = True
            st.rerun()
        else:
            st.error("Senha incorreta.")
    st.stop()

st.markdown(ui.CSS, unsafe_allow_html=True)

# ============================================================
# DADOS DO DIÁRIO OFICIAL (coleta em dou.py; cache por dia)
# ============================================================


class ColetaFalhou(Exception):
    def __init__(self, erros, parcial):
        super().__init__("; ".join(erros))
        self.erros = erros
        self.parcial = parcial


@st.cache_data(ttl=1800, show_spinner=False)
def _dou_cache(data: dt.date, secoes: tuple) -> pd.DataFrame:
    # Excecao nao e guardada pelo st.cache_data: falha nunca fica presa no cache.
    df, erros = dou.coletar_dou(data, list(secoes))
    if erros:
        raise ColetaFalhou(erros, df)
    return df


def carregar_dia(data: dt.date, secoes: List[str]):
    try:
        return _dou_cache(data, tuple(secoes)), []
    except ColetaFalhou as ex:
        return ex.parcial, ex.erros


def carregar_periodo(modo: str, faixa, secoes: List[str]):
    """Retorna (df, erros, datas)."""
    hoje = dou.hoje_brasilia()
    if modo == "Último dia com edição":
        d, df, erros = dou.ultimo_dia_com_edicao(lambda x: carregar_dia(x, secoes), hoje)
        return df, erros, [d]
    if modo == "Hoje":
        datas = [hoje]
    elif modo == "Ontem":
        datas = [hoje - dt.timedelta(days=1)]
    elif modo == "Últimos 3 dias":
        datas = [hoje - dt.timedelta(days=i) for i in range(3)]
    else:
        ini, fim = faixa
        datas = [ini + dt.timedelta(days=i) for i in range((fim - ini).days + 1)]
    dfs, erros = [], []
    for d in datas:
        df, er = carregar_dia(d, secoes)
        dfs.append(df)
        erros += [f"{ui.fmt_data(d)} · {x}" for x in er]
    df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=dou.COLUNAS)
    return df, erros, datas


# ============================================================
# CABEÇALHO
# ============================================================

st.markdown(ui.hero(
    "FiscalizaGov", "Painel de fiscalização do Poder Executivo federal: para onde vai o dinheiro público e quem decide",
    ["💸 Gastos", "🧾 Licitações e contratos", "✈️ Viagens", "🏗️ Obras e repasses", "👔 Cargos", "📜 Diário Oficial"]),
    unsafe_allow_html=True)

c_info, c_btn = st.columns([6, 1])
c_info.caption("Fontes oficiais: Portal da Transparência e CGU, PNCP, Imprensa Nacional (Diário Oficial) · "
               f"horário de Brasília: {dou.agora_brasilia_str()}")
if c_btn.button("🔄 Atualizar tudo", use_container_width=True, help="Limpa o cache e busca de novo (DOU, PNCP, orçamento…)"):
    st.cache_data.clear()
    st.rerun()

# ============================================================
# ABAS (as quatro do Diário Oficial ficam juntas: 6 a 9)
# ============================================================

(aba_apres, aba_gastos, aba_pncp, aba_viagens, aba_obras,
 aba_dou, aba_busca, aba_orgaos, aba_cargos, aba_breve) = st.tabs([
    "🏠 Apresentação",
    "💸 Gastos do governo",
    "🧾 Licitações e contratos",
    "✈️ Viagens e diárias",
    "🏗️ Obras e programas",
    "📜 DOU · Achados",
    "📜 DOU · Buscar",
    "📜 DOU · Órgãos e tipos",
    "👔 Cargos e nomeações",
    "🚧 Em breve",
])

# Estado do DOU compartilhado entre as abas do Diário e a Apresentação.
# Os filtros ficam na aba "DOU · Achados" e valem para as três abas do Diário.
dou_df = pd.DataFrame(columns=dou.COLUNAS)
dou_f = dou_df
dou_rank = dou_df
dou_erros: List[str] = []
dou_datas: List[dt.date] = []
termos_alerta: List[str] = []

# ------------------------------------------------------------
# DOU · ACHADOS (rodada antes das outras para alimentar a Apresentação)
# ------------------------------------------------------------
with aba_dou:
    st.subheader("Diário Oficial da União · Achados")
    st.caption("O Diário é uma das fontes do painel. Estes filtros valem para as três abas “DOU”.")
    for k, v in (("fg_busca", BUSCA_PADRAO), ("fg_alerta", ALERTA_PADRAO)):
        st.session_state.setdefault(k, v)

    modo = ui.escolha_unica("Período", ui.PERIODOS, ui.PERIODOS[0], key="fg_periodo")
    faixa = None
    periodo_valido = True
    if modo == "Personalizado":
        hoje_ = dou.hoje_brasilia()
        faixa = st.date_input("📅 Período do DOU", value=(hoje_ - dt.timedelta(days=1), hoje_),
                              format="DD/MM/YYYY", key="fg_faixa")
        if not (isinstance(faixa, (tuple, list)) and len(faixa) == 2):
            st.caption("Selecione a data final no calendário.")
            periodo_valido = False
        elif (faixa[1] - faixa[0]).days > 6:
            st.warning("Escolha no máximo 7 dias por vez (cada dia baixa milhares de atos).")
            periodo_valido = False

    with st.expander("⚙️ Seções e palavras-chave"):
        secoes = st.multiselect("Seções do DOU", dou.SECOES_DISPONIVEIS, default=dou.DEFAULT_SECOES_DOU,
                                format_func=lambda s: dou.ROTULOS_SECAO.get(s, s),
                                help="Extras são edições publicadas fora do horário normal.")
        ocultar_pref = st.checkbox("Ocultar atos de prefeituras", value=True, key="fg_ocultar_pref",
                                   help="O DOU 3 traz milhares de extratos de contratos municipais. "
                                        "Como o foco é o Executivo federal, ficam ocultos por padrão.")
        c1, c2 = st.columns(2)
        c1.text_area("🔑 Palavras-chave de busca (uma por linha)", key="fg_busca", height=170,
                     help="Só entram na análise os atos que contenham ao menos uma destas palavras.")
        c2.text_area("🔔 Palavras-chave de alerta (uma por linha)", key="fg_alerta", height=170,
                     help="Cada palavra encontrada soma pontos de relevância e aparece com 🔔 no card.")
        st.button("Restaurar listas padrão", on_click=lambda: st.session_state.update(
            fg_busca=BUSCA_PADRAO, fg_alerta=ALERTA_PADRAO))

    termos_busca = [t.strip() for t in st.session_state["fg_busca"].splitlines() if t.strip()]
    termos_alerta = [t.strip() for t in st.session_state["fg_alerta"].splitlines() if t.strip()]

    if not secoes:
        st.info("Escolha ao menos uma seção do DOU.")
    elif periodo_valido:
        with st.spinner("Coletando o DOU (Imprensa Nacional)…"):
            dou_df, dou_erros, dou_datas = carregar_periodo(modo, faixa, secoes)
        if ocultar_pref and not dou_df.empty:
            dou_df = dou_df[dou_df["Órgão"].map(dou.orgao_raiz) != "Prefeituras"]
        dou_f = dou.filtrar(dou_df, termos_busca)
        dou_rank = dou.rankear(dou_f, termos_alerta) if not dou_f.empty else dou_f
        if not dou_rank.empty:
            dou_rank = dou_rank.copy()
            dou_rank["Nível"] = dou_rank["Score"].map(dou.nivel_relevancia)
            dou_rank["Órgão raiz"] = dou_rank["Órgão"].map(dou.orgao_raiz)

    periodo_txt = "—"
    if dou_datas:
        periodo_txt = ui.fmt_data(dou_datas[0]) if len(dou_datas) == 1 else \
            f"{ui.fmt_data(min(dou_datas))} a {ui.fmt_data(max(dou_datas))}"
        st.caption(f"📅 DOU de {periodo_txt} · {ui.fmt_num(len(dou_df))} atos coletados · "
                   f"{ui.fmt_num(len(dou_f))} com as palavras-chave de busca")

    if dou_erros:
        st.error("⚠️ **Falha na coleta do DOU.** Os resultados podem estar incompletos, e a ausência de itens "
                 "**não** significa ausência de atos. Clique em **Atualizar tudo** em instantes.\n\n"
                 + "\n".join(f"- {x}" for x in dou_erros))
    elif dou_df.empty and secoes and periodo_valido:
        st.info("Nenhuma publicação nas seções escolhidas neste período (dia sem edição ou edição ainda não publicada).")

    if not dou_rank.empty:
        n_alta = int((dou_rank["Nível"] == "Alta").sum())
        n_media = int((dou_rank["Nível"] == "Média").sum())
        n_org = int(dou_rank["Órgão raiz"].nunique())
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui.kpi("📜 Atos publicados", ui.fmt_num(len(dou_df)), ui.COR_ALERTA, "nas seções escolhidas")
        with k2:
            ui.kpi("🔎 Com palavras-chave", ui.fmt_num(len(dou_f)), "#2e7d32", "entram no ranking")
        with k3:
            ui.kpi("🚨 Relevância alta", ui.fmt_num(n_alta), ui.COR_ALTA, f"+ {ui.fmt_num(n_media)} média")
        with k4:
            ui.kpi("🏛️ Órgãos com achados", ui.fmt_num(n_org), ui.COR_MEDIA, "ministérios e entidades")

        ui.secao("⭐ Destaques (um mix de órgãos)")
        st.caption("Os 5 atos de maior relevância, no máximo 2 por órgão, para não repetir sempre o mesmo.")
        for _, row in dou.top_diverso(dou_rank, 5, 2).iterrows():
            st.markdown(ui.html_card(row), unsafe_allow_html=True)

        ui.secao("📋 Todos os achados")
        f1, f2 = st.columns(2)
        with f1:
            st.caption("Relevância")
            sel_niveis = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="fg_f_nivel")
        with f2:
            secs = sorted(dou_rank["Seção"].unique())
            st.caption("Seção")
            sel_secs = ui.escolha_multipla("Seção", secs, key=f"fg_f_sec_{'|'.join(secs)}")
        raizes = sorted(dou_rank["Órgão raiz"].replace("", "(sem órgão)").unique())
        sel_org = st.multiselect("🏛️ Órgãos (vazio = todos)", raizes, key="fg_f_org")

        d = dou_rank[dou_rank["Nível"].isin(sel_niveis) & dou_rank["Seção"].isin(sel_secs)]
        if sel_org:
            d = d[d["Órgão raiz"].replace("", "(sem órgão)").isin(sel_org)]
        cA, cB = st.columns([4, 2])
        cA.caption(f"{ui.fmt_num(len(d))} atos nos filtros")
        with cB:
            ui.botoes_download(d.drop(columns=["Órgão raiz"]), f"fiscalizagov_{dt.date.today():%Y%m%d}", "fg_dl2")
        if d.empty:
            st.info("Nenhum ato com os filtros selecionados.")
        else:
            ui.render_cards(d, 25, key="fg_t2")
    elif secoes and periodo_valido and not dou_erros and not dou_df.empty:
        st.info("Nenhum ato encontrado com os filtros atuais.")

    with st.expander("📖 Como ler a relevância (importante)"):
        st.markdown(
            "- **Relevância temática ≠ irregularidade.** O score (0 a 100) mede o quanto o ato trata de temas "
            "sensíveis (tributos, benefícios, regulamentação, autorizações) e quantas palavras-chave de alerta ele traz.\n"
            "- **Alta** = 60 ou mais · **Média** = 35 a 59 · **Baixa** = abaixo de 35.\n"
            "- Os selos cinza mostram *por que* o ato pontuou; o selo azul 🔔 é uma palavra-chave de alerta.\n"
            "- O ato é só o ponto de partida: confira sempre a matéria oficial pelo link (↗) antes de concluir qualquer coisa.\n"
            "- Se aparecer o aviso vermelho de falha, o DOU não pôde ser lido: **ausência de achados não é ausência de atos**."
        )

# ------------------------------------------------------------
# APRESENTAÇÃO (um dado de cada aba)
# ------------------------------------------------------------
with aba_apres:
    apresentacao.render({
        "atos": len(dou_df), "palavras": len(dou_f), "periodo": periodo_txt,
        "altos": int((dou_rank["Nível"] == "Alta").sum()) if not dou_rank.empty else 0,
    })

# ------------------------------------------------------------
# GASTOS, LICITAÇÕES, VIAGENS, OBRAS
# ------------------------------------------------------------
with aba_gastos:
    tab_gastos.render()

with aba_pncp:
    tab_contratos.render()

with aba_viagens:
    tab_viagens.render()

with aba_obras:
    tab_repasses.render()

# ------------------------------------------------------------
# DOU · BUSCAR
# ------------------------------------------------------------
with aba_busca:
    st.subheader("Diário Oficial da União · Buscar")
    st.caption("Procura em todos os atos coletados do período escolhido na aba “DOU · Achados” (título, ementa e órgão), "
               "mesmo os que não bateram nas palavras-chave.")
    consulta = st.text_input("🔍 Termo(s) de busca", placeholder="ex.: dispensa de licitação, Ministério da Saúde", key="fg_q")
    if consulta.strip():
        if dou_df.empty:
            st.info("Não há atos coletados para buscar neste período.")
        else:
            palavras = consulta.strip().lower().split()
            blob = (dou_df["Título"] + " " + dou_df["Ementa/Resumo"] + " " + dou_df["Órgão"]).str.lower()
            mask = pd.Series(True, index=dou_df.index)
            for p in palavras:
                mask &= blob.str.contains(p, regex=False)
            res = dou.rankear(dou_df[mask], termos_alerta)
            st.caption(f"{ui.fmt_num(len(res))} atos contêm todas as palavras digitadas")
            if not res.empty:
                ui.botoes_download(res, "fiscalizagov_busca", "fg_dl3")
                ui.render_cards(res, 20, key=f"fg_t3_{consulta.strip().lower()}")
    else:
        st.info("Digite um termo para buscar.")

# ------------------------------------------------------------
# DOU · ÓRGÃOS E TIPOS
# ------------------------------------------------------------
with aba_orgaos:
    st.subheader("Diário Oficial da União · Órgãos e tipos de ato")
    if dou_rank.empty:
        st.info("Sem dados para os filtros atuais da aba “DOU · Achados”.")
    else:
        por_org = dou_rank.groupby("Órgão raiz").agg(Atos=("Score", "size"), ScoreMax=("Score", "max"),
                                                      Alta=("Nível", lambda s: int((s == "Alta").sum())))
        por_org = por_org.sort_values(["Alta", "Atos"], ascending=False)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Órgãos com mais atos de relevância alta**")
            st.bar_chart(por_org.head(12)["Alta"], horizontal=True, color="#1f5f99")
        with c2:
            st.markdown("**Tipos de ato**")
            st.bar_chart(dou_rank["Tipo"].replace("", "(sem tipo)").value_counts().head(12), horizontal=True, color="#1b8a5a")
        st.markdown("**Todos os órgãos**")
        st.dataframe(por_org.rename(columns={"ScoreMax": "Maior score", "Alta": "Atos de relevância alta"}),
                     use_container_width=True)

# ------------------------------------------------------------
# CARGOS E NOMEAÇÕES (atos de pessoal do DOU)
# ------------------------------------------------------------
with aba_cargos:
    tab_cargos.render()

# ------------------------------------------------------------
# EM BREVE
# ------------------------------------------------------------
with aba_breve:
    st.subheader("Próximos módulos")
    st.caption("Ainda não implementados.")
    st.markdown(
        "- 🏗️ **Execução física das obras** — Obrasgov.br (a API antiga parou em julho de 2026; falta a nova)\n"
        "- 🕘 **Histórico de alertas** — acompanhar o mesmo fornecedor ou órgão ao longo das semanas"
    )

st.markdown("---")
st.caption("FiscalizaGov · Painel de fiscalização do Poder Executivo federal · Fontes: Portal da Transparência e CGU, "
           "PNCP e Imprensa Nacional (Diário Oficial da União)")
