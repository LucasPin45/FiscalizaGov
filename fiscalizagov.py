# fiscalizagov.py - FiscalizaGov (v0.3)
# ============================================================
# Radar do Executivo (padrao visual do Monitor Zanatta):
# - Abas numeradas, cards com borda colorida, selos de relevancia
# - Coleta do DOU compartilhada com o notificador (dou.py)
# - Falha de coleta e sempre sinalizada, nunca vira "sem resultados"
# ============================================================

import datetime as dt
import hmac
import html
import os
from typing import List

import pandas as pd
import requests
import streamlit as st

import dou
import ui
import notificar_fiscalizagov as notif
import tab_contratos

APP_TITLE = "FiscalizaGov — Radar do Executivo"

st.set_page_config(
    page_title="FiscalizaGov — Radar do Executivo",
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
    .stApp{background:linear-gradient(135deg,#1e3a5f 0%,#3b6ea5 100%)}
    .login-icon{text-align:center;font-size:4rem;margin-top:3rem}
    .login-title{text-align:center;color:#fff;font-size:2rem;font-weight:700}
    .login-sub{text-align:center;color:#FFD700;margin-bottom:1.5rem}
    </style>
    <div class="login-icon">🔎</div>
    <div class="login-title">FiscalizaGov</div>
    <div class="login-sub">Radar do Executivo — acesso restrito</div>
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
# DADOS (coleta em dou.py; cache por dia)
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
# TELEGRAM
# ============================================================


def telegram_enviar(bot_token: str, chat_id: str, mensagem: str) -> dict:
    if not bot_token or not chat_id:
        return {"ok": False, "error": "BOT_TOKEN e CHAT_ID são obrigatórios."}
    res = notif.telegram_send(bot_token, chat_id, mensagem)
    if res.get("ok"):
        return {"ok": True}
    return {"ok": False, "error": res.get("description", "Erro desconhecido")}


# ============================================================
# CABEÇALHO
# ============================================================

st.title("🔎 FiscalizaGov — Radar do Executivo")
st.caption("v0.3 · Dados: Diário Oficial da União (Imprensa Nacional) · Horário de Brasília")

col_info, col_btn = st.columns([5, 1])
with col_info:
    st.markdown("**Fiscalização do Poder Executivo federal** — atos publicados no DOU, ordenados por relevância temática, "
                "com link direto para a matéria oficial.")
with col_btn:
    if st.button("🔄 Atualizar tudo", use_container_width=True, help="Limpa o cache e busca de novo no DOU"):
        st.cache_data.clear()
        st.rerun()

# ============================================================
# FILTROS GERAIS (valem para todas as abas)
# ============================================================

for k, v in (("fg_busca", BUSCA_PADRAO), ("fg_alerta", ALERTA_PADRAO)):
    st.session_state.setdefault(k, v)

modo = ui.escolha_unica("Período", ui.PERIODOS, ui.PERIODOS[0], key="fg_periodo")
faixa = None
if modo == "Personalizado":
    hoje_ = dou.hoje_brasilia()
    faixa = st.date_input("📅 Período do DOU", value=(hoje_ - dt.timedelta(days=1), hoje_),
                          format="DD/MM/YYYY", key="fg_faixa")
    if not (isinstance(faixa, (tuple, list)) and len(faixa) == 2):
        st.caption("Selecione a data final no calendário.")
        st.stop()
    if (faixa[1] - faixa[0]).days > 6:
        st.warning("Escolha no máximo 7 dias por vez (cada dia baixa milhares de atos).")
        st.stop()

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

if not secoes:
    st.info("Escolha ao menos uma seção do DOU.")
    st.stop()

termos_busca = [t.strip() for t in st.session_state["fg_busca"].splitlines() if t.strip()]
termos_alerta = [t.strip() for t in st.session_state["fg_alerta"].splitlines() if t.strip()]

with st.spinner("Coletando o DOU (Imprensa Nacional)…"):
    df_dou, erros, datas = carregar_periodo(modo, faixa, secoes)

if ocultar_pref and not df_dou.empty:
    df_dou = df_dou[df_dou["Órgão"].map(dou.orgao_raiz) != "Prefeituras"]

df_f = dou.filtrar(df_dou, termos_busca)
df_rank = dou.rankear(df_f, termos_alerta) if not df_f.empty else df_f
if not df_rank.empty:
    df_rank = df_rank.copy()
    df_rank["Nível"] = df_rank["Score"].map(dou.nivel_relevancia)
    df_rank["Órgão raiz"] = df_rank["Órgão"].map(dou.orgao_raiz)

periodo_txt = ui.fmt_data(datas[0]) if len(datas) == 1 else f"{ui.fmt_data(min(datas))} a {ui.fmt_data(max(datas))}"
st.caption(f"📅 DOU de {periodo_txt} · {ui.fmt_num(len(df_dou))} atos coletados · "
           f"{ui.fmt_num(len(df_f))} com as palavras-chave de busca")

if erros:
    st.error("⚠️ **Falha na coleta do DOU.** Os resultados podem estar incompletos, e a ausência de itens "
             "**não** significa ausência de atos. Clique em **Atualizar tudo** em instantes.\n\n"
             + "\n".join(f"- {x}" for x in erros))
elif df_dou.empty:
    st.info("Nenhuma publicação nas seções escolhidas neste período (dia sem edição ou edição ainda não publicada).")

st.markdown("---")

# ============================================================
# ABAS
# ============================================================

tab1, tab2, tab3, tab4, tab_pncp, tab5, tab6 = st.tabs([
    "1️⃣ Apresentação",
    "2️⃣ Achados do DOU",
    "3️⃣ Buscar no DOU",
    "4️⃣ Órgãos e tipos de ato",
    "🧾 Licitações e contratos",
    "📨 Notificações Telegram",
    "🚧 Em construção",
])

# ------------------------------------------------------------
# ABA 1 — APRESENTAÇÃO
# ------------------------------------------------------------
with tab1:
    st.subheader("Visão geral do período")
    n_alta = int((df_rank["Nível"] == "Alta").sum()) if not df_rank.empty else 0
    n_media = int((df_rank["Nível"] == "Média").sum()) if not df_rank.empty else 0
    n_org = int(df_rank["Órgão raiz"].nunique()) if not df_rank.empty else 0
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("📜 Atos publicados", ui.fmt_num(len(df_dou)), ui.COR_ALERTA, "nas seções escolhidas")
    with k2:
        ui.kpi("🔎 Com palavras-chave", ui.fmt_num(len(df_f)), "#2e7d32", "entram no ranking")
    with k3:
        ui.kpi("🚨 Relevância alta", ui.fmt_num(n_alta), ui.COR_ALTA, f"+ {ui.fmt_num(n_media)} média")
    with k4:
        ui.kpi("🏛️ Órgãos com achados", ui.fmt_num(n_org), ui.COR_MEDIA, "ministérios e entidades")

    st.subheader("Destaques (um mix de órgãos)")
    if df_rank.empty:
        st.info("Sem destaques para os filtros atuais.")
    else:
        st.caption("Os 5 atos de maior relevância, no máximo 2 por órgão, para não repetir sempre o mesmo.")
        for _, row in dou.top_diverso(df_rank, 5, 2).iterrows():
            st.markdown(ui.html_card(row), unsafe_allow_html=True)

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
# ABA 2 — ACHADOS
# ------------------------------------------------------------
with tab2:
    st.subheader("Achados do DOU")
    st.caption("Atos com as palavras-chave de busca, do mais relevante para o menos relevante.")
    if df_rank.empty:
        st.info("Nenhum ato encontrado com os filtros atuais.")
    else:
        f1, f2 = st.columns(2)
        with f1:
            st.caption("Relevância")
            sel_niveis = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="fg_f_nivel")
        with f2:
            secs = sorted(df_rank["Seção"].unique())
            st.caption("Seção")
            sel_secs = ui.escolha_multipla("Seção", secs, key=f"fg_f_sec_{'|'.join(secs)}")
        raizes = sorted(df_rank["Órgão raiz"].replace("", "(sem órgão)").unique())
        sel_org = st.multiselect("🏛️ Órgãos (vazio = todos)", raizes, key="fg_f_org")

        d = df_rank[df_rank["Nível"].isin(sel_niveis) & df_rank["Seção"].isin(sel_secs)]
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

# ------------------------------------------------------------
# ABA 3 — BUSCA
# ------------------------------------------------------------
with tab3:
    st.subheader("Buscar no DOU")
    st.caption("Procura em todos os atos coletados do período (título, ementa e órgão), mesmo os que não bateram nas palavras-chave.")
    consulta = st.text_input("🔍 Termo(s) de busca", placeholder="ex.: dispensa de licitação, Ministério da Saúde", key="fg_q")
    if consulta.strip():
        if df_dou.empty:
            st.info("Não há atos coletados para buscar neste período.")
        else:
            palavras = consulta.strip().lower().split()
            blob = (df_dou["Título"] + " " + df_dou["Ementa/Resumo"] + " " + df_dou["Órgão"]).str.lower()
            mask = pd.Series(True, index=df_dou.index)
            for p in palavras:
                mask &= blob.str.contains(p, regex=False)
            res = dou.rankear(df_dou[mask], termos_alerta)
            st.caption(f"{ui.fmt_num(len(res))} atos contêm todas as palavras digitadas")
            if not res.empty:
                ui.botoes_download(res, "fiscalizagov_busca", "fg_dl3")
                ui.render_cards(res, 20, key=f"fg_t3_{consulta.strip().lower()}")
    else:
        st.info("Digite um termo para buscar.")

# ------------------------------------------------------------
# ABA 4 — ÓRGÃOS E TIPOS
# ------------------------------------------------------------
with tab4:
    st.subheader("Órgãos e tipos de ato")
    if df_rank.empty:
        st.info("Sem dados para os filtros atuais.")
    else:
        por_org = df_rank.groupby("Órgão raiz").agg(Atos=("Score", "size"), ScoreMax=("Score", "max"),
                                                    Alta=("Nível", lambda s: int((s == "Alta").sum())))
        por_org = por_org.sort_values(["Alta", "Atos"], ascending=False)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**Órgãos com mais atos de relevância alta**")
            st.bar_chart(por_org.head(12)["Alta"], horizontal=True)
        with c2:
            st.markdown("**Tipos de ato**")
            st.bar_chart(df_rank["Tipo"].replace("", "(sem tipo)").value_counts().head(12), horizontal=True)
        st.markdown("**Todos os órgãos**")
        st.dataframe(por_org.rename(columns={"ScoreMax": "Maior score", "Alta": "Atos de relevância alta"}),
                     use_container_width=True)

# ------------------------------------------------------------
# ABA — LICITAÇÕES E CONTRATOS (PNCP)
# ------------------------------------------------------------
with tab_pncp:
    tab_contratos.render()

# ------------------------------------------------------------
# ABA 5 — TELEGRAM
# ------------------------------------------------------------
with tab5:
    st.subheader("Notificações no Telegram")
    st.caption("O alerta diário automático roda no GitHub Actions (seg a sáb, 08h). Aqui você testa o bot e envia o resumo na hora.")
    bot_token = st.text_input("BOT_TOKEN", type="password", value=os.getenv("TELEGRAM_BOT_TOKEN", ""))
    chat_id = st.text_input("CHAT_ID", value=os.getenv("TELEGRAM_CHAT_ID", ""))

    b1, b2 = st.columns(2)
    if b1.button("🧪 Testar conexão", use_container_width=True):
        r = telegram_enviar(bot_token, chat_id,
                            f"🔎 <b>{APP_TITLE}</b>\n✅ Conexão configurada com sucesso.\n🕒 {dou.agora_brasilia_str()}")
        st.success("Mensagem de teste enviada.") if r["ok"] else st.error(r["error"])
    if b2.button("📨 Enviar TOP 5 agora", use_container_width=True):
        if df_rank.empty:
            st.warning("Sem achados para enviar.")
        elif erros:
            st.error("A coleta falhou: não vou enviar um resumo possivelmente incompleto.")
        else:
            msg = notif.montar_mensagem_achados(df_rank, max(datas), len(df_dou))
            r = telegram_enviar(bot_token, chat_id, msg)
            st.success("TOP 5 enviado.") if r["ok"] else st.error(r["error"])

    with st.expander("❓ Como obter o token e o ID"):
        st.markdown(
            "1. **Token:** no Telegram, abra o @BotFather → `/mybots` → seu bot → **API Token**.\n"
            "2. **ID do chat:** mande `/start` ao bot e abra `https://api.telegram.org/bot<TOKEN>/getUpdates`; "
            "procure `\"chat\":{\"id\":...}`.\n"
            "3. **Alerta automático:** no GitHub, *Settings → Secrets and variables → Actions*, crie "
            "`TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID`.\n\n"
            "⚠️ Nunca divulgue o token. Se vazar, gere outro com `/revoke` no @BotFather."
        )

# ------------------------------------------------------------
# ABA 6 — EM CONSTRUÇÃO
# ------------------------------------------------------------
with tab6:
    st.subheader("Próximos módulos")
    st.caption("Ainda não implementados. Cada um cruza o DOU com bases oficiais de controle.")
    st.markdown(
        "- 💸 **Gastos diretos** — Portal da Transparência: empenhos e pagamentos\n"
        "- 👔 **Cargos e nomeações** — nomeações e exonerações do DOU por órgão\n"
        "- ✈️ **Viagens e diárias** — Portal da Transparência\n"
        "- 🚫 **Empresas sancionadas** — CEIS/CNEP (CGU) e inidôneas do TCU cruzadas com contratos\n"
        "- 🏗️ **Obras e programas** — acompanhamento de execução\n"
        "- 🚨 **Alertas e evidências** — dossiê por fornecedor ou órgão, com histórico"
    )

st.markdown("---")
st.caption("FiscalizaGov · Dados: Diário Oficial da União (Imprensa Nacional)")
