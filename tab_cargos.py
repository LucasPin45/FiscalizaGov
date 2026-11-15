# tab_cargos.py - aba "Cargos e nomeações": Presidência da República e alta gestão (atos do DOU)
import datetime as dt

import altair as alt
import pandas as pd
import streamlit as st

import cargos
import dou
import ui

SECOES = ["do2", "do1", "do1e", "do2e"]  # atos de pessoal estão quase todos na Seção 2
PERIODOS = ["Último dia com edição", "Últimos 7 dias úteis", "Últimos 15 dias úteis"]


class _Falha(Exception):
    def __init__(self, erros):
        super().__init__("; ".join(erros))
        self.erros = erros


@st.cache_data(ttl=1800, show_spinner=False)
def _dia(data: dt.date) -> pd.DataFrame:
    df, erros = dou.coletar_dou(data, SECOES)
    if erros:
        raise _Falha(erros)  # exceção não é guardada: falha nunca fica presa no cache
    return df


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _pipeline(datas: tuple):
    """DOU dos dias -> atos de pessoal do Executivo -> texto completo -> movimentações."""
    frames = [_dia(d) for d in datas]
    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=dou.COLUNAS)
    previa = cargos.analisar(df)
    textos = cargos.baixar_textos(previa["Link"].tolist(), workers=6) if not previa.empty else {}
    atos = cargos.analisar(df, textos)
    return cargos.movimentos_df(atos), len(atos), int(sum(1 for t in textos.values() if t)), len(textos)


def _dias_uteis(n: int, hoje: dt.date) -> list:
    out, d = [], hoje
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d -= dt.timedelta(days=1)
    return out


def _card(r) -> str:
    quem = r["Nome"] or "(nome não identificado no trecho)"
    linhas = []
    if r["Cargo"]:
        linhas.append(f'💼 {r["Cargo"]}')
    if r["Códigos"]:
        linhas.append(f'🏷️ {r["Códigos"]}')
    unidade = r["Unidade da Presidência"] or dou.orgao_curto(r["Órgão"], 80)
    chips = [m for m in r["Motivos"].split(" | ") if m]
    if r["Substituição"]:
        chips.append("substituição ou interinidade")
    return ui.card_generico(
        r["Score"], f'{r["Ação"]}: {quem}', r["Link"], r["Nível"], f'{r["Data"]} · {unidade}', linhas, "", chips)


def _grafico_por_dia(mov: pd.DataFrame) -> None:
    d = mov[mov["Ação"].isin(["Nomeação", "Exoneração"])].copy()
    if d.empty:
        return
    d["dia"] = pd.to_datetime(d["Data"], format="%d/%m/%Y", errors="coerce")
    g = d.groupby(["dia", "Ação"]).size().reset_index(name="Quantidade")
    ch = alt.Chart(g).mark_bar().encode(
        x=alt.X("dia:T", title=None, axis=alt.Axis(format="%d/%m")), y=alt.Y("Quantidade:Q", title="Movimentações"),
        color=alt.Color("Ação:N", scale=alt.Scale(domain=["Nomeação", "Exoneração"], range=["#2e7d32", "#d32f2f"])),
        tooltip=["dia:T", "Ação:N", "Quantidade:Q"]).properties(height=220)
    st.altair_chart(ch, use_container_width=True)


def render() -> None:
    st.subheader("Cargos e nomeações")
    st.caption("Nomeações, exonerações e designações publicadas no DOU, com destaque para a **Presidência da República** "
               "e para os cargos de **alta gestão**. O que aparece aqui é o que o ato publicou: confira sempre o link (↗).")

    modo = ui.escolha_unica("Período", PERIODOS, PERIODOS[0], key="cg_per")
    hoje = dou.hoje_brasilia()
    if modo == PERIODOS[0]:
        d, _, erros = dou.ultimo_dia_com_edicao(lambda x: _seguro(x), hoje)
        if erros:
            st.error("⚠️ Falha ao ler o DOU: " + "; ".join(erros[:3]) + ". Tente de novo em instantes.")
            return
        datas = [d]
    else:
        n = 7 if "7" in modo else 15
        datas = _dias_uteis(n, hoje)
        chave = f"cg_ok_{n}_{datas[0].isoformat()}"
        if not st.session_state.get(chave):
            st.info(f"Analisa {n} dias úteis do DOU e baixa o texto de cada ato de pessoal: leva de **1 a 3 minutos** "
                    "na primeira vez. Depois fica guardado por 6 horas.")
            if not st.button(f"📥 Analisar {n} dias úteis", type="primary", key=f"cg_btn_{n}"):
                return
            st.session_state[chave] = True

    try:
        with st.spinner("Lendo os atos de pessoal no DOU…"):
            mov, n_atos, n_textos, n_total = _pipeline(tuple(datas))
    except _Falha as ex:
        st.error("⚠️ **Falha ao ler o DOU.** Os resultados poderiam ficar incompletos, então não mostro nada parcial. "
                 f"Tente de novo em instantes.\n\n- {'; '.join(ex.erros[:3])}")
        return

    periodo = ui.fmt_data(datas[0]) if len(datas) == 1 else f"{ui.fmt_data(min(datas))} a {ui.fmt_data(max(datas))}"
    st.caption(f"📅 DOU de {periodo} · {ui.fmt_num(n_atos)} atos de pessoal do Executivo · "
               f"texto completo lido em {ui.fmt_num(n_textos)} de {ui.fmt_num(n_total)}")
    if n_total and n_textos < n_total:
        st.warning(f"Não consegui baixar o texto completo de {n_total - n_textos} atos; neles uso só o trecho inicial, "
                   "então nomes e cargos podem estar incompletos.")
    if mov.empty:
        st.info("Nenhuma movimentação de pessoal do Executivo neste período.")
        return

    pres = mov[mov["Presidência"] | mov["Decreto presidencial"]]
    alta = mov[mov["Nível"] == "Alta direção"]
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("👥 Movimentações", ui.fmt_num(len(mov)), ui.COR_ALERTA, "nomeações, exonerações, designações…")
    with k2:
        ui.kpi("🏛️ Presidência e decretos", ui.fmt_num(len(pres)), ui.COR_MEDIA, "inclui decretos do Presidente")
    with k3:
        ui.kpi("⭐ Alta direção", ui.fmt_num(len(alta)), ui.COR_ALTA, "Ministro, CCE 1.17-1.18, reitor…")
    with k4:
        nom = int((mov["Ação"] == "Nomeação").sum())
        exo = int((mov["Ação"] == "Exoneração").sum())
        ui.kpi("🔁 Nomeações × exonerações", f"{nom} × {exo}", "#2e7d32", "no Executivo, no período")

    sub_presid, sub_alta, sub_todos, sub_graf = st.tabs(
        ["🏛️ Presidência da República", "⭐ Alta gestão", "👥 Todas as movimentações", "📊 Por dia"])

    with sub_presid:
        st.caption("Casa Civil, Secretarias da Presidência, Gabinete Pessoal, Vice-Presidência, AGU e os decretos do "
                   "Presidente da República.")
        if pres.empty:
            st.info("Sem movimentações da Presidência neste período.")
        else:
            unidades = sorted(pres["Unidade da Presidência"].replace("", "Gabinete e demais").unique())
            sel_u = ui.escolha_multipla("Unidade", unidades, key=f"cg_u_{'|'.join(unidades)}")
            niveis = ui.escolha_multipla("Nível", ["Alta direção", "Direção intermediária", "Demais"], key="cg_pn")
            so_nomexo = st.checkbox("Só nomeações e exonerações (esconde designações de substitutos)", key="cg_pnx")
            d = pres[pres["Unidade da Presidência"].replace("", "Gabinete e demais").isin(sel_u) & pres["Nível"].isin(niveis)]
            if so_nomexo:
                d = d[d["Ação"].isin(["Nomeação", "Exoneração"])]
            cA, cB = st.columns([4, 2])
            cA.caption(f"{ui.fmt_num(len(d))} movimentações")
            with cB:
                ui.botoes_download(d.drop(columns=["Presidência"]), "fiscalizagov_presidencia", "cg_dl1")
            ui.render_cards(d, 20, key="cg_cards_pres", card=_card)

    with sub_alta:
        st.caption("Ministro de Estado, Secretário-Executivo e Nacional, Natureza Especial, CCE/FCE 1.17 e 1.18 "
                   "(equivalentes ao DAS 5 e 6), presidentes e diretores-gerais de entidades, reitores e comandantes.")
        d = alta
        if d.empty:
            st.info("Nenhuma movimentação de alta direção neste período.")
        else:
            ui.botoes_download(d.drop(columns=["Presidência"]), "fiscalizagov_alta_gestao", "cg_dl2")
            ui.render_cards(d, 20, key="cg_cards_alta", card=_card)

    with sub_todos:
        c1, c2 = st.columns(2)
        with c1:
            acoes = sorted(mov["Ação"].unique())
            sel_a = st.multiselect("Ação", acoes, default=[a for a in acoes if a in ("Nomeação", "Exoneração")] or acoes,
                                   key="cg_a")
        with c2:
            sel_n = st.multiselect("Nível", ["Alta direção", "Direção intermediária", "Demais"],
                                   default=["Alta direção", "Direção intermediária"], key="cg_n")
        raizes = sorted(mov["Órgão"].map(dou.orgao_raiz).replace("", "(sem órgão)").unique())
        sel_o = st.multiselect("🏛️ Órgão superior (vazio = todos)", raizes, key="cg_o")
        q = st.text_input("🔍 Buscar por nome ou cargo", key="cg_q")
        d = mov[mov["Ação"].isin(sel_a) & mov["Nível"].isin(sel_n)]
        if sel_o:
            d = d[d["Órgão"].map(dou.orgao_raiz).replace("", "(sem órgão)").isin(sel_o)]
        if q.strip():
            blob = (d["Nome"] + " " + d["Cargo"] + " " + d["Órgão"]).str.lower()
            for p in q.strip().lower().split():
                blob_ok = blob.str.contains(p, regex=False)
                d, blob = d[blob_ok], blob[blob_ok]
        cA, cB = st.columns([4, 2])
        cA.caption(f"{ui.fmt_num(len(d))} movimentações")
        with cB:
            ui.botoes_download(d.drop(columns=["Presidência"]), "fiscalizagov_cargos", "cg_dl3")
        if d.empty:
            st.info("Nada com esses filtros.")
        else:
            ui.render_cards(d, 20, key=f"cg_cards_todos_{q}", card=_card)

    with sub_graf:
        st.caption("Nomeações e exonerações por dia de publicação (toda a lista, com os filtros de nível abaixo).")
        nivel_g = st.multiselect("Nível", ["Alta direção", "Direção intermediária", "Demais"],
                                 default=["Alta direção", "Direção intermediária"], key="cg_ng")
        _grafico_por_dia(mov[mov["Nível"].isin(nivel_g)])
        topo = mov[mov["Nível"].isin(["Alta direção", "Direção intermediária"])]
        if not topo.empty:
            st.markdown("**Órgãos superiores com mais movimentações de direção**")
            st.bar_chart(topo["Órgão"].map(dou.orgao_raiz).replace("", "(sem órgão)").value_counts().head(12),
                         horizontal=True)

    with st.expander("📖 Como ler esta aba (e o que ela não faz)"):
        st.markdown(
            "- **Alta direção:** decreto do Presidente com cargo de alta direção; Ministro de Estado; Secretário-Executivo, "
            "Especial ou Nacional; Natureza Especial; CCE/FCE 1.17 e 1.18 (≈ DAS 5 e 6); presidentes e diretores-gerais "
            "de entidades; reitores; comandantes das Forças. **Direção intermediária:** CCE/FCE 1.15 e 1.16 (≈ DAS 4), "
            "diretores, secretários, superintendentes, chefes de gabinete.\n"
            "- **Substituições:** designação como substituto eventual ou interino não é nomeação; aparece marcada e com nível "
            "no máximo intermediário.\n"
            "- **Leitura automática:** nomes e cargos são lidos do texto do ato por padrões. Em atos com tabelas ou texto "
            "fora do padrão pode faltar o nome; o link leva ao ato oficial.\n"
            "- **O que o DOU sozinho não mostra:** parentesco (nepotismo, Súmula Vinculante 13), vínculo anterior com "
            "empresas e conflito de interesses. Esses cruzamentos exigem outras bases.\n"
            "- Nomeação é ato público e legítimo: esta aba mostra **quem entrou e saiu**, não aponta irregularidade."
        )


def _seguro(data: dt.date):
    """(df, erros) do dia, sem levantar (para ultimo_dia_com_edicao)."""
    try:
        return _dia(data), []
    except _Falha as ex:
        return pd.DataFrame(columns=dou.COLUNAS), ex.erros
