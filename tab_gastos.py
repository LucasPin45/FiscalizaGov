# tab_gastos.py - aba "Gastos do governo": ranking e graficos dos maiores gastadores
import datetime as dt
import os
import time

import altair as alt
import pandas as pd
import streamlit as st

import dou
import gastos
import ui

VALIDADE_S = 12 * 3600
COR = "#1a6ef5"
COR_ANT = "rgba(128,128,128,.55)"


@st.cache_resource
def _armazem() -> dict:
    """Guarda ano -> (instante, DataFrame) so quando a coleta foi COMPLETA (sem erros)."""
    return {}


def _chave_api() -> str:
    try:
        return str(st.secrets["TRANSPARENCIA_API_KEY"])
    except Exception:
        return os.getenv("TRANSPARENCIA_API_KEY", "")


def _do_armazem(ano: int):
    item = _armazem().get(ano)
    if item and time.time() - item[0] < VALIDADE_S:
        return item[1]
    return None


def _coletar(ano: int, chave: str, rotulo: str):
    """Coleta com barra de progresso; so guarda no armazem se completa."""
    barra = st.progress(0.0, text=f"Coletando gastos de {ano}…")
    df, erros = gastos.coletar_ano(
        ano, chave, progresso=lambda i, t: barra.progress(i / t, text=f"Coletando {rotulo} {ano}… {i}/{t} órgãos superiores"))
    barra.empty()
    if not erros:
        _armazem()[ano] = (time.time(), df)
    return df, erros


def _barras(df: pd.DataFrame, rotulo: str, valor: str, titulo_x: str, altura: int, cor=COR):
    d = df.copy()
    d["bi"] = d[valor] / 1e9
    d["Valor"] = d[valor].map(gastos.fmt_brl_curto)
    base = alt.Chart(d).encode(
        y=alt.Y(f"{rotulo}:N", sort="-x", title=None, axis=alt.Axis(labelLimit=340)),
        x=alt.X("bi:Q", title=titulo_x),
        tooltip=[alt.Tooltip(f"{rotulo}:N", title="Órgão"), alt.Tooltip("Valor:N", title="Valor")] +
                ([alt.Tooltip("Órgão superior:N", title="Superior")] if "Órgão superior" in d.columns and rotulo != "Órgão superior" else []),
    )
    barras = base.mark_bar(color=cor)
    texto = base.mark_text(align="left", dx=4).encode(text="Valor:N")
    st.altair_chart((barras + texto).properties(height=altura), use_container_width=True)


def _tabela(df: pd.DataFrame, colunas_valor: list) -> pd.DataFrame:
    out = df.copy()
    for c in colunas_valor:
        out[c] = out[c].map(gastos.fmt_brl_curto)
    for c in ("% do total", "% pago do empenhado"):
        if c in out.columns:
            out[c] = out[c].map(lambda v: f"{v:.1f}%".replace(".", ","))
    return out


def render() -> None:
    st.subheader("Gastos do governo federal")
    st.caption("Execução orçamentária por órgão · Fonte: Portal da Transparência (CGU). "
               "**Empenhado** = compromisso assumido · **Liquidado** = entrega reconhecida · **Pago** = dinheiro que saiu.")

    api = _chave_api()
    if not api:
        st.warning("Falta o secret `TRANSPARENCIA_API_KEY` (chave gratuita do Portal da Transparência).")
        return

    ano_atual = dou.hoje_brasilia().year
    c1, c2 = st.columns([1, 2])
    ano = c1.selectbox("Ano", list(range(ano_atual, ano_atual - 5, -1)), key="gs_ano")
    metrica = c2.radio("Medir por", gastos.METRICAS, horizontal=True, key="gs_metrica",
                       help="Pago é o gasto efetivo. Empenhado mostra o que já está comprometido.")
    c3, c4 = st.columns(2)
    incluir = c3.checkbox("Incluir encargos da dívida, refinanciamento e transferências a estados e municípios",
                          value=False, key="gs_encargos",
                          help="Esses itens não são gasto próprio de um ministério e distorcem o ranking. Ficam fora por padrão.")
    comparar = c4.checkbox(f"Comparar com {ano - 1}", value=False, key="gs_comparar",
                           help="Dobra o tempo de coleta. O ano em curso é parcial, o anterior é completo.")

    df = _do_armazem(ano)
    df_ant = _do_armazem(ano - 1) if comparar else None
    faltam = [a for a, d in ((ano, df), (ano - 1, df_ant if comparar else 0)) if d is None]
    if faltam:
        n = len(faltam)
        st.info(f"A coleta percorre cerca de 60 órgãos superiores e leva **1 a 2 minutos por ano** "
                f"({'2 anos: até 4 minutos' if n == 2 else '1 ano'}). Depois fica guardada por 12 horas.")
        if not st.button(f"💸 Carregar gastos ({', '.join(str(a) for a in faltam)})", type="primary", key="gs_btn"):
            return
        erros_total = []
        for a in faltam:
            d, er = _coletar(a, api, "gastos de")
            erros_total += [f"{a} · {x}" for x in er]
            if a == ano:
                df = d
            else:
                df_ant = d
        if erros_total:
            st.error("⚠️ **Falha parcial na coleta.** O ranking pode estar incompleto, e a ausência de um órgão "
                     "**não** significa ausência de gasto. Tente de novo.\n\n" + "\n".join(f"- {x}" for x in erros_total[:6]))
            if any("chave" in x for x in erros_total):
                return

    if df is None or df.empty:
        st.info("Sem dados para este ano.")
        return

    base = gastos.filtrar_finalisticos(df, incluir)
    sup = gastos.ranking_superiores(base, metrica)
    org = gastos.ranking_orgaos(base, metrica)
    if sup.empty:
        st.info("Sem dados para os filtros escolhidos.")
        return

    total = float(base[metrica].sum())
    execucao = float(base["Pago"].sum() / base["Empenhado"].sum() * 100) if base["Empenhado"].sum() else 0.0
    parcial = " (ano em curso, parcial)" if ano == ano_atual else ""
    st.caption(f"📅 {ano}{parcial} · {ui.fmt_num(len(org))} órgãos em {ui.fmt_num(len(sup))} órgãos superiores")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi(f"💰 Total {metrica.lower()}", gastos.fmt_brl_curto(total), "#2e7d32", f"em {ano}")
    with k2:
        top = sup.iloc[0]
        ui.kpi("🥇 Maior gastador", top["Órgão superior"][:38], ui.COR_ALERTA,
               f'{gastos.fmt_brl_curto(top[metrica])} · {top["% do total"]:.1f}% do total'.replace(".", ","))
    with k3:
        ui.kpi("🎯 Top 5 concentram", f'{gastos.concentracao(sup, 5):.0f}%'.replace(".", ","), ui.COR_MEDIA,
               "do total, em 5 órgãos superiores")
    with k4:
        ui.kpi("⚙️ Pago do empenhado", f"{execucao:.0f}%".replace(".", ","), ui.COR_ALTA,
               "o resto é compromisso ainda não pago")

    st.markdown("#### 🏛️ Maiores gastadores — órgãos superiores")
    st.caption("Ministérios e Poderes. Soma de todas as entidades vinculadas.")
    _barras(sup.head(15), "Órgão superior", metrica, "R$ bilhões", 30 * min(15, len(sup)) + 40)

    st.markdown("#### 🏢 Maiores órgãos e entidades")
    st.caption("Autarquias, fundos, universidades e empresas dependentes. Passe o mouse para ver o ministério.")
    _barras(org.head(20), "Órgão", metrica, "R$ bilhões", 28 * min(20, len(org)) + 40)

    if comparar and df_ant is not None and not df_ant.empty:
        st.markdown(f"#### 📊 {ano} × {ano - 1} — maiores órgãos superiores")
        if ano == ano_atual:
            st.warning(f"{ano} é um ano em curso (parcial); {ano - 1} está completo. "
                       "Compare como ritmo de execução, não como queda ou alta.")
        ant = gastos.ranking_superiores(gastos.filtrar_finalisticos(df_ant, incluir), metrica)[["Órgão superior", metrica]]
        top10 = sup.head(10)[["Órgão superior", metrica]].merge(ant, on="Órgão superior", how="left",
                                                                suffixes=(f" {ano}", f" {ano - 1}")).fillna(0)
        long = top10.melt("Órgão superior", var_name="Ano", value_name="v")
        long["bi"] = long["v"] / 1e9
        long["Valor"] = long["v"].map(gastos.fmt_brl_curto)
        ch = alt.Chart(long).mark_bar().encode(
            y=alt.Y("Órgão superior:N", sort=alt.EncodingSortField("bi", op="max", order="descending"), title=None,
                    axis=alt.Axis(labelLimit=340)),
            yOffset="Ano:N", x=alt.X("bi:Q", title="R$ bilhões"),
            color=alt.Color("Ano:N", scale=alt.Scale(range=[COR, COR_ANT])),
            tooltip=["Órgão superior:N", "Ano:N", "Valor:N"]).properties(height=44 * len(top10) + 40)
        st.altair_chart(ch, use_container_width=True)

    st.markdown("#### ⏳ Comprometido e ainda não pago")
    st.caption("Empenhado menos pago: dinheiro que o órgão já se comprometeu a gastar e ainda não pagou "
               "(parte vira “restos a pagar”).")
    pend = sup.assign(**{"Empenhado e não pago": (sup["Empenhado"] - sup["Pago"]).clip(lower=0)})
    pend = pend.sort_values("Empenhado e não pago", ascending=False).head(10)
    _barras(pend, "Órgão superior", "Empenhado e não pago", "R$ bilhões", 30 * len(pend) + 40, cor="#b7791f")

    st.markdown("#### 🔎 Detalhe por ministério ou Poder")
    escolhido = st.selectbox("Órgão superior", sup["Órgão superior"].tolist(), key="gs_sel")
    det = gastos.ranking_orgaos(base[base["Órgão superior"] == escolhido], metrica)
    cols = ["Órgão", "Empenhado", "Liquidado", "Pago", "% do total", "% pago do empenhado"]
    st.dataframe(_tabela(det[cols], ["Empenhado", "Liquidado", "Pago"]), use_container_width=True, hide_index=True)

    with st.expander("📥 Baixar tabelas completas"):
        ui.botoes_download(org.drop(columns=["Cód. superior", "Cód. órgão"]), f"fiscalizagov_gastos_{ano}", "gs_dl")

    with st.expander("📖 Como ler estes números"):
        st.markdown(
            "- **Gasto grande não é gasto irregular.** O ranking mostra *onde está o dinheiro*; ministérios como Saúde, "
            "Educação e Previdência são grandes por natureza (folha, benefícios, transferências).\n"
            "- **Pago × Empenhado:** a distância entre os dois é dinheiro comprometido e ainda não pago.\n"
            "- **Ano em curso é parcial.** Não compare com o ano fechado como se fosse queda.\n"
            "- **Encargos da dívida, refinanciamento e transferências a estados e municípios** ficam fora por padrão: "
            "são fluxos financeiros, não despesa de um ministério.\n"
            "- Os valores vêm da execução orçamentária (SIAFI) e podem ser ajustados depois."
        )
