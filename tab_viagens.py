# tab_viagens.py - aba "Viagens e diárias" (resumos de data/viagens_AAAA_*.csv; ver viagens.py)
import altair as alt
import pandas as pd
import streamlit as st

import dou
import gastos
import ui
import viagens as vg

COR = "#1a6ef5"
MESES = ["", "jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"]


@st.cache_data(ttl=3600, show_spinner=False)
def _dados_cache(ano: int):
    r = vg.ler(ano)
    if r is None:
        raise LookupError(ano)  # a ausência não fica em cache: aparece assim que o arquivo for gerado
    return r


def _dados(ano: int):
    try:
        return _dados_cache(ano)
    except LookupError:
        return None


def _brl(v) -> str:
    return gastos.fmt_brl_curto(v)


def _pct(v) -> str:
    return f"{v:.1f}%".replace(".", ",")


def _num(s) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def _barras(df, y, x, texto, altura, titulo_x, cor=COR, tooltip=None):
    base = alt.Chart(df).encode(
        y=alt.Y(f"{y}:N", sort=alt.EncodingSortField(x, order="descending"), title=None, axis=alt.Axis(labelLimit=380)),
        x=alt.X(f"{x}:Q", title=titulo_x), tooltip=tooltip or [y, texto])
    st.altair_chart((base.mark_bar(color=cor) + base.mark_text(align="left", dx=4).encode(text=f"{texto}:N"))
                    .properties(height=altura), use_container_width=True)


def _por_orgao(orgao: pd.DataFrame, nivel: str) -> pd.DataFrame:
    chaves = ["sup"] if nivel == "Órgão superior" else ["sup", "org"]
    cols = ["viagens", "realizadas", "urgentes", "internacionais", "diarias", "passagens", "outros", "devolucao",
            "total", "compra_tardia", "com_passagem"]
    g = orgao.assign(**{c: _num(orgao[c]) for c in cols}).groupby(chaves, as_index=False)[cols].sum()
    g["custo_medio"] = g["total"] / g["viagens"].where(g["viagens"] > 0)
    g["pct_urgentes"] = g["urgentes"] / g["viagens"].where(g["viagens"] > 0) * 100
    g["pct_intl"] = g["internacionais"] / g["viagens"].where(g["viagens"] > 0) * 100
    g["pct_tardia"] = g["compra_tardia"] / g["com_passagem"].where(g["com_passagem"] > 0) * 100
    return g.fillna(0)


def _card_alerta(r) -> str:
    quem = r["nome"] if r["nome"] and "sigilo" not in r["nome"].lower() else "Informações protegidas por sigilo"
    linhas = [f'🏛️ {dou.orgao_curto(r["sup"] + "/" + r["org"], 90)}',
              f'🧳 {quem}' + (f' · {r["cargo"]}' if r["cargo"] else "") + (f' · CPF {r["cpf"]}' if r["cpf"] and "sigilo" not in r["cpf"].lower() else ""),
              f'🧾 Diárias {vg.fmt_brl(r["diarias"])} · passagens {vg.fmt_brl(r["passagens"])}'
              + (f' · devolução {vg.fmt_brl(r["devolucao"])}' if float(r["devolucao"] or 0) > 0 else "")]
    return ui.card_generico(
        r["score"], f'{vg.fmt_brl(r["total"])} · {r["destinos"] or "destino não informado"}', "",
        "Internacional" if str(r["internacional"]) == "True" else r["situacao"],
        f'{r["ini"]} a {r["fim"]} · {int(float(r["dias"] or 0))} dia(s)', linhas, r["motivo"],
        [c for c in str(r["pontos"]).split(" | ") if c])


def render() -> None:
    st.subheader("Viagens e diárias")
    st.caption("Viagens a serviço do Executivo federal · Fonte: arquivo Viagens a Serviço da CGU (Portal da Transparência). "
               "Os números vêm de um resumo que o GitHub Actions refaz toda semana.")

    ano_atual = dou.hoje_brasilia().year
    ano = st.selectbox("Ano", [ano_atual, ano_atual - 1], key="vg_ano")
    r = _dados(ano)
    if r is None:
        st.info(f"Ainda não há resumo de viagens para {ano}. Ele é gerado toda segunda-feira pelo GitHub Actions "
                "(workflow “Atualizar viagens”).")
        return
    dados, meta = r
    orgao_df, mes_df, vi_df, dest_df, al_df = (dados[k] for k in vg.RECURSOS)
    st.caption(f"📅 Resumo de {ano} · gerado em {meta.get('gerado_em', '—')}"
               + (" · inclui viagens já programadas para os meses seguintes" if ano == ano_atual else ""))

    tot = _por_orgao(orgao_df, "Órgão superior")
    n, total = float(tot["viagens"].sum()), float(tot["total"].sum())
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("🧳 Viagens", ui.fmt_num(n), ui.COR_ALERTA, f'{ui.fmt_num(tot["realizadas"].sum())} realizadas')
    with k2:
        ui.kpi("💰 Gasto total", _brl(total), "#2e7d32", f'diárias {_brl(tot["diarias"].sum())} · passagens {_brl(tot["passagens"].sum())}')
    with k3:
        ui.kpi("⚡ Viagens urgentes", _pct(tot["urgentes"].sum() / n * 100 if n else 0), ui.COR_MEDIA,
               "pedidas com pouca antecedência")
    with k4:
        ui.kpi("🌎 Internacionais", ui.fmt_num(tot["internacionais"].sum()), ui.COR_ALTA,
               f'custo médio por viagem {_brl(total / n if n else 0)}')

    t_geral, t_orgaos, t_viaj, t_alertas, t_dest = st.tabs(
        ["📊 Visão geral", "🏛️ Órgãos", "🧳 Viajantes", "🚩 Pontos de atenção", "🌎 Destinos"])

    with t_geral:
        st.markdown("**Gasto por mês de início da viagem**")
        m = mes_df.assign(**{c: _num(mes_df[c]) for c in ("diarias", "passagens", "outros", "mes")})
        m = m.groupby("mes", as_index=False)[["diarias", "passagens", "outros"]].sum()
        m["Mês"] = m["mes"].astype(int).map(lambda i: MESES[i] if 0 < i < 13 else "?")
        long = m.melt(["mes", "Mês"], value_vars=["diarias", "passagens", "outros"], var_name="Tipo", value_name="v")
        long["Tipo"] = long["Tipo"].map({"diarias": "Diárias", "passagens": "Passagens", "outros": "Outros gastos"})
        long["R$ milhões"] = long["v"] / 1e6
        ch = alt.Chart(long).mark_bar().encode(
            x=alt.X("Mês:N", sort=MESES[1:], title=None), y=alt.Y("R$ milhões:Q", title="R$ milhões"),
            color=alt.Color("Tipo:N", scale=alt.Scale(range=[COR, "#b7791f", "#9e9e9e"])),
            tooltip=["Mês", "Tipo", alt.Tooltip("R$ milhões:Q", format=",.1f")]).properties(height=260)
        st.altair_chart(ch, use_container_width=True)

        st.markdown("**Órgãos superiores que mais gastam**")
        met = st.radio("Medir por", ["Gasto total", "Diárias", "Passagens", "Número de viagens"], horizontal=True, key="vg_met")
        col = {"Gasto total": "total", "Diárias": "diarias", "Passagens": "passagens", "Número de viagens": "viagens"}[met]
        top = tot.nlargest(12, col).copy()
        top["Valor"] = (top[col].map(_brl) if col != "viagens" else top[col].map(ui.fmt_num)) + "  ·  " + \
            top["pct_urgentes"].map(_pct) + " urgentes"
        top["v"] = top[col] / (1e6 if col != "viagens" else 1)
        _barras(top, "sup", "v", "Valor", 34 * len(top) + 40, "R$ milhões" if col != "viagens" else "viagens",
                tooltip=["sup", "Valor"])

    with t_orgaos:
        c1, c2 = st.columns(2)
        nivel = c1.radio("Comparar", ["Órgão superior", "Órgão solicitante"], horizontal=True, key="vg_niv")
        vmin = c2.number_input("Mínimo de viagens", min_value=0, value=200, step=100, key="vg_min",
                               help="Esconde órgãos com poucas viagens, onde um percentual pesa pouco.")
        g = _por_orgao(orgao_df, nivel)
        g = g[g["viagens"] >= vmin]
        ordem = st.radio("Ordenar por", ["Gasto total", "Custo médio por viagem", "% de viagens urgentes",
                                         "% de passagens compradas com menos de 5 dias"], horizontal=True, key="vg_ord")
        col = {"Gasto total": "total", "Custo médio por viagem": "custo_medio", "% de viagens urgentes": "pct_urgentes",
               "% de passagens compradas com menos de 5 dias": "pct_tardia"}[ordem]
        g = g.sort_values(col, ascending=False)
        if g.empty:
            st.info("Nenhum órgão com esses filtros.")
        else:
            nome = "sup" if nivel == "Órgão superior" else "org"
            top = g.head(20).copy()
            if nome == "org":
                top["org"] = top["org"] + " (" + top["sup"].str.slice(0, 24) + ")"
            money = col in ("total", "custo_medio")
            top["Valor"] = top[col].map(_brl if money else _pct) + "  ·  " + top["viagens"].map(ui.fmt_num) + " viagens"
            top["v"] = top[col] / (1e6 if col == "total" else 1)
            _barras(top, nome, "v", "Valor", 32 * len(top) + 40,
                    {"total": "R$ milhões", "custo_medio": "R$ por viagem"}.get(col, "%"), tooltip=[nome, "Valor"])
            tab = g.rename(columns={"sup": "Órgão superior", "org": "Órgão solicitante", "viagens": "Viagens",
                                    "total": "Gasto total", "diarias": "Diárias", "passagens": "Passagens",
                                    "custo_medio": "Custo médio", "pct_urgentes": "% urgentes",
                                    "pct_intl": "% internacionais", "pct_tardia": "% passagens < 5 dias"})
            cols = [c for c in (["Órgão superior"] + (["Órgão solicitante"] if nome == "org" else []) +
                                ["Viagens", "Gasto total", "Diárias", "Passagens", "Custo médio", "% urgentes",
                                 "% internacionais", "% passagens < 5 dias"]) if c in tab.columns]
            vis = tab[cols].copy()
            for c in ("Gasto total", "Diárias", "Passagens", "Custo médio"):
                vis[c] = vis[c].map(_brl)
            for c in ("% urgentes", "% internacionais", "% passagens < 5 dias"):
                vis[c] = vis[c].map(_pct)
            vis["Viagens"] = vis["Viagens"].map(ui.fmt_num)
            st.dataframe(vis, use_container_width=True, hide_index=True)
            ui.botoes_download(tab[cols], f"fiscalizagov_viagens_orgaos_{ano}", "vg_dl1")
        st.caption("⚠️ Urgente = viagem pedida com pouca antecedência. Em alguns órgãos (fiscalização, segurança, defesa) "
                   "isso é do trabalho; compare com órgãos parecidos antes de concluir.")

    with t_viaj:
        st.caption("Pessoas que mais viajam a serviço. Viajar muito pode ser a própria função (fiscais, peritos, militares, "
                   "pilotos). Os registros sob sigilo não entram aqui.")
        sups = sorted(vi_df["sup"].unique())
        c1, c2 = st.columns(2)
        sel = c1.selectbox("Órgão superior", ["Todos"] + sups, key="vg_vsup")
        ordem_v = c2.radio("Ordenar por", ["Gasto total", "Número de viagens"], horizontal=True, key="vg_vord")
        d = vi_df if sel == "Todos" else vi_df[vi_df["sup"] == sel]
        col = "total" if ordem_v == "Gasto total" else "viagens"
        d = d.assign(**{c: _num(d[c]) for c in ("viagens", "dias", "urgentes", "internacionais", "diarias", "passagens", "total")})
        d = d.sort_values(col, ascending=False).head(300)
        if d.empty:
            st.info("Sem dados para este recorte.")
        else:
            freq = int((d["viagens"] >= vg.LIMITE_FREQUENTE).sum())
            st.caption(f"{freq} de {len(d)} nesta lista têm {vg.LIMITE_FREQUENTE} ou mais viagens no ano.")
            vis = pd.DataFrame({
                "Nome": d["nome"], "CPF": d["cpf"], "Cargo": d["cargo"], "Função": d["funcao"],
                "Órgão superior": d["sup"], "Viagens": d["viagens"].astype(int), "Dias viajados": d["dias"].astype(int),
                "% urgentes": (d["urgentes"] / d["viagens"] * 100).map(_pct),
                "Internacionais": d["internacionais"].astype(int), "Gasto total": d["total"].map(_brl)})
            st.dataframe(vis, use_container_width=True, hide_index=True)
            ui.botoes_download(d, f"fiscalizagov_viajantes_{ano}", "vg_dl2")

    with t_alertas:
        st.caption("Viagens com mais pontos de atenção. Cada ponto é uma regra simples, mostrada no card; "
                   "viagem a serviço é gasto previsto e nada aqui prova irregularidade.")
        a = al_df.copy()
        a["score"] = _num(a["score"]).astype(int)
        a["nivel"] = a["score"].map(dou.nivel_relevancia)
        f1, f2 = st.columns(2)
        with f1:
            st.caption("Relevância")
            niveis = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="vg_f_nivel")
        sel_sup = f2.selectbox("Órgão superior", ["Todos"] + sorted(a["sup"].unique()), key="vg_asup")
        so_intl = st.checkbox("Só viagens internacionais", key="vg_intl")
        a = a[a["nivel"].isin(niveis)]
        if sel_sup != "Todos":
            a = a[a["sup"] == sel_sup]
        if so_intl:
            a = a[a["internacional"].astype(str) == "True"]
        cA, cB = st.columns([4, 2])
        cA.caption(f"{ui.fmt_num(len(a))} viagens nos filtros (lista das {ui.fmt_num(len(al_df))} de maior pontuação)")
        with cB:
            ui.botoes_download(a.drop(columns=["nivel"]), f"fiscalizagov_viagens_alertas_{ano}", "vg_dl3")
        if a.empty:
            st.info("Nenhuma viagem com esses filtros.")
        else:
            ui.render_cards(a, 20, key=f"vg_cards_{sel_sup}_{so_intl}_{'|'.join(niveis)}", card=_card_alerta)

    with t_dest:
        sig = dest_df[dest_df["destinos"].str.contains("sigilo", case=False)]
        d = dest_df[~dest_df["destinos"].str.contains("sigilo", case=False)].copy()
        d = d.assign(**{c: _num(d[c]) for c in ("viagens", "total", "internacionais")})
        if not sig.empty:
            s = float(_num(sig["viagens"]).sum())
            st.info(f"{ui.fmt_num(s)} viagens ({_pct(s / n * 100 if n else 0)}) têm destino protegido por sigilo e não "
                    "aparecem no ranking.")
        top = d.nlargest(20, "viagens")
        top["Valor"] = top["viagens"].map(ui.fmt_num) + " viagens  ·  " + top["total"].map(_brl)
        _barras(top, "destinos", "viagens", "Valor", 32 * len(top) + 40, "viagens", tooltip=["destinos", "Valor"])
        st.markdown("**Destinos de maior gasto**")
        topv = d.nlargest(15, "total").copy()
        topv["Valor"] = topv["total"].map(_brl) + "  ·  " + topv["viagens"].map(ui.fmt_num) + " viagens"
        topv["v"] = topv["total"] / 1e6
        _barras(topv, "destinos", "v", "Valor", 32 * len(topv) + 40, "R$ milhões", cor="#b7791f", tooltip=["destinos", "Valor"])

    with st.expander("📖 Como ler esta aba"):
        st.markdown(
            "- **Gasto** = diárias + passagens + outros gastos − devoluções, por viagem, somado pelo mês de início.\n"
            "- **Urgente** é a marcação do próprio sistema de viagens (pedido com pouca antecedência). Quase 6 em cada 10 viagens "
            "estão marcadas assim, então o que importa é comparar órgãos parecidos, não o número isolado.\n"
            "- **Pontos de atenção:** valor alto (≥ R$ 30 mil) e muito alto (≥ R$ 100 mil); viagem internacional; viagem não "
            "realizada com valor pago e sem devolução; urgente sem justificativa; passagem emitida depois do início ou com "
            "menos de 3 dias de antecedência (pode ser remarcação); viagem de mais de 30 dias; viajante com 60 ou mais viagens "
            "no ano.\n"
            "- **Sigilo:** cerca de 14% das viagens têm viajante e destino protegidos por sigilo e ficam fora dos rankings de "
            "pessoas e de destinos (mas contam nos totais por órgão).\n"
            "- CPF aparece como a fonte publica (mascarado). Viagens a serviço são gasto previsto em lei; esta aba mostra "
            "onde olhar primeiro, **não aponta irregularidade**."
        )
