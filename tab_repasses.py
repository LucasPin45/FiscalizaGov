# tab_repasses.py - aba "Obras e programas": repasses da União (convênios e contratos de repasse), SC em destaque
import altair as alt
import pandas as pd
import streamlit as st

import dou
import gastos
import repasses as rp
import ui

COR = "#1a6ef5"
COR_SC = "#d32f2f"
COR_NEUTRA = "rgba(128,128,128,.55)"


@st.cache_data(ttl=3600, show_spinner=False)
def _dados_cache():
    r = rp.ler()
    if r is None:
        raise LookupError("repasses")  # a ausência não fica em cache
    return r


def _dados():
    try:
        return _dados_cache()
    except LookupError:
        return None


def _brl(v) -> str:
    return gastos.fmt_brl_curto(v)


def _pct(v) -> str:
    return f"{v:.1f}%".replace(".", ",")


def _num(s) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0)


def _bool(s) -> pd.Series:
    return s.astype(str).str.lower().eq("true")


def _barras(df, y, x, texto, altura, titulo_x, cor=COR, cor_campo=None, tooltip=None):
    enc_cor = alt.Color(f"{cor_campo}:N", scale=alt.Scale(domain=["SC", "Outros"], range=[COR_SC, COR_NEUTRA]),
                        legend=None) if cor_campo else alt.value(cor)
    base = alt.Chart(df).encode(
        y=alt.Y(f"{y}:N", sort=alt.EncodingSortField(x, order="descending"), title=None, axis=alt.Axis(labelLimit=330)),
        x=alt.X(f"{x}:Q", title=titulo_x), tooltip=tooltip or [y, texto])
    st.altair_chart((base.mark_bar().encode(color=enc_cor) + base.mark_text(align="left", dx=4).encode(text=f"{texto}:N"))
                    .properties(height=altura), use_container_width=True)


def _card(r) -> str:
    linhas = [f'📍 {r["municipio"] or "—"}/{r["uf"]} · {r["convenente"]}',
              f'🏛️ {r["orgao_superior"]} · {r["instrumento"] or "instrumento não informado"} · {r["situacao"].title()}',
              f'💵 Valor {rp.fmt_brl(r["valor"])} · liberado {rp.fmt_brl(r["liberado"])} ({_pct(float(r["pct_liberado"] or 0))})'
              f' · vigência {r["inicio"]} a {r["fim"]}']
    return ui.card_generico(
        r["score"], (r["objeto"] or "(sem descrição)")[:140], "", "Obra" if str(r["obra"]) == "True" else "Programa",
        f'nº {r["numero"]}' if r.get("numero") else "", linhas, "", [c for c in str(r["pontos"]).split(" | ") if c])


def render() -> None:
    st.subheader("Obras e programas")
    st.caption("Repasses da União a estados, municípios e entidades (convênios, contratos de repasse e similares) · "
               "Fonte: Portal da Transparência (CGU). **Santa Catarina em destaque.**")
    r = _dados()
    if r is None:
        st.info("Ainda não há dados de obras e programas. Eles são gerados toda semana pelo GitHub Actions "
                "(workflow “Atualizar obras e programas”).")
        return
    dados, meta = r
    uf_df, sc_df, mun_df, al_df = (dados[k] for k in rp.RECURSOS)
    uf_df = uf_df.assign(**{c: _num(uf_df[c]) for c in uf_df.columns if c not in ("uf", "completa")})
    uf_df["completa"] = _bool(uf_df["completa"])
    st.caption(f"📅 Gerado em {meta.get('gerado_em', '—')} · {meta.get('janela', '')} · "
               f"{int(uf_df['completa'].sum())} de {len(uf_df)} UFs completas no comparativo")

    sc = uf_df[uf_df["uf"] == rp.UF_DESTAQUE]
    if sc.empty:
        st.warning("Santa Catarina não está nos dados desta geração.")
        return
    s = sc.iloc[0]
    completas = uf_df[uf_df["completa"]]
    rank = int((completas["pct_vencidos"] > s["pct_vencidos"]).sum()) + 1 if len(completas) > 1 else 0

    t_sc, t_br, t_alerta = st.tabs(["📍 Santa Catarina", "🇧🇷 Brasil por UF", "🚩 Alertas em todo o país"])

    # ------------------------------------------------------------------ SC
    with t_sc:
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            ui.kpi("📋 Instrumentos em andamento", ui.fmt_num(s["ativos"]), COR_SC,
                   f'{ui.fmt_num(s["obras_ativas"])} deles são obras')
        with k2:
            ui.kpi("💰 Valor da carteira", _brl(s["valor_ativo"]), "#2e7d32",
                   f'{_pct(s["pct_liberado_ativo"])} já liberado')
        with k3:
            ui.kpi("⏰ Vigência vencida", ui.fmt_num(s["vencidos"]), ui.COR_MEDIA,
                   f'{_pct(s["pct_vencidos"])} da carteira · {_brl(s["valor_vencido"])}')
        with k4:
            ui.kpi("🚩 Atenção alta", ui.fmt_num(s["alertas_altos"]), ui.COR_ALTA,
                   (f"SC é o {rank}º de {len(completas)} UFs em % de vencidos" if rank else "sem comparativo"))

        sc_d = sc_df.copy()
        for c in ("valor", "liberado", "dias_vencido", "pct_liberado", "pct_prazo", "score"):
            sc_d[c] = _num(sc_d[c])
        sc_d["nivel"] = sc_d["score"].astype(int).map(dou.nivel_relevancia)
        sc_d["obra_b"] = _bool(sc_d["obra"])

        st.markdown("#### 🏘️ Municípios de SC com mais recursos federais em andamento")
        m = mun_df.assign(**{c: _num(mun_df[c]) for c in ("ativos", "valor", "liberado", "obras", "vencidos", "valor_vencido", "pct_liberado")})
        ordem = st.radio("Ordenar por", ["Valor da carteira", "Valor com vigência vencida", "Instrumentos vencidos"],
                         horizontal=True, key="rp_mord")
        col = {"Valor da carteira": "valor", "Valor com vigência vencida": "valor_vencido", "Instrumentos vencidos": "vencidos"}[ordem]
        top = m.nlargest(15, col).copy()
        top["Valor"] = (top[col].map(_brl) if col != "vencidos" else top[col].map(ui.fmt_num)) + "  ·  " + \
            top["ativos"].map(ui.fmt_num) + " instrumentos"
        top["v"] = top[col] / (1e6 if col != "vencidos" else 1)
        _barras(top, "municipio", "v", "Valor", 32 * len(top) + 40, "R$ milhões" if col != "vencidos" else "instrumentos",
                cor=COR_SC, tooltip=["municipio", "Valor"])

        st.markdown("#### 🚩 Instrumentos de SC com pontos de atenção")
        f1, f2 = st.columns(2)
        with f1:
            st.caption("Relevância")
            niveis = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="rp_f_nivel")
        mun_opts = ["Todos"] + sorted(sc_d["municipio"].replace("", "(sem município)").unique())
        mun_sel = f2.selectbox("Município", mun_opts, key="rp_f_mun")
        c1, c2 = st.columns(2)
        so_obras = c1.checkbox("Só obras", key="rp_obras")
        so_pontos = c2.checkbox("Só com pontos de atenção", value=True, key="rp_pontos")
        d = sc_d[sc_d["nivel"].isin(niveis)]
        if mun_sel != "Todos":
            d = d[d["municipio"].replace("", "(sem município)") == mun_sel]
        if so_obras:
            d = d[d["obra_b"]]
        if so_pontos:
            d = d[d["score"] > 0]
        cA, cB = st.columns([4, 2])
        cA.caption(f"{ui.fmt_num(len(d))} instrumentos nos filtros")
        with cB:
            ui.botoes_download(d.drop(columns=["nivel", "obra_b"]), "fiscalizagov_repasses_sc", "rp_dl1")
        if d.empty:
            st.info("Nenhum instrumento com esses filtros.")
        else:
            ui.render_cards(d, 20, key=f"rp_cards_{mun_sel}_{so_obras}_{so_pontos}_{'|'.join(niveis)}", card=_card)

    # ------------------------------------------------------------------ Brasil
    with t_br:
        st.caption("Compara as UFs pela carteira de instrumentos em andamento. Só entram as UFs cujos dados foram lidos "
                   "por inteiro; Santa Catarina aparece em vermelho.")
        b = completas.copy()
        if b.empty:
            st.info("Sem UFs completas para comparar.")
        else:
            met = st.radio("Medir por", ["Valor da carteira", "% de instrumentos com vigência vencida", "% já liberado do valor",
                                         "Valor com vigência vencida"], horizontal=True, key="rp_bmet")
            col = {"Valor da carteira": "valor_ativo", "% de instrumentos com vigência vencida": "pct_vencidos",
                   "% já liberado do valor": "pct_liberado_ativo", "Valor com vigência vencida": "valor_vencido"}[met]
            dinheiro = col in ("valor_ativo", "valor_vencido")
            t = b.nlargest(27, col).copy()
            t["Destaque"] = t["uf"].map(lambda u: "SC" if u == rp.UF_DESTAQUE else "Outros")
            t["Valor"] = (t[col].map(_brl) if dinheiro else t[col].map(_pct)) + "  ·  " + t["ativos"].map(ui.fmt_num) + " instrumentos"
            t["v"] = t[col] / (1e6 if dinheiro else 1)
            _barras(t, "uf", "v", "Valor", 26 * len(t) + 40, "R$ milhões" if dinheiro else "%", cor_campo="Destaque",
                    tooltip=["uf", "Valor"])
            vis = b.rename(columns={"uf": "UF", "ativos": "Em andamento", "valor_ativo": "Valor em andamento",
                                    "pct_liberado_ativo": "% liberado", "vencidos": "Vigência vencida",
                                    "pct_vencidos": "% vencidos", "valor_vencido": "Valor vencido",
                                    "obras_ativas": "Obras", "alertas_altos": "Atenção alta"})
            cols = ["UF", "Em andamento", "Valor em andamento", "% liberado", "Vigência vencida", "% vencidos",
                    "Valor vencido", "Obras", "Atenção alta"]
            tab = vis[cols].sort_values("Valor em andamento", ascending=False)
            show = tab.copy()
            for c in ("Valor em andamento", "Valor vencido"):
                show[c] = show[c].map(_brl)
            for c in ("% liberado", "% vencidos"):
                show[c] = show[c].map(_pct)
            for c in ("Em andamento", "Vigência vencida", "Obras", "Atenção alta"):
                show[c] = show[c].map(ui.fmt_num)
            st.dataframe(show, use_container_width=True, hide_index=True)
            ui.botoes_download(tab, "fiscalizagov_repasses_uf", "rp_dl2")

    # ------------------------------------------------------------------ alertas
    with t_alerta:
        a = al_df.copy()
        for c in ("valor", "liberado", "dias_vencido", "pct_liberado", "pct_prazo", "score"):
            a[c] = _num(a[c])
        a["nivel"] = a["score"].astype(int).map(dou.nivel_relevancia)
        st.caption(f"As {ui.fmt_num(len(a))} maiores pontuações do país (entre as UFs lidas).")
        g1, g2 = st.columns(2)
        ufs = ["Todas"] + sorted(a["uf"].unique())
        uf_sel = g1.selectbox("UF", ufs, index=ufs.index(rp.UF_DESTAQUE) if rp.UF_DESTAQUE in ufs else 0, key="rp_auf")
        with g2:
            st.caption("Relevância")
            niv = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="rp_a_nivel")
        so_ob = st.checkbox("Só obras", key="rp_a_obras")
        a = a[a["nivel"].isin(niv)]
        if uf_sel != "Todas":
            a = a[a["uf"] == uf_sel]
        if so_ob:
            a = a[_bool(a["obra"])]
        cA, cB = st.columns([4, 2])
        cA.caption(f"{ui.fmt_num(len(a))} instrumentos nos filtros")
        with cB:
            ui.botoes_download(a.drop(columns=["nivel"]), "fiscalizagov_repasses_alertas", "rp_dl3")
        if a.empty:
            st.info("Nenhum instrumento com esses filtros.")
        else:
            ui.render_cards(a, 20, key=f"rp_acards_{uf_sel}_{so_ob}_{'|'.join(niv)}", card=_card)

    with st.expander("📖 Como ler esta aba (e o que ela não faz)"):
        st.markdown(
            "- **O que são os dados:** convênios, contratos de repasse (operados pela Caixa), termos de compromisso e "
            "transferências legais pelos quais a União manda dinheiro a estados, municípios e entidades. **Obra** = contrato "
            "de repasse ou objeto de construção, reforma, pavimentação, saneamento e semelhantes (leitura do texto do objeto).\n"
            "- **Recorte:** instrumentos cuja vigência termina a partir de 1º de janeiro do ano anterior. A carteira "
            "'em andamento' exclui os concluídos, arquivados, anulados e os que já estão em prestação de contas.\n"
            "- **Pontos de atenção:** vigência encerrada sem conclusão (há mais de um ano pesa mais); pouco dinheiro liberado com "
            "70% ou mais do prazo decorrido; nenhuma liberação com metade do prazo; mais de 12 meses sem liberação; "
            "convenente inadimplente; prestação de contas rejeitada ou atrasada; valor alto (≥ R$ 10 mi).\n"
            "- **Vigência vencida não é obra parada.** A vigência pode ter sido prorrogada fora do que a fonte mostra, e o "
            "sistema mostra dinheiro e prazo, **não o avanço físico da obra**. Esse dado vem do Obrasgov.br, cuja API antiga "
            "parou de ser atualizada em julho de 2026 e cuja versão nova ainda não tem documentação pública.\n"
            "- Esta aba diz **onde olhar primeiro**; não aponta irregularidade."
        )
