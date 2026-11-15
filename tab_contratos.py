# tab_contratos.py - aba "Licitações e contratos" (PNCP, Executivo federal)
import datetime as dt
import os

import pandas as pd
import streamlit as st

import dou
import pncp
import sancoes
import segredos
import ui


class ColetaFalhou(Exception):
    def __init__(self, erros, parcial):
        super().__init__("; ".join(erros))
        self.erros = erros
        self.parcial = parcial


# Cache longo: a coleta de um dia leva de 1 a 2 minutos (limite de requisições do PNCP).
# Exceção não é guardada pelo cache, então falha nunca fica "presa".
@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _contratos_cache(data: dt.date) -> pd.DataFrame:
    df, erros = pncp.coletar_contratos(data)
    if erros:
        raise ColetaFalhou(erros, df)
    return pncp.avaliar_contratos(df)


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _dispensas_cache(data: dt.date) -> pd.DataFrame:
    df, erros = pncp.coletar_dispensas(data)
    if erros:
        raise ColetaFalhou(erros, df)
    return pncp.avaliar_dispensas(df)


def _carregar(fn, avaliar, data):
    try:
        return fn(data), []
    except ColetaFalhou as ex:
        return avaliar(ex.parcial), ex.erros


def card_contrato(r) -> str:
    return ui.card_generico(
        r["Score"], f'{r["Fornecedor"] or "(fornecedor não informado)"} · {pncp.fmt_brl(r["Valor"])}', r["Link"],
        r["Tipo"], f'{r["Data"]} · {r["Categoria"]}',
        [f'🏛️ {r["Órgão"]}' + (f' › {r["Unidade"]}' if r["Unidade"] and r["Unidade"] != r["Órgão"] else "")
         + (f' · {r["UF"]}' if r["UF"] else ""),
         f'🧾 Documento: {r["Documento"]} · vigência {r["Início vigência"]} a {r["Fim vigência"]}'],
        r["Objeto"], [c for c in r["Pontos de atenção"].split(" | ") if c])


def card_dispensa(r) -> str:
    return ui.card_generico(
        r["Score"], f'{pncp.fmt_brl(r["Valor estimado"])} · {r["Órgão"]}', r["Link"],
        r["Modalidade"], f'{r["Data"]} · {r["Situação"]}',
        [f'🏛️ {r["Unidade"]}' + (f' · {r["UF"]}' if r["UF"] else ""), f'⚖️ {r["Amparo legal"]}'],
        r["Objeto"], [c for c in r["Pontos de atenção"].split(" | ") if c])


def _chave_api() -> str:
    return segredos.chave_transparencia(st.secrets)


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _sancoes_cnpj(cnpj: str, _chave: str) -> list:
    # `_chave` fica fora da chave de cache. Erro não é guardado: falha nunca vira "sem sanção".
    return sancoes.consultar_cnpj(cnpj, _chave)


def _bloco_sancoes(df: pd.DataFrame, chave: str) -> pd.DataFrame:
    """Cruza os fornecedores com CEIS/CNEP sob demanda e soma os pontos de atenção."""
    k_mapa, k_err = f"sc_{chave}", f"sc_err_{chave}"
    mapa = st.session_state.get(k_mapa, {})
    with st.expander("🚫 Cruzar fornecedores com CEIS/CNEP (empresas sancionadas)", expanded=bool(mapa)):
        api = _chave_api()
        if not api:
            st.warning("Falta o secret `TRANSPARENCIA_API_KEY` (chave gratuita do Portal da Transparência).")
        else:
            st.caption("Consulta o cadastro de empresas punidas (CGU). Vale como **ponto de atenção**: o alcance da "
                       "sanção varia (impedimento vale só para o ente sancionador; inidoneidade vale para toda a "
                       "Administração). Confira o tipo e o alcance mostrados no card.")
            n = st.slider("Fornecedores a verificar (os de maior atenção e valor primeiro)", 10, 150, 50, 10, key="sc_n")
            cand = df[df["Documento"].str.len() == 14]["Documento"].drop_duplicates().head(n).tolist()
            st.caption(f"{len(cand)} CNPJs · cerca de {max(1, round(len(cand) * 2 / 60))} min (limite da API do Portal).")
            if st.button("🚫 Verificar sanções", key=f"sc_btn_{chave}", disabled=not cand):
                barra = st.progress(0.0, text="Consultando CEIS e CNEP…")
                novo, erros = sancoes.verificar(
                    cand, api, progresso=lambda i, t: barra.progress(i / t, text=f"Consultando CEIS e CNEP… {i}/{t}"),
                    consultar=lambda c: _sancoes_cnpj(c, api))
                st.session_state[k_mapa] = {**mapa, **novo}
                st.session_state[k_err] = erros
                st.rerun()
        for er in st.session_state.get(k_err, [])[:5]:
            st.error(f"⚠️ Consulta incompleta: {er}")
        if mapa:
            vig = sum(1 for s in mapa.values() if s)
            st.success(f"{len(mapa)} fornecedores verificados · {vig} com algum registro no CEIS/CNEP.")
    return sancoes.aplicar_sancoes(df, mapa) if mapa else df


VISAO_CONTRATOS = "Contratos e aditivos"
VISAO_DISPENSAS = "Dispensas e inexigibilidades"
VISAO_EVIDENCIAS = "🚨 Alertas e evidências"


def _evidencias(data: dt.date) -> None:
    """Dossiê do dia: cruza contratos, dispensas/inexigibilidades e sanções por fornecedor e por órgão."""
    chave = f"pn_ok_e_{data.isoformat()}"
    if not st.session_state.get(chave):
        st.info("Cruza, para o dia escolhido, os **contratos** e as **dispensas e inexigibilidades**: quais fornecedores e "
                "órgãos concentram mais pontos de atenção. Baixa os dois conjuntos do PNCP e leva **2 a 4 minutos**; "
                "depois fica guardado por 6 horas. Se você já cruzou fornecedores com o CEIS/CNEP na visão de contratos, "
                "esse resultado entra aqui.")
        if not st.button("🔎 Montar o painel de evidências", key=f"pn_btn_{chave}", type="primary"):
            return
        st.session_state[chave] = True

    with st.spinner("Coletando contratos e dispensas no PNCP (pode levar alguns minutos)…"):
        dfc, erc = _carregar(_contratos_cache, pncp.avaliar_contratos, data)
        dfd, erd = _carregar(_dispensas_cache, pncp.avaliar_dispensas, data)
    erros = erc + erd
    if erros:
        st.error("⚠️ **Falha parcial na coleta do PNCP.** O painel pode estar incompleto, e a ausência de um item "
                 "**não** significa ausência de registros. Tente de novo em instantes.\n\n" + "\n".join(f"- {x}" for x in erros[:6]))
        if st.button("🔄 Tentar de novo", key=f"pn_retry_{chave}"):
            st.cache_data.clear()
            st.rerun()
    if dfc.empty and dfd.empty:
        if not erros:
            st.info("Nenhum registro federal neste dia (fim de semana, feriado ou ainda não publicado).")
        return

    dfc = dfc[dfc["Tipo"] != "Empenho"].copy()
    mapa = st.session_state.get(f"sc_pn_ok_c_{data.isoformat()}", {})
    if mapa and not dfc.empty:
        dfc = sancoes.aplicar_sancoes(dfc, mapa)
    dfc["Nível"] = dfc["Score"].map(dou.nivel_relevancia)
    dfd = dfd.copy()
    dfd["Nível"] = dfd["Score"].map(dou.nivel_relevancia)
    atencao_c = dfc[dfc["Score"] >= dou.NIVEL_MEDIO]
    atencao_d = dfd[dfd["Score"] >= dou.NIVEL_MEDIO]
    sancionados = dfc[dfc["Pontos de atenção"].str.contains("sanção VIGENTE", na=False)]

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("📄 Contratos com atenção", ui.fmt_num(len(atencao_c)), ui.COR_ALERTA, f"de {ui.fmt_num(len(dfc))} contratos e aditivos")
    with k2:
        ui.kpi("⚖️ Dispensas com atenção", ui.fmt_num(len(atencao_d)), ui.COR_MEDIA, f"de {ui.fmt_num(len(dfd))} dispensas e inexigibilidades")
    with k3:
        v = float(atencao_c["Valor"].fillna(0).sum() + atencao_d["Valor estimado"].fillna(0).sum())
        ui.kpi("💰 Valor sob atenção", pncp.fmt_brl(v), "#2e7d32", "média ou alta relevância")
    with k4:
        ui.kpi("🚫 Com sanção vigente", ui.fmt_num(len(sancionados)) if mapa else "—", ui.COR_ALTA,
               "CEIS/CNEP verificado" if mapa else "faça o cruzamento na visão de contratos")

    st.markdown("#### 🏢 Fornecedores com mais evidências")
    if dfc.empty:
        st.info("Sem contratos neste dia.")
    else:
        g = dfc.groupby(["Fornecedor", "Documento"], as_index=False).agg(
            Contratos=("Nº PNCP", "size"), Valor=("Valor", "sum"), Score_max=("Score", "max"),
            Atencao=("Score", lambda s: int((s >= dou.NIVEL_MEDIO).sum())), Orgaos=("Órgão", "nunique"))
        g["Sanção vigente"] = g["Documento"].isin(sancionados["Documento"])
        g = g[(g["Score_max"] > 0)].sort_values(["Sanção vigente", "Score_max", "Valor"], ascending=[False, False, False])
        vis = g.head(30).rename(columns={"Score_max": "Maior score", "Atencao": "Com atenção", "Orgaos": "Órgãos"}).copy()
        vis["Valor"] = vis["Valor"].map(pncp.fmt_brl)
        vis["Sanção vigente"] = vis["Sanção vigente"].map({True: "Sim", False: "—"})
        st.dataframe(vis, use_container_width=True, hide_index=True)
        opcoes = [f'{r.Fornecedor} ({r.Documento})' for r in g.head(30).itertuples()]
        if opcoes:
            sel = st.selectbox("Abrir as evidências de um fornecedor", opcoes, key="pn_ev_forn")
            doc = sel.rsplit("(", 1)[1].rstrip(")")
            ui.render_cards(dfc[dfc["Documento"] == doc].sort_values("Score", ascending=False), 10,
                            key=f"pn_ev_cards_{doc}_{data}", card=card_contrato)

    st.markdown("#### 🏛️ Órgãos com mais evidências (contratos e dispensas)")
    a = atencao_c.groupby("Órgão").agg(contratos=("Nº PNCP", "size"), valor_c=("Valor", "sum"))
    b = atencao_d.groupby("Órgão").agg(dispensas=("Nº PNCP", "size"), valor_d=("Valor estimado", "sum"))
    org = a.join(b, how="outer").fillna(0)
    if org.empty:
        st.info("Nenhum órgão com contratos ou dispensas de média ou alta relevância neste dia.")
    else:
        org["Valor sob atenção"] = org["valor_c"] + org["valor_d"]
        org["Itens com atenção"] = org["contratos"] + org["dispensas"]
        topo = org.sort_values("Valor sob atenção", ascending=False).head(15)
        st.bar_chart((topo["Valor sob atenção"] / 1e6).rename("R$ milhões sob atenção"), horizontal=True)
        tab = org.sort_values("Valor sob atenção", ascending=False).reset_index().rename(
            columns={"contratos": "Contratos", "dispensas": "Dispensas"})
        tab["Valor sob atenção"] = tab["Valor sob atenção"].map(pncp.fmt_brl)
        st.dataframe(tab[["Órgão", "Contratos", "Dispensas", "Itens com atenção", "Valor sob atenção"]],
                     use_container_width=True, hide_index=True)

    st.markdown("#### 🚩 Os registros mais graves do dia")
    graves = pd.concat([
        atencao_c.assign(_tipo="c", _score=atencao_c["Score"], _valor=atencao_c["Valor"].fillna(0)),
        atencao_d.assign(_tipo="d", _score=atencao_d["Score"], _valor=atencao_d["Valor estimado"].fillna(0)),
    ], ignore_index=True).sort_values(["_score", "_valor"], ascending=[False, False]).head(20)
    if graves.empty:
        st.info("Nada com média ou alta relevância neste dia.")
    else:
        for _, r in graves.iterrows():
            st.markdown(card_contrato(r) if r["_tipo"] == "c" else card_dispensa(r), unsafe_allow_html=True)

    with st.expander("📖 Como ler este painel"):
        st.markdown(
            "- **Evidência** = ponto de atenção explicável (valor alto, emergência, aditivo acima de 25%, fracionamento, "
            "inexigibilidade de alto valor, sanção vigente…). Nenhum prova irregularidade; indicam onde olhar primeiro.\n"
            "- **Fornecedores:** agrupados pelo CNPJ/CPF; a lista abre os contratos de cada um no PNCP.\n"
            "- **Órgãos:** soma contratos e dispensas/inexigibilidades com relevância média ou alta (35 pontos ou mais).\n"
            "- **Sanção vigente** só aparece depois do cruzamento com o CEIS/CNEP, na visão de contratos.\n"
            "- O painel cobre **um dia** de publicação. Cruzamentos com viagens, nomeações e execução do orçamento ficam "
            "nas abas próprias."
        )


def render() -> None:
    st.subheader("Licitações e contratos")
    st.caption("Executivo federal · Fonte: PNCP (Portal Nacional de Contratações Públicas). "
               "Os pontos de atenção indicam **onde olhar primeiro**; não são conclusão de irregularidade.")

    visao = ui.escolha_unica("Visão", [VISAO_CONTRATOS, VISAO_DISPENSAS, VISAO_EVIDENCIAS], VISAO_CONTRATOS, key="pn_visao")
    contratos = visao == VISAO_CONTRATOS

    hoje = dou.hoje_brasilia()
    data = st.date_input("📅 Data de publicação no PNCP", value=pncp.ultimo_dia_util(hoje), max_value=hoje,
                         format="DD/MM/YYYY", key="pn_data")
    if visao == VISAO_EVIDENCIAS:
        _evidencias(data)
        return
    chave = f"pn_ok_{'c' if contratos else 'd'}_{data.isoformat()}"

    if not st.session_state.get(chave):
        st.info("A coleta baixa o dia inteiro do PNCP (milhares de registros) e leva **1 a 2 minutos**. "
                "Depois fica guardada por 6 horas.")
        if not st.button("🔎 Carregar do PNCP", key=f"pn_btn_{chave}", type="primary"):
            return
        st.session_state[chave] = True

    with st.spinner("Coletando no PNCP (pode levar 1 a 2 minutos)…"):
        if contratos:
            df, erros = _carregar(_contratos_cache, pncp.avaliar_contratos, data)
        else:
            df, erros = _carregar(_dispensas_cache, pncp.avaliar_dispensas, data)

    if erros:
        st.error("⚠️ **Falha parcial na coleta do PNCP.** Os resultados podem estar incompletos, e a ausência "
                 "de itens **não** significa ausência de registros. Tente de novo em instantes.\n\n"
                 + "\n".join(f"- {x}" for x in erros[:6]))
        if st.button("🔄 Tentar de novo", key=f"pn_retry_{chave}"):
            st.cache_data.clear()
            st.rerun()
    if df.empty:
        if not erros:
            st.info("Nenhum registro federal neste dia (fim de semana, feriado ou ainda não publicado).")
        return

    col_valor = "Valor" if contratos else "Valor estimado"
    df = df.copy()

    if contratos:
        com_empenho = st.checkbox("Incluir empenhos", value=False, key="pn_empenho",
                                  help="Empenhos são a maioria dos registros (compras pequenas). Ficam ocultos por padrão.")
        df = df if com_empenho else df[df["Tipo"] != "Empenho"]
        df = _bloco_sancoes(df, chave)
    else:
        st.caption("Dispensa e inexigibilidade são contratações **sem licitação**. São legais nas hipóteses da lei; "
                   "o que vale conferir é o enquadramento e o valor.")

    df["Nível"] = df["Score"].map(dou.nivel_relevancia)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("📄 Registros", ui.fmt_num(len(df)), ui.COR_ALERTA, "Executivo federal")
    with k2:
        total = float(df[col_valor].fillna(0).sum())
        ui.kpi("💰 Valor somado", pncp.fmt_brl(total), "#2e7d32")
    with k3:
        ui.kpi("🚨 Atenção alta", ui.fmt_num(int((df["Nível"] == "Alta").sum())), ui.COR_ALTA,
               f"+ {ui.fmt_num(int((df['Nível'] == 'Média').sum()))} média")
    with k4:
        ui.kpi("🏛️ Órgãos", ui.fmt_num(df["Órgão"].nunique()), ui.COR_MEDIA)

    st.caption("Relevância")
    niveis = ui.escolha_multipla("Relevância", ["Alta", "Média", "Baixa"], key="pn_f_nivel")
    orgaos = st.multiselect("🏛️ Órgãos (vazio = todos)", sorted(df["Órgão"].unique()), key="pn_f_org")
    vmin = st.number_input("Valor mínimo (R$)", min_value=0.0, value=0.0, step=10_000.0, key="pn_vmin")

    d = df[df["Nível"].isin(niveis) & (df[col_valor].fillna(0) >= vmin)]
    if orgaos:
        d = d[d["Órgão"].isin(orgaos)]

    cA, cB = st.columns([4, 2])
    cA.caption(f"{ui.fmt_num(len(d))} registros nos filtros")
    with cB:
        ui.botoes_download(d.drop(columns=["Nível"]), f"fiscalizagov_pncp_{data:%Y%m%d}", "pn_dl")
    if d.empty:
        st.info("Nenhum registro com os filtros selecionados.")
        return
    ui.render_cards(d, 25, key=f"pn_cards_{chave}", card=card_contrato if contratos else card_dispensa)

    with st.expander("📖 Como as regras funcionam"):
        st.markdown(
            "- **Contratos:** valor alto (≥ R$ 1 mi) ou muito alto (≥ R$ 10 mi); menção a emergência ou calamidade; "
            "termo aditivo; acréscimo acima de 25% sobre o valor inicial; vários contratos do mesmo fornecedor "
            "com o mesmo órgão (recorrência ou possível fracionamento); vigência acima de 5 anos; valor simbólico "
            "(possível erro de cadastro).\n"
            "- **Dispensas e inexigibilidades:** valor estimado alto; enquadramento por emergência; inexigibilidade "
            "(sem competição); serviço técnico especializado de alto valor; homologado acima do estimado.\n"
            "- CPF de pessoa física aparece mascarado. Confirme sempre no PNCP pelo link (↗) antes de concluir."
        )
