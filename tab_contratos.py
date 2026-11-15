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


def render() -> None:
    st.subheader("Licitações e contratos")
    st.caption("Executivo federal · Fonte: PNCP (Portal Nacional de Contratações Públicas). "
               "Os pontos de atenção indicam **onde olhar primeiro**; não são conclusão de irregularidade.")

    visao = ui.escolha_unica("Visão", ["Contratos e aditivos", "Dispensas e inexigibilidades"],
                             "Contratos e aditivos", key="pn_visao")
    contratos = visao.startswith("Contratos")

    hoje = dou.hoje_brasilia()
    data = st.date_input("📅 Data de publicação no PNCP", value=pncp.ultimo_dia_util(hoje), max_value=hoje,
                         format="DD/MM/YYYY", key="pn_data")
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
