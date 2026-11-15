# tab_orcamento.py - visões "Em quê gasta" (tipo de gasto) e "Quem usa menos o orçamento" (execução)
# Dados: arquivo oficial Orçamento da Despesa (CGU). Ver orcamento.py.
import datetime as dt

import altair as alt
import pandas as pd
import streamlit as st

import dou
import gastos
import orcamento
import ui
from dou import ErroColeta

COR = "#1a6ef5"
COR_ALERTA = "#d32f2f"
VISAO_TIPO = "Em quê gasta (tipo de gasto)"
VISAO_EXEC = "Quem menos usa o orçamento"


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _orcamento(ano: int) -> pd.DataFrame:
    # Erro não é guardado no cache: falha nunca fica presa por 6 horas.
    return orcamento.coletar(ano)


def _carregar(ano: int):
    try:
        with st.spinner(f"Baixando o orçamento oficial de {ano} (arquivo de ~1 MB da CGU)…"):
            return _orcamento(ano)
    except ErroColeta as e:
        st.error(f"⚠️ Não consegui baixar o orçamento de {ano} ({e}). A ausência de dados **não** significa ausência de "
                 "gasto. Tente de novo em instantes.")
        if st.button("🔄 Tentar de novo", key="oc_retry"):
            st.cache_data.clear()
            st.rerun()
        return None


def _brl(v) -> str:
    return gastos.fmt_brl_curto(v)


def _pct(v) -> str:
    return f"{v:.1f}%".replace(".", ",")


def _barras(df: pd.DataFrame, y: str, x: str, texto: str, altura: int, titulo_x: str, cor=COR, regra: float = None,
            dominio=None, tooltip=None) -> None:
    base = alt.Chart(df).encode(
        y=alt.Y(f"{y}:N", sort=alt.EncodingSortField(x, order="descending"), title=None, axis=alt.Axis(labelLimit=380)),
        x=alt.X(f"{x}:Q", title=titulo_x, scale=alt.Scale(domain=dominio) if dominio else alt.Undefined),
        tooltip=tooltip or [y, texto])
    camadas = base.mark_bar(color=cor) + base.mark_text(align="left", dx=4).encode(text=f"{texto}:N")
    if regra is not None:
        camadas = camadas + alt.Chart(pd.DataFrame({"r": [regra]})).mark_rule(color="#b7791f", strokeDash=[5, 4], size=2).encode(x="r:Q")
    st.altair_chart(camadas.properties(height=altura), use_container_width=True)


# ------------------------------------------------------------
# Em quê gasta
# ------------------------------------------------------------

def _tipo(df: pd.DataFrame, ano: int, incluir: bool) -> None:
    st.markdown("#### 🧩 Em quê o governo gasta")
    st.caption("Classificação oficial do orçamento (grupo e elemento de despesa), reagrupada em tipos que se entendem "
               "sem ser contador: pessoal, benefícios, transferências, investimentos…")
    c1, c2 = st.columns([3, 2])
    modo = c1.radio("Classificação", ["Tipo de gasto (visão simplificada)", "Grupo de despesa (oficial)"], horizontal=True,
                    key="oc_modo")
    metrica = c2.radio("Medir por", orcamento.METRICAS, horizontal=True, key="oc_met",
                       help="Empenhado = compromisso assumido · Liquidado = entrega reconhecida · "
                            "Dotação = orçamento autorizado no ano.")
    oficial = modo.startswith("Grupo")
    sups = df.groupby("Órgão superior")["Dotação atualizada"].sum().sort_values(ascending=False).index.tolist()
    escolhido = st.selectbox("Recorte", ["Todo o governo federal"] + sups, key="oc_sup")
    base = df if escolhido.startswith("Todo") else df[df["Órgão superior"] == escolhido]
    d = orcamento.sem_divida(base, incluir)
    t = orcamento.por_tipo(d, metrica, oficial)
    if t.empty or t[metrica].sum() <= 0:
        st.info("Sem dados para este recorte.")
        return
    rotulo = "Grupo de despesa" if oficial else "Tipo de gasto"
    t["Valor"] = t[metrica].map(_brl) + "  ·  " + t["% do total"].map(_pct)
    t["bi"] = t[metrica] / 1e9

    total = float(t[metrica].sum())
    pessoal = float(t[t[rotulo].isin([orcamento.T_PESSOAL, orcamento.T_APOSENT_SERV, "Pessoal e Encargos Sociais"])][metrica].sum())
    benef = float(t[t[rotulo].isin([orcamento.T_INSS, orcamento.T_SOCIAL])][metrica].sum())
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi(f"💰 Total {metrica.lower()}", _brl(total), "#2e7d32", f"{ano} · {escolhido[:34]}")
    with k2:
        ui.kpi("🥇 Maior tipo", t.iloc[0][rotulo][:40], ui.COR_ALERTA, f'{_pct(t.iloc[0]["% do total"])} do total')
    with k3:
        ui.kpi("👥 Pessoal (ativos e inativos)", _pct(pessoal / total * 100), ui.COR_MEDIA, _brl(pessoal))
    with k4:
        ui.kpi("🤝 Benefícios (INSS e sociais)", _pct(benef / total * 100), ui.COR_ALERTA, _brl(benef))

    _barras(t, rotulo, "bi", "Valor", 46 * len(t) + 40, "R$ bilhões", tooltip=[rotulo, "Valor"])

    st.markdown("**Detalhar um tipo**")
    tipo = st.selectbox("Tipo", t[rotulo].tolist(), key="oc_tipo")
    det = orcamento.detalhar(d, tipo, metrica, oficial)
    if not det.empty:
        det["Valor"] = det[metrica].map(_brl) + "  ·  " + det["% do tipo"].map(_pct)
        det["bi"] = det[metrica] / 1e9
        _barras(det, "Elemento", "bi", "Valor", 34 * len(det) + 40, "R$ bilhões", cor="#b7791f", tooltip=["Elemento", "Valor"])

    st.markdown("#### 🏥 Por área de atuação (função)")
    f = orcamento.por_funcao(d, metrica).head(12)
    f["Valor"] = f[metrica].map(_brl) + "  ·  " + f["% do total"].map(_pct)
    f["bi"] = f[metrica] / 1e9
    _barras(f, "Função", "bi", "Valor", 34 * len(f) + 40, "R$ bilhões", cor="#2e7d32", tooltip=["Função", "Valor"])

    with st.expander("📥 Baixar tabela"):
        ui.botoes_download(t[[rotulo, metrica, "% do total"]], f"fiscalizagov_tipo_gasto_{ano}", "oc_dl1")
    with st.expander("📖 Como os tipos foram montados"):
        st.markdown(
            "- Base: **grupo de despesa** (Pessoal, Juros, Outras despesas correntes, Investimentos, Inversões financeiras, "
            "Amortização) e **elemento de despesa**, da classificação oficial do orçamento.\n"
            "- *Aposentadorias e pensões de servidores* = elementos 01, 03 e 05 do grupo Pessoal. *Benefícios do INSS* = "
            "aposentadorias, pensões e outros benefícios do RGPS (elementos 53 a 58). *Benefícios sociais* = BPC, seguro-"
            "desemprego e abono, auxílios a pessoas físicas, auxílio a estudantes e pensões especiais (06, 10, 18, 48, 59).\n"
            "- *Transferências, contribuições e subvenções* inclui a distribuição constitucional de receitas a estados e "
            "municípios (FPE e FPM) e os repasses fundo a fundo, como os da saúde.\n"
            "- Juros e amortização/refinanciamento da dívida ficam fora por padrão (marque a opção no topo para incluir). "
            "A reserva de contingência nunca entra: é dotação sem executor.\n"
            "- É um agrupamento didático nosso sobre a classificação oficial: a coluna **Grupo de despesa (oficial)** mostra "
            "a versão sem reagrupar."
        )


# ------------------------------------------------------------
# Quem menos usa o orçamento
# ------------------------------------------------------------

def _execucao(df: pd.DataFrame, ano: int, incluir: bool) -> None:
    st.markdown("#### 🐢 Quem menos usa o orçamento que tem")
    st.caption("Mede quanto da **dotação atualizada** (orçamento autorizado) cada órgão já **empenhou** (comprometeu). "
               "Quanto menor, mais dinheiro parado.")
    c1, c2 = st.columns(2)
    nivel = c1.radio("Comparar", ["Órgão ou entidade", "Órgão superior (ministério)"], horizontal=True, key="oc_nivel")
    ordem = c2.radio("Ordenar por", ["Menor % empenhado", "Maior valor não empenhado"], horizontal=True, key="oc_ordem")
    escopo = st.selectbox("Tipo de despesa considerado", list(orcamento.ESCOPOS), key="oc_escopo",
                          help="Investimentos é a medida clássica de capacidade de execução; folha de pagamento e "
                               "benefícios executam quase 100% por natureza.")
    dmin = st.number_input("Dotação mínima (R$ milhões)", min_value=0.0, value=100.0, step=50.0, key="oc_dmin",
                           help="Esconde órgãos pequenos, onde um valor baixo pesa pouco.")

    r = orcamento.execucao(df, "Órgão" if nivel.startswith("Órgão ou") else "Órgão superior", escopo, dmin * 1e6, incluir)
    if r.empty:
        st.info("Nenhum órgão com esses filtros.")
        return
    if ordem.startswith("Maior"):
        r = r.sort_values("Não empenhado (R$)", ascending=False).reset_index(drop=True)

    ref = orcamento.fracao_ano(ano, dou.hoje_brasilia())
    dot, emp = float(r["Dotação atualizada"].sum()), float(r["Empenhado"].sum())
    nao = float(r["Não empenhado (R$)"].sum())
    abaixo = int((r["% empenhado"] < ref - 10).sum())
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        ui.kpi("📋 Dotação atualizada", _brl(dot), ui.COR_ALERTA, f"{ui.fmt_num(len(r))} órgãos no recorte")
    with k2:
        ui.kpi("✅ Empenhado", _pct(emp / dot * 100 if dot else 0), "#2e7d32", f"{_brl(emp)} · ano {_pct(ref)} decorrido")
    with k3:
        ui.kpi("💤 Não empenhado", _brl(nao), ui.COR_MEDIA, "orçamento autorizado ainda parado")
    with k4:
        ui.kpi("🚩 Bem abaixo do ritmo", ui.fmt_num(abaixo), ui.COR_ALERTA, "mais de 10 pontos abaixo do ano decorrido")

    rotulo = "Órgão" if nivel.startswith("Órgão ou") else "Órgão superior"
    top = r.head(20).copy()
    top["Nome"] = top[rotulo] + (" (" + top["Órgão superior"].str.slice(0, 28) + ")" if rotulo == "Órgão" else "")
    top["Valor"] = top["% empenhado"].map(_pct) + "  ·  parado " + top["Não empenhado (R$)"].map(_brl)
    st.markdown("**Os 20 que menos empenharam**" if ordem.startswith("Menor") else "**Os 20 com mais dinheiro parado**")
    if ordem.startswith("Menor"):
        _barras(top, "Nome", "% empenhado", "Valor", 34 * len(top) + 50, "% empenhado da dotação", cor=COR_ALERTA,
                regra=ref, dominio=[0, 100], tooltip=["Nome", "Valor"])
        st.caption(f"Linha tracejada = {_pct(ref)} do ano já decorrido: referência de quanto seria 'normal' empenhar.")
    else:
        top["bi"] = top["Não empenhado (R$)"] / 1e9
        _barras(top, "Nome", "bi", "Valor", 34 * len(top) + 50, "R$ bilhões não empenhados", cor=COR_ALERTA,
                tooltip=["Nome", "Valor"])

    tab = r.copy()
    for c in ("Dotação atualizada", "Empenhado", "Liquidado", "Não empenhado (R$)"):
        tab[c] = tab[c].map(_brl)
    for c in ("% empenhado", "% liquidado"):
        tab[c] = tab[c].map(_pct)
    st.dataframe(tab, use_container_width=True, hide_index=True)
    ui.botoes_download(r, f"fiscalizagov_execucao_{ano}", "oc_dl2")

    with st.expander("⚠️ Como ler este ranking (importante)"):
        st.markdown(
            "- **Baixa execução não prova má gestão.** Pode vir de **bloqueio ou contingenciamento** por decreto, de "
            "dependência de convênios e transferências, de licitações em andamento ou de projetos plurianuais.\n"
            "- **Fundos de crédito** (como FDCO, FDA e FDNE) e fundos que dependem de demanda do mercado têm lógica "
            "própria: dinheiro parado ali costuma ser demanda, não gestão.\n"
            "- **O ano em curso é parcial**, e boa parte do empenho acontece no fim do ano. Compare sempre com a linha do "
            "ano decorrido; no começo do ano quase todos aparecem 'mal'.\n"
            "- **Dotação atualizada** já inclui créditos adicionais e cortes feitos ao longo do ano.\n"
            "- Reserva de contingência, juros e amortização da dívida ficam fora (não têm executor).\n"
            "- Use o ranking para **perguntar o motivo**, não para concluir. Confira nos relatórios de avaliação "
            "bimestral e nas decisões de bloqueio."
        )


def render(visao: str, ano: int, incluir: bool) -> None:
    df = _carregar(ano)
    if df is None:
        return
    if visao == VISAO_TIPO:
        _tipo(df, ano, incluir)
    else:
        _execucao(df, ano, incluir)
    st.caption("Fonte: Orçamento da Despesa, CGU (Portal da Transparência). Empenhado e liquidado conferem com a execução "
               "por órgão da outra visão.")
