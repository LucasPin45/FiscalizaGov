# apresentacao.py - aba "Apresentação": relógio do Governo Federal + um cartão com dado de cada módulo do painel
# Cada cartão usa só dados que já estão guardados ou em cache (resumos semanais, arquivos da CGU, cache do app). Quando uma
# fonte não está disponível, o cartão diz isso: a Apresentação nunca quebra por causa de um módulo.
from typing import Callable, List

import streamlit as st

import dou
import gastos
import orcamento
import pncp
import relogio
import repasses
import tab_cargos
import tab_contratos
import tab_gastos
import tab_orcamento
import tab_repasses
import tab_viagens
import ui

VERDE, AZUL, AMBAR, VERMELHO, ROXO = "#1b8a5a", "#1f5f99", "#b7791f", "#c0392b", "#6a4c93"


def _pct(v: float) -> str:
    return f"{v:.1f}%".replace(".", ",")


def _seguro(fn: Callable[[], str], icone: str, titulo: str, destino: str, cor: str) -> str:
    try:
        return fn()
    except Exception:  # noqa: BLE001 - um módulo fora do ar não pode derrubar a Apresentação
        return ui.modulo(icone, titulo, "", "", [], destino, cor, "Não consegui ler esta fonte agora. Tente “Atualizar tudo”.")


# ------------------------------------------------------------
# Um cartão por módulo
# ------------------------------------------------------------

def card_gastos(ano: int) -> str:
    snap = tab_gastos._snapshot(ano)
    if not snap:
        return ui.modulo("💸", "Gastos do governo", "", "", [], "Gastos do governo", VERDE,
                         "Resumo do ano ainda não gerado. Ele é atualizado toda madrugada.")
    df, quando = snap
    base = gastos.filtrar_finalisticos(df, False)
    sup = gastos.ranking_superiores(base, "Pago")
    total = float(base["Pago"].sum())
    top = sup.iloc[0]
    return ui.modulo("💸", "Gastos do governo", gastos.fmt_brl_curto(total), f"pagos em {ano}, sem o Tesouro e a dívida",
                     [f'Maior gastador: {top["Órgão superior"]} ({_pct(top["% do total"])})',
                      f"Os 5 maiores concentram {_pct(gastos.concentracao(sup, 5))}",
                      f"Atualizado em {quando}" if quando else ""], "Gastos do governo", VERDE)


def card_execucao(ano: int) -> str:
    df = tab_orcamento._orcamento(ano)
    r = orcamento.execucao(df, "Órgão superior", "Todas as despesas", 0.0, False)
    if r.empty:
        raise ValueError("sem dados")
    emp = float(r["Empenhado"].sum() / r["Dotação atualizada"].sum() * 100)
    ref = orcamento.fracao_ano(ano, dou.hoje_brasilia())
    pior = r.sort_values("% empenhado").iloc[0]
    parado = r.sort_values("Não empenhado (R$)", ascending=False).iloc[0]
    return ui.modulo("🐢", "Capacidade de gerir o orçamento", _pct(emp), f"da dotação já empenhada · {_pct(ref)} do ano decorrido",
                     [f'Menor execução: {pior["Órgão superior"]} ({_pct(pior["% empenhado"])})',
                      f'Mais dinheiro parado: {parado["Órgão superior"]} ({gastos.fmt_brl_curto(parado["Não empenhado (R$)"])})'],
                     "Gastos do governo → Quem menos usa o orçamento", AMBAR)


def card_licitacoes() -> str:
    dia = pncp.ultimo_dia_util(dou.hoje_brasilia())
    destino = "Licitações e contratos"
    if not st.session_state.get(f"pn_ok_c_{dia.isoformat()}"):
        return ui.modulo("🧾", "Licitações e contratos", "", "", [], destino, AZUL,
                         "O PNCP é lido sob demanda (1 a 2 minutos). Abra a aba e clique em “Carregar do PNCP”; "
                         "o resultado aparece aqui na próxima atualização.")
    df, erros = tab_contratos._carregar(tab_contratos._contratos_cache, pncp.avaliar_contratos, dia)
    df = df[df["Tipo"] != "Empenho"]
    if df.empty:
        raise ValueError("sem contratos")
    niveis = df["Score"].map(dou.nivel_relevancia)
    return ui.modulo("🧾", "Licitações e contratos", f"{ui.fmt_num(len(df))} contratos", f"publicados em {ui.fmt_data(dia)}",
                     [f"{ui.fmt_num(int((niveis == 'Alta').sum()))} de atenção alta, {ui.fmt_num(int((niveis == 'Média').sum()))} média",
                      f"Valor somado: {gastos.fmt_brl_curto(float(df['Valor'].fillna(0).sum()))}"], destino, AZUL)


def card_viagens(ano: int) -> str:
    r = tab_viagens._dados(ano)
    if r is None:
        return ui.modulo("✈️", "Viagens e diárias", "", "", [], "Viagens e diárias", ROXO, "Resumo semanal ainda não gerado.")
    dados, meta = r
    tot = tab_viagens._por_orgao(dados["orgao"], "Órgão superior")
    n, total = float(tot["viagens"].sum()), float(tot["total"].sum())
    top = tot.sort_values("total", ascending=False).iloc[0]
    return ui.modulo("✈️", "Viagens e diárias", gastos.fmt_brl_curto(total), f"em {ui.fmt_num(n)} viagens a serviço em {ano}",
                     [f"{_pct(tot['urgentes'].sum() / n * 100 if n else 0)} marcadas como urgentes",
                      f'Maior gasto: {top["sup"]} ({gastos.fmt_brl_curto(top["total"])})'], "Viagens e diárias", ROXO)


def card_obras() -> str:
    r = tab_repasses._dados()
    if r is None:
        return ui.modulo("🏗️", "Obras e programas", "", "", [], "Obras e programas", VERMELHO,
                         "Aguardando a primeira carga semanal dos repasses da União.")
    dados, meta = r
    uf = dados["uf"]
    sc = uf[uf["uf"] == repasses.UF_DESTAQUE]
    if sc.empty:
        raise ValueError("sem SC")
    s = sc.iloc[0]
    ativos, vencidos = float(s["ativos"]), float(s["vencidos"])
    return ui.modulo("🏗️", "Obras e programas · SC", gastos.fmt_brl_curto(float(s["valor_ativo"])),
                     f"em {ui.fmt_num(ativos)} repasses da União em andamento",
                     [f"{_pct(float(s['pct_liberado_ativo']))} do valor já liberado",
                      f"{ui.fmt_num(vencidos)} com vigência vencida ({_pct(vencidos / ativos * 100 if ativos else 0)})"],
                     "Obras e programas", VERMELHO)


def card_cargos() -> str:
    r = tab_cargos.resumo_apresentacao()
    if r is None:
        return ui.modulo("👔", "Cargos e nomeações", "", "", [], "Cargos e nomeações", AZUL,
                         "Não consegui ler os atos de pessoal agora.")
    return ui.modulo("👔", "Cargos e nomeações", f"{ui.fmt_num(r['movimentacoes'])} movimentações",
                     f"no Diário de {ui.fmt_data(r['data'])}",
                     [f"{r['nomeacoes']} nomeações e {r['exoneracoes']} exonerações",
                      f"{r['alta']} de alta direção · {r['presidencia']} na Presidência e em decretos"], "Cargos e nomeações", AZUL)


def card_dou(ctx: dict) -> str:
    return ui.modulo("📜", "Diário Oficial", f"{ui.fmt_num(ctx['atos'])} atos", f"lidos · {ctx['periodo']}",
                     [f"{ui.fmt_num(ctx['palavras'])} com as palavras-chave de busca",
                      f"{ui.fmt_num(ctx['altos'])} de relevância alta"], "DOU · Achados", AZUL)


def card_como_ler() -> str:
    return ui.modulo("🧭", "Como usar este painel", "Onde olhar primeiro", "pontos de atenção, não acusações",
                     ["Cada alerta mostra a regra que o gerou", "Confira sempre a fonte oficial (link ↗)",
                      "Vigência vencida, gasto alto ou viagem urgente podem ter explicação legal"],
                     "cada aba tem a sua explicação", VERDE)


# ------------------------------------------------------------
# Aba
# ------------------------------------------------------------

def cartoes(ano: int, ctx_dou: dict) -> List[str]:
    return [
        _seguro(lambda: card_gastos(ano), "💸", "Gastos do governo", "Gastos do governo", VERDE),
        _seguro(lambda: card_execucao(ano), "🐢", "Capacidade de gerir o orçamento", "Gastos do governo", AMBAR),
        _seguro(card_licitacoes, "🧾", "Licitações e contratos", "Licitações e contratos", AZUL),
        _seguro(lambda: card_viagens(ano), "✈️", "Viagens e diárias", "Viagens e diárias", ROXO),
        _seguro(card_obras, "🏗️", "Obras e programas", "Obras e programas", VERMELHO),
        _seguro(card_cargos, "👔", "Cargos e nomeações", "Cargos e nomeações", AZUL),
        _seguro(lambda: card_dou(ctx_dou), "📜", "Diário Oficial", "DOU · Achados", AZUL),
        card_como_ler(),
    ]


def render(ctx_dou: dict) -> None:
    relogio.render_apresentacao()
    ui.secao("📊 O painel em números")
    ano = dou.hoje_brasilia().year
    html_cards = cartoes(ano, ctx_dou)
    for i in range(0, len(html_cards), 4):
        cols = st.columns(4)
        for col, h in zip(cols, html_cards[i:i + 4]):
            col.markdown(h, unsafe_allow_html=True)
    st.caption("Cada cartão resume uma aba. As fontes, as regras e as ressalvas de cada módulo estão na própria aba.")
