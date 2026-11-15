# fiscalizagov.py - FiscalizaGov (v0.2)
# ============================================================
# Sistema de fiscalizacao do Executivo (padrao Monitor Zanatta)
# - Coleta do DOU compartilhada com o notificador (dou.py)
# - Falha de coleta e sempre sinalizada, nunca vira "sem resultados"
# - Exportacao CSV/XLSX e notificacoes via Telegram
# ============================================================

import hmac
import io
import os
import html
import datetime as dt
from typing import List

import pandas as pd
import requests
import streamlit as st

import dou
from dou import DEFAULT_SECOES_DOU, SECOES_DISPONIVEIS, agora_brasilia_str, normalize_text

# ============================================================
# CONFIGURACAO DA PAGINA (OBRIGATORIAMENTE PRIMEIRA CHAMADA)
# ============================================================

st.set_page_config(
    page_title="FiscalizaGov — Radar do Executivo",
    layout="wide"
)

APP_TITLE = "FiscalizaGov — Radar do Executivo"

# ============================================================
# UTILITARIOS
# ============================================================

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";").encode("utf-8-sig")

def to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "FiscalizaGov") -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
        worksheet = writer.sheets[sheet_name[:31]]

        # Autoajuste simples de largura
        for i, col in enumerate(df.columns):
            max_len = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).head(200).tolist()])
            worksheet.set_column(i, i, min(max_len + 2, 65))
    output.seek(0)
    return output.read()

# ============================================================
# TELEGRAM
# ============================================================

def telegram_enviar_mensagem(bot_token: str, chat_id: str, mensagem: str, parse_mode: str = "HTML") -> dict:
    """
    Envia mensagem via Telegram Bot API.
    parse_mode: "HTML" ou "MarkdownV2" (recomendo HTML).
    """
    try:
        if not bot_token or not chat_id:
            return {"ok": False, "error": "BOT_TOKEN e CHAT_ID são obrigatórios."}

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": mensagem,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        resp = requests.post(url, json=payload, timeout=12)
        data = resp.json()
        if data.get("ok"):
            return {"ok": True, "message": "Mensagem enviada com sucesso!"}
        return {"ok": False, "error": data.get("description", "Erro desconhecido")}
    except Exception as e:
        # nao usar str(e): a mensagem da excecao inclui a URL com o token do bot
        return {"ok": False, "error": f"Falha de rede ao falar com o Telegram ({type(e).__name__})."}

def telegram_testar_conexao(bot_token: str, chat_id: str) -> dict:
    msg = (
        f"🔎 <b>{APP_TITLE}</b>\n\n"
        f"✅ Conexão configurada com sucesso.\n"
        f"🕒 {agora_brasilia_str()}\n\n"
        f"Você receberá notificações dos achados do Executivo."
    )
    return telegram_enviar_mensagem(bot_token, chat_id, msg, parse_mode="HTML")


# ============================================================
# DADOS DO DOU (coleta em dou.py)
# ============================================================

class ColetaFalhou(Exception):
    def __init__(self, erros, parcial):
        super().__init__("; ".join(erros))
        self.erros = erros
        self.parcial = parcial

@st.cache_data(ttl=1800, show_spinner=False)
def _dou_cache(data: dt.date, secoes: tuple) -> pd.DataFrame:
    # Se algo falhar, levanta excecao: o st.cache_data NAO guarda excecoes,
    # entao falha nunca fica "presa" no cache por 30 minutos.
    df, erros = dou.coletar_dou(data, list(secoes))
    if erros:
        raise ColetaFalhou(erros, df)
    return df

def carregar_dou(data: dt.date, secoes: List[str]):
    try:
        return _dou_cache(data, tuple(secoes)), []
    except ColetaFalhou as e:
        return e.parcial, e.erros

# ============================================================
# UI — ACESSO RESTRITO (igual monitor)
# ============================================================

if "autenticado" not in st.session_state:
    st.session_state.autenticado = False

if not st.session_state.autenticado:
    st.markdown("## 🔒 Acesso restrito — FiscalizaGov")
    st.markdown("Sistema interno de fiscalização do Executivo.")
    senha = st.text_input("Digite a senha de acesso", type="password")

    if senha:
        try:
            senha_correta = st.secrets["auth"]["senha"]
        except Exception:
            senha_correta = os.getenv("FISCALIZAGOV_SENHA", "")
        # sem senha configurada, ninguem entra (evita aceitar senha vazia)
        senha_ok = bool(senha_correta) and hmac.compare_digest(
            senha.encode("utf-8"), str(senha_correta).encode("utf-8")
        )

        if senha_ok:
            st.session_state.autenticado = True
            st.success("Acesso liberado.")
            st.rerun()
        else:
            st.error("Senha incorreta.")
    st.stop()

# ============================================================
# CABEÇALHO
# ============================================================

st.title(APP_TITLE)
st.caption(f"🕒 Atualizado em {agora_brasilia_str()} (horário de Brasília).")

# ============================================================
# SIDEBAR — CONFIG GLOBAL
# ============================================================

st.sidebar.header("⚙️ Configurações")
st.sidebar.write("Filtros gerais (MVP: DOU).")

data_ref = st.sidebar.date_input("Data do DOU", value=dou.hoje_brasilia())
secoes = st.sidebar.multiselect(
    "Seções do DOU", SECOES_DISPONIVEIS, default=DEFAULT_SECOES_DOU,
    format_func=lambda s: dou.ROTULOS_SECAO.get(s, s),
)

termos_busca = st.sidebar.text_area(
    "Palavras-chave (busca)",
    value="imposto\ntribut\ncontribui\nbenefício\nprograma\nportaria\ndecreto\ninstrução normativa",
    height=170
)
termos_alerta = st.sidebar.text_area(
    "Palavras-chave (alerta/score)",
    value="imposto\nbenefício\nregulamenta\nfica instituído\nfica criado",
    height=130
)

termos_busca_list = [t.strip() for t in termos_busca.splitlines() if t.strip()]
termos_alerta_list = [t.strip() for t in termos_alerta.splitlines() if t.strip()]

st.sidebar.divider()

st.sidebar.subheader("📨 Telegram")
bot_token = st.sidebar.text_input("BOT_TOKEN", type="password", value=os.getenv("TELEGRAM_BOT_TOKEN", ""))
chat_id = st.sidebar.text_input("CHAT_ID", value=os.getenv("TELEGRAM_CHAT_ID", ""))

c1, c2 = st.sidebar.columns(2)
with c1:
    if st.button("Testar"):
        res = telegram_testar_conexao(bot_token, chat_id)
        if res.get("ok"):
            st.sidebar.success(res.get("message", "OK"))
        else:
            st.sidebar.error(res.get("error", "Erro"))
with c2:
    enviar_top = st.button("Enviar TOP 5")

# ============================================================
# ABAS
# ============================================================

TAB_NOMES = [
    "🏠 Visão Geral",
    "📜 DOU Inteligente (MVP)",
    "💸 Gastos Diretos (placeholder)",
    "🧾 Licitações & Contratos (placeholder)",
    "👔 Cargos & Nomeações (placeholder)",
    "✈️ Viagens/Diárias (placeholder)",
    "🎁 Programas (placeholder)",
    "🏗️ Obras (placeholder)",
    "🚨 Alertas & Evidências (placeholder)",
]
tabs = st.tabs(TAB_NOMES)

# ============================================================
# DADOS — DOU
# ============================================================

with st.spinner("Coletando DOU (Imprensa Nacional)..."):
    df_dou, erros_coleta = carregar_dou(data_ref, secoes)
    df_dou_f = dou.filtrar(df_dou, termos_busca_list)
    df_dou_rank = dou.rankear(df_dou_f, termos_alerta_list) if not df_dou_f.empty else df_dou_f

if erros_coleta:
    st.error(
        "⚠️ **Falha na coleta do DOU.** Os resultados abaixo podem estar incompletos "
        "e a ausência de itens **não** significa ausência de atos.\n\n"
        + "\n".join(f"- {e}" for e in erros_coleta)
    )
elif df_dou.empty and secoes:
    st.info("Nenhuma publicação nas seções escolhidas nesta data (dia sem edição ou edição ainda não publicada).")
else:
    st.caption(f"📊 {len(df_dou)} atos coletados; {len(df_dou_f)} com as palavras-chave de busca.")

# ============================================================
# ABA 1 — VISÃO GERAL
# ============================================================

with tabs[0]:
    st.subheader("Achados do dia (MVP: DOU)")
    if df_dou_rank.empty:
        st.info("Nenhum item encontrado no DOU com os filtros atuais.")
    else:
        topn = df_dou_rank.head(15).copy()

        st.dataframe(
            topn,
            use_container_width=True,
            hide_index=True
        )

        colA, colB, colC = st.columns([1, 1, 2])
        with colA:
            st.download_button(
                "⬇️ Baixar CSV (achados)",
                data=to_csv_bytes(df_dou_rank),
                file_name=f"fiscalizagov_dou_{data_ref.strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
        with colB:
            st.download_button(
                "⬇️ Baixar XLSX (achados)",
                data=to_xlsx_bytes(df_dou_rank, "DOU"),
                file_name=f"fiscalizagov_dou_{data_ref.strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        with colC:
            st.markdown("**Dica:** refine palavras‑chave para achar apenas atos sensíveis (tributos, benefícios, regulação, gastos).")

# ============================================================
# ABA 2 — DOU INTELIGENTE
# ============================================================

with tabs[1]:
    st.subheader("📜 DOU Inteligente — Portarias/Decretos/INs (MVP)")
    st.caption("Coleta por data e seções. Filtra por palavras‑chave e aplica score por risco/tema.")

    if df_dou_rank.empty:
        st.warning("Sem resultados para os filtros escolhidos.")
    else:
        # “Ficha” clicável simples: seleção por índice
        st.write("**Lista ordenada por Score (maior primeiro):**")
        st.dataframe(df_dou_rank, use_container_width=True, hide_index=True)

        st.markdown("---")
        st.write("**Abrir detalhe do item:**")
        idx = st.number_input("Linha (0 = primeira)", min_value=0, max_value=max(0, len(df_dou_rank)-1), value=0, step=1)
        row = df_dou_rank.iloc[int(idx)].to_dict()

        st.markdown(f"### {row.get('Título','(sem título)')}")
        st.markdown(f"**Score:** {row.get('Score','')}  \n**Motivos:** {row.get('Motivos','')}")
        st.markdown(f"**Data:** {row.get('Data','')}  \n**Seção:** {row.get('Seção','')}  \n**Órgão:** {row.get('Órgão','')}")
        st.markdown("**Ementa/Resumo:**")
        st.write(row.get("Ementa/Resumo", ""))

        link = row.get("Link", "")
        if link:
            st.markdown(f"**Link oficial:** {link}")
        else:
            st.info("Este item não trouxe link direto no payload. Você pode abrir o DOU pela data e seção e localizar pelo título.")

        # Botão de enviar item no Telegram
        if st.button("📨 Enviar este item no Telegram"):
            if not bot_token or not chat_id:
                st.error("Preencha BOT_TOKEN e CHAT_ID na sidebar.")
            else:
                esc = lambda k: html.escape(str(row.get(k, "")))
                msg = (
                    f"🔎 <b>{APP_TITLE}</b>\n"
                    f"🕒 {agora_brasilia_str()}\n\n"
                    f"📜 <b>DOU</b> — {esc('Seção')}\n"
                    f"🏛️ <b>Órgão:</b> {esc('Órgão')}\n"
                    f"⭐ <b>Score:</b> {esc('Score')} ({esc('Motivos')})\n\n"
                    f"<b>{esc('Título')}</b>\n\n"
                    f"{esc('Ementa/Resumo')[:1500]}\n\n"
                    f"{esc('Link')}"
                )
                res = telegram_enviar_mensagem(bot_token, chat_id, msg, parse_mode="HTML")
                if res.get("ok"):
                    st.success("Enviado no Telegram.")
                else:
                    st.error(res.get("error", "Erro ao enviar."))

# ============================================================
# PLACEHOLDERS (as demais abas entram na sequência)
# ============================================================

placeholder_text = (
    "Este módulo está como placeholder no MVP.\n\n"
    "Próximo passo: implementar coletor + normalização para o schema FiscalizaGov (Evento), "
    "seguindo o mesmo padrão do Monitor Zanatta: filtros, ordenação, detalhes, exportação e alertas."
)

for i in range(2, len(tabs)):
    with tabs[i]:
        st.subheader(TAB_NOMES[i])
        st.info(placeholder_text)

# ============================================================
# AÇÃO GLOBAL — ENVIAR TOP 5 (Visão Geral)
# ============================================================

if enviar_top:
    if df_dou_rank.empty:
        st.sidebar.warning("Sem achados para enviar.")
    elif not bot_token or not chat_id:
        st.sidebar.error("Preencha BOT_TOKEN e CHAT_ID.")
    else:
        top5 = df_dou_rank.head(5)
        linhas = []
        for _, r in top5.iterrows():
            linhas.append(
                f"• <b>{html.escape(str(r.get('Título','')))}</b>\n"
                f"  🏛️ {html.escape(str(r.get('Órgão','')))} | ⭐ {r.get('Score','')}\n"
                f"  {html.escape(str(r.get('Link','')))}"
            )
        msg = (
            f"🔔 <b>{APP_TITLE}</b>\n"
            f"🕒 {agora_brasilia_str()}\n\n"
            f"🏷️ <b>TOP 5 Achados (DOU)</b>\n\n" + "\n\n".join(linhas)
        )
        res = telegram_enviar_mensagem(bot_token, chat_id, msg, parse_mode="HTML")
        if res.get("ok"):
            st.sidebar.success("TOP 5 enviado.")
        else:
            st.sidebar.error(res.get("error", "Erro ao enviar."))
