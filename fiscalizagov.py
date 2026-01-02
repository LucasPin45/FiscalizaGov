# fiscalizagov.py - FiscalizaGov (v0.1 MVP)
# ============================================================
# Super sistema de fiscalização do Executivo (padrão Monitor Zanatta)
# - Abas por módulo
# - Ordenação pelo mais novo
# - Exportação CSV/XLSX
# - Notificações via Telegram (bot)
# ============================================================

import os
import re
import time
import json
import datetime as dt
from typing import List, Dict, Optional, Tuple

import pandas as pd
import requests
import streamlit as st

# ============================================================
# CONFIGURAÇÃO DA PÁGINA (OBRIGATORIAMENTE PRIMEIRA CHAMADA)
# ============================================================

st.set_page_config(
    page_title="FiscalizaGov — Radar do Executivo",
    layout="wide"
)

# ============================================================
# CONSTANTES
# ============================================================

TZ_BRASILIA = "America/Sao_Paulo"  # compatível com Brasília (UTC-3)
DEFAULT_SECOES_DOU = ["do1", "do2", "do3"]  # DOU 1, 2, 3
APP_TITLE = "FiscalizaGov — Radar do Executivo"

# Endpoint que costuma funcionar para leitura por data e seção.
# Observação: a Imprensa Nacional pode mudar formatos. O coletor abaixo é resiliente e retorna vazio se falhar.
IN_LEITURAJORNAL_URL = "https://www.in.gov.br/leiturajornal"

# ============================================================
# UTILITÁRIOS
# ============================================================

def agora_brasilia_str() -> str:
    """Retorna data/hora (string) no fuso de Brasília (aproximação sem pytz)."""
    # Streamlit Cloud nem sempre vem com tzdata; evitar dependência.
    # UTC-3 fixo (suficiente para boletins). Se quiser DST no futuro, use zoneinfo/pytz.
    now_utc = dt.datetime.utcnow()
    now_brt = now_utc - dt.timedelta(hours=3)
    return now_brt.strftime("%d/%m/%Y %H:%M")

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.strip()
    return re.sub(r"\s+", " ", s)

def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig").encode("utf-8-sig")

def to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "FiscalizaGov") -> bytes:
    import io
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
        workbook = writer.book
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
        return {"ok": False, "error": str(e)}

def telegram_testar_conexao(bot_token: str, chat_id: str) -> dict:
    msg = (
        f"🔎 <b>{APP_TITLE}</b>\n\n"
        f"✅ Conexão configurada com sucesso.\n"
        f"🕒 {agora_brasilia_str()}\n\n"
        f"Você receberá notificações dos achados do Executivo."
    )
    return telegram_enviar_mensagem(bot_token, chat_id, msg, parse_mode="HTML")

# ============================================================
# COLETOR DOU (MVP)
# ============================================================

def _dou_parse_payload(payload: dict, data_str: str, secao: str) -> List[Dict]:
    """
    Tenta extrair itens do payload do leiturajornal.
    O formato do JSON varia; por isso usamos heurísticas defensivas.
    """
    itens = []

    # Heurística 1: listas em chaves comuns
    candidate_lists = []
    for key in ["jsonArray", "itens", "items", "data", "conteudo", "materias", "publicacoes", "publicacao"]:
        v = payload.get(key)
        if isinstance(v, list):
            candidate_lists.append(v)

    # Heurística 2: busca profunda em 1 nível
    if not candidate_lists:
        for k, v in payload.items():
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if isinstance(vv, list):
                        candidate_lists.append(vv)

    for lst in candidate_lists:
        for raw in lst:
            if not isinstance(raw, dict):
                continue

            titulo = normalize_text(raw.get("title") or raw.get("titulo") or raw.get("tituloMateria") or raw.get("nome"))
            ementa = normalize_text(raw.get("ementa") or raw.get("summary") or raw.get("resumo") or raw.get("descricao") or raw.get("texto"))
            orgao = normalize_text(raw.get("orgao") or raw.get("orgaoPessoa") or raw.get("orgaoPublicador") or raw.get("hierarquia"))

            # Link / ID: varia muito; tentamos montar o melhor possível
            link = raw.get("url") or raw.get("link") or raw.get("href") or ""
            if not link:
                # Alguns retornam um id numérico para a página /-/{id}
                possible_id = raw.get("id") or raw.get("identificador") or raw.get("idMateria") or raw.get("idPublicacao")
                if possible_id:
                    link = f"https://www.in.gov.br/web/dou/-/{possible_id}"

            itens.append({
                "Data": data_str,
                "Seção": secao.upper(),
                "Título": titulo,
                "Órgão": orgao,
                "Ementa/Resumo": ementa,
                "Link": link
            })

    # Remove itens vazios demais
    itens = [x for x in itens if (x.get("Título") or x.get("Ementa/Resumo") or x.get("Link"))]
    return itens

@st.cache_data(ttl=1800, show_spinner=False)
def dou_coletar(data: dt.date, secoes: List[str]) -> pd.DataFrame:
    """
    Coleta matérias do DOU por data e seção via endpoint 'leiturajornal'.
    Retorna DataFrame com colunas padronizadas para o FiscalizaGov.
    """
    data_str = data.strftime("%Y-%m-%d")
    all_items: List[Dict] = []

    for secao in secoes:
        params = {"data": data_str, "secao": secao}
        try:
            r = requests.get(IN_LEITURAJORNAL_URL, params=params, timeout=18)
            if r.status_code != 200:
                continue

            # Alguns retornos não são JSON puro; tentar extrair
            try:
                payload = r.json()
            except Exception:
                # fallback: tenta achar JSON no texto
                txt = r.text.strip()
                payload = {}
                if txt.startswith("{") and txt.endswith("}"):
                    payload = json.loads(txt)

            if isinstance(payload, dict) and payload:
                all_items.extend(_dou_parse_payload(payload, data_str, secao))

        except Exception:
            continue

        time.sleep(0.2)

    df = pd.DataFrame(all_items)
    if df.empty:
        return df

    # Normalizações finais
    for c in ["Título", "Órgão", "Ementa/Resumo", "Link"]:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str).map(normalize_text)

    # Ordenação: por padrão, não há hora; manter título/ementa como está, mas com Data fixada
    # Se existir campo que indique ordem/hora, o parser pode ser estendido.
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce").dt.strftime("%d/%m/%Y")
    df["Fonte"] = "DOU"
    return df[["Fonte", "Data", "Seção", "Órgão", "Título", "Ementa/Resumo", "Link"]]

def dou_filtrar(df: pd.DataFrame, termos: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    if not termos:
        return df
    termos = [t.strip().lower() for t in termos if t.strip()]
    if not termos:
        return df

    blob = (
        df["Título"].fillna("").astype(str) + " " +
        df["Ementa/Resumo"].fillna("").astype(str) + " " +
        df["Órgão"].fillna("").astype(str)
    ).str.lower()

    mask = False
    for t in termos:
        mask = mask | blob.str.contains(re.escape(t), na=False)
    return df[mask].copy()

def score_dou_row(row: pd.Series, termos_alerta: List[str]) -> Tuple[int, List[str]]:
    """
    Score simples (0–100) baseado em termos de risco/temas.
    Ajuste livre: esta é a 'primeira versão' do motor.
    """
    texto = f"{row.get('Título','')} {row.get('Ementa/Resumo','')} {row.get('Órgão','')}".lower()
    score = 10
    motivos = []

    gatilhos = {
        "imposto": 25,
        "tribut": 25,
        "contribui": 18,
        "taxa": 18,
        "benefício": 18,
        "programa": 12,
        "fica instituído": 22,
        "fica criado": 22,
        "regulamenta": 18,
        "dispõe sobre": 12,
        "autoriza": 15,
        "estabelece": 10,
        "prorroga": 8,
        "excepcional": 10,
        "em caráter": 10
    }

    for k, pts in gatilhos.items():
        if k in texto:
            score += pts
            motivos.append(k)

    for t in termos_alerta:
        if t and t.lower() in texto:
            score += 12
            motivos.append(f"match:{t.lower()}")

    score = max(0, min(100, score))
    return score, motivos[:8]

def dou_rankear(df: pd.DataFrame, termos_alerta: List[str]) -> pd.DataFrame:
    if df.empty:
        return df
    scores = []
    motivos_list = []
    for _, row in df.iterrows():
        sc, mot = score_dou_row(row, termos_alerta)
        scores.append(sc)
        motivos_list.append(", ".join(mot))
    out = df.copy()
    out["Score"] = scores
    out["Motivos"] = motivos_list
    out = out.sort_values(["Score", "Data"], ascending=[False, False])
    cols = ["Score", "Motivos"] + [c for c in out.columns if c not in ["Score", "Motivos"]]
    return out[cols]

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
            senha_ok = (senha == st.secrets["auth"]["senha"])
        except Exception:
            senha_ok = (senha == os.getenv("FISCALIZAGOV_SENHA", ""))

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

data_ref = st.sidebar.date_input("Data do DOU", value=dt.date.today())
secoes = st.sidebar.multiselect("Seções do DOU", DEFAULT_SECOES_DOU, default=DEFAULT_SECOES_DOU)

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

tabs = st.tabs([
    "🏠 Visão Geral",
    "📜 DOU Inteligente (MVP)",
    "💸 Gastos Diretos (placeholder)",
    "🧾 Licitações & Contratos (placeholder)",
    "👔 Cargos & Nomeações (placeholder)",
    "✈️ Viagens/Diárias (placeholder)",
    "🎁 Programas (placeholder)",
    "🏗️ Obras (placeholder)",
    "🚨 Alertas & Evidências (placeholder)",
])

# ============================================================
# DADOS — DOU
# ============================================================

with st.spinner("Coletando DOU (Imprensa Nacional)..."):
    df_dou = dou_coletar(data_ref, secoes)
    df_dou_f = dou_filtrar(df_dou, termos_busca_list)
    df_dou_rank = dou_rankear(df_dou_f, termos_alerta_list) if not df_dou_f.empty else df_dou_f

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
                msg = (
                    f"🔎 <b>{APP_TITLE}</b>\n"
                    f"🕒 {agora_brasilia_str()}\n\n"
                    f"📜 <b>DOU</b> — {row.get('Seção','')}\n"
                    f"🏛️ <b>Órgão:</b> {row.get('Órgão','')}\n"
                    f"⭐ <b>Score:</b> {row.get('Score','')} ({row.get('Motivos','')})\n\n"
                    f"<b>{row.get('Título','')}</b>\n\n"
                    f"{row.get('Ementa/Resumo','')}\n\n"
                    f"{row.get('Link','')}"
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
        st.subheader(tabs[i]._label)  # type: ignore[attr-defined]
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
                f"• <b>{r.get('Título','')}</b>\n"
                f"  🏛️ {r.get('Órgão','')} | ⭐ {r.get('Score','')}\n"
                f"  {r.get('Link','')}"
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
