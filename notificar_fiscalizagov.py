# notificar_fiscalizagov.py - Notificador FiscalizaGov (MVP: DOU)
# ============================================================
# Uso típico (GitHub Actions / Cron):
#   python notificar_fiscalizagov.py
#
# Variáveis de ambiente:
#   TELEGRAM_BOT_TOKEN
#   TELEGRAM_CHAT_ID
#   FISCALIZAGOV_TERMS (opcional, separado por vírgula)
#   FISCALIZAGOV_SECOES (opcional, ex: do1,do2,do3)
#   FISCALIZAGOV_DATE (opcional, YYYY-MM-DD; default: hoje)
# ============================================================

import os
import re
import json
import time
import datetime as dt
from typing import List, Dict

import requests
import pandas as pd


IN_LEITURAJORNAL_URL = "https://www.in.gov.br/leiturajornal"
DEFAULT_SECOES_DOU = ["do1", "do2", "do3"]

def agora_brasilia_str() -> str:
    now_utc = dt.datetime.utcnow()
    now_brt = now_utc - dt.timedelta(hours=3)
    return now_brt.strftime("%d/%m/%Y %H:%M")

def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    return re.sub(r"\s+", " ", s)

def telegram_send(bot_token: str, chat_id: str, msg: str, parse_mode: str = "HTML") -> Dict:
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True
        }
        r = requests.post(url, json=payload, timeout=12)
        return r.json()
    except Exception as e:
        return {"ok": False, "description": str(e)}

def _dou_parse_payload(payload: dict, data_str: str, secao: str) -> List[Dict]:
    itens = []

    candidate_lists = []
    for key in ["jsonArray", "itens", "items", "data", "conteudo", "materias", "publicacoes", "publicacao"]:
        v = payload.get(key)
        if isinstance(v, list):
            candidate_lists.append(v)

    if not candidate_lists:
        for _, v in payload.items():
            if isinstance(v, dict):
                for _, vv in v.items():
                    if isinstance(vv, list):
                        candidate_lists.append(vv)

    for lst in candidate_lists:
        for raw in lst:
            if not isinstance(raw, dict):
                continue

            titulo = normalize_text(raw.get("title") or raw.get("titulo") or raw.get("tituloMateria") or raw.get("nome"))
            ementa = normalize_text(raw.get("ementa") or raw.get("summary") or raw.get("resumo") or raw.get("descricao") or raw.get("texto"))
            orgao = normalize_text(raw.get("orgao") or raw.get("orgaoPessoa") or raw.get("orgaoPublicador") or raw.get("hierarquia"))

            link = raw.get("url") or raw.get("link") or raw.get("href") or ""
            if not link:
                possible_id = raw.get("id") or raw.get("identificador") or raw.get("idMateria") or raw.get("idPublicacao")
                if possible_id:
                    link = f"https://www.in.gov.br/web/dou/-/{possible_id}"

            if titulo or ementa or link:
                itens.append({
                    "data": data_str,
                    "secao": secao.upper(),
                    "orgao": orgao,
                    "titulo": titulo,
                    "ementa": ementa,
                    "link": link
                })
    return itens

def dou_collect(date_: dt.date, secoes: List[str]) -> pd.DataFrame:
    data_str = date_.strftime("%Y-%m-%d")
    all_items: List[Dict] = []

    for secao in secoes:
        params = {"data": data_str, "secao": secao}
        try:
            r = requests.get(IN_LEITURAJORNAL_URL, params=params, timeout=18)
            if r.status_code != 200:
                continue
            try:
                payload = r.json()
            except Exception:
                payload = {}
                txt = r.text.strip()
                if txt.startswith("{") and txt.endswith("}"):
                    payload = json.loads(txt)

            if isinstance(payload, dict) and payload:
                all_items.extend(_dou_parse_payload(payload, data_str, secao))
        except Exception:
            continue

        time.sleep(0.2)

    return pd.DataFrame(all_items)

def filter_terms(df: pd.DataFrame, terms: List[str]) -> pd.DataFrame:
    if df.empty or not terms:
        return df
    terms = [t.strip().lower() for t in terms if t.strip()]
    if not terms:
        return df

    blob = (df["titulo"].fillna("").astype(str) + " " + df["ementa"].fillna("").astype(str) + " " + df["orgao"].fillna("").astype(str)).str.lower()
    mask = False
    for t in terms:
        mask = mask | blob.str.contains(re.escape(t), na=False)
    return df[mask].copy()

def main():
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID nas variáveis de ambiente.")

    terms_env = os.getenv("FISCALIZAGOV_TERMS", "")
    if terms_env.strip():
        terms = [x.strip() for x in terms_env.split(",") if x.strip()]
    else:
        terms = ["imposto", "tribut", "contribui", "benefício", "programa", "decreto", "portaria", "instrução normativa"]

    secoes_env = os.getenv("FISCALIZAGOV_SECOES", "")
    secoes = [x.strip() for x in secoes_env.split(",") if x.strip()] if secoes_env.strip() else DEFAULT_SECOES_DOU

    date_env = os.getenv("FISCALIZAGOV_DATE", "")
    if date_env.strip():
        date_ = dt.datetime.strptime(date_env.strip(), "%Y-%m-%d").date()
    else:
        date_ = dt.date.today()

    df = dou_collect(date_, secoes)
    df = filter_terms(df, terms)

    if df.empty:
        msg = (
            "🔔 <b>FiscalizaGov</b>\n"
            f"🕒 {agora_brasilia_str()}\n\n"
            "📜 DOU — Sem achados para os filtros de hoje."
        )
        res = telegram_send(bot_token, chat_id, msg)
        if not res.get("ok"):
            raise SystemExit(res.get("description", "Erro ao enviar Telegram"))
        return

    # TOP 5
    df = df.head(5)
    linhas = []
    for _, r in df.iterrows():
        linhas.append(
            f"• <b>{r.get('titulo','')}</b>\n"
            f"  🏛️ {r.get('orgao','')} | {r.get('secao','')}\n"
            f"  {r.get('link','')}"
        )
    msg = (
        "🔎 <b>FiscalizaGov</b>\n"
        f"🕒 {agora_brasilia_str()}\n\n"
        "🏷️ <b>TOP 5 Achados (DOU)</b>\n\n" + "\n\n".join(linhas)
    )
    res = telegram_send(bot_token, chat_id, msg)
    if not res.get("ok"):
        raise SystemExit(res.get("description", "Erro ao enviar Telegram"))

if __name__ == "__main__":
    main()
