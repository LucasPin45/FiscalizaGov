# notificar_fiscalizagov.py - Notificador FiscalizaGov (MVP: DOU)
# ============================================================
# Uso (GitHub Actions / cron):
#   python notificar_fiscalizagov.py
#
# Variaveis de ambiente:
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID  (obrigatorias)
#   FISCALIZAGOV_TERMS   termos de busca, separados por virgula (opcional)
#   FISCALIZAGOV_ALERTA  termos de alerta/score, separados por virgula (opcional)
#   FISCALIZAGOV_SECOES  ex: do1,do2,do3 (opcional)
#   FISCALIZAGOV_DATE    AAAA-MM-DD (opcional; padrao: hoje em Brasilia)
#
# Falha de coleta NUNCA vira "sem achados": o script avisa no Telegram
# e termina com codigo 1 (o job do GitHub Actions fica vermelho).
# ============================================================

import datetime as dt
import html
import os
import sys
from typing import Dict, List

import requests

import dou

TERMOS_PADRAO = [
    "imposto", "tribut", "contribui", "benefício", "programa",
    "decreto", "portaria", "instrução normativa",
]
ALERTA_PADRAO = ["imposto", "benefício", "regulamenta", "fica instituído", "fica criado"]
LIMITE_TELEGRAM = 4000  # limite da API e 4096 caracteres


def _lista_env(nome: str, padrao: List[str]) -> List[str]:
    bruto = os.getenv(nome, "")
    itens = [x.strip() for x in bruto.split(",") if x.strip()]
    return itens or padrao


def telegram_send(bot_token: str, chat_id: str, msg: str) -> Dict:
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": msg[:LIMITE_TELEGRAM],
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        return r.json()
    except Exception as e:
        # nao usar str(e): a mensagem da excecao inclui a URL com o token
        return {"ok": False, "description": type(e).__name__}


def _enviar_ou_falhar(bot_token: str, chat_id: str, msg: str) -> None:
    res = telegram_send(bot_token, chat_id, msg)
    if not res.get("ok"):
        raise SystemExit(f"Falha ao enviar Telegram: {res.get('description', 'erro')}")


def montar_mensagem_achados(df, data: dt.date, total_analisados: int) -> str:
    linhas = []
    for _, r in df.head(5).iterrows():
        titulo = html.escape(str(r.get("Título", "")))
        orgao = html.escape(str(r.get("Órgão", "")))
        motivos = html.escape(str(r.get("Motivos", "")))
        link = html.escape(str(r.get("Link", "")))
        linhas.append(
            f"• <b>{titulo}</b>\n"
            f"  🏛️ {orgao} | {r.get('Seção', '')} | ⭐ {r.get('Score', '')}\n"
            f"  {('🧭 ' + motivos + chr(10) + '  ') if motivos else ''}{link}"
        )
    return (
        "🔎 <b>FiscalizaGov</b>\n"
        f"🕒 {dou.agora_brasilia_str()} | DOU de {data.strftime('%d/%m/%Y')}\n"
        f"📊 {total_analisados} atos analisados, {len(df)} com os termos monitorados\n\n"
        "🏷️ <b>TOP 5 (por relevância temática)</b>\n\n" + "\n\n".join(linhas)
    )


def main() -> int:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not bot_token or not chat_id:
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID.")

    termos = _lista_env("FISCALIZAGOV_TERMS", TERMOS_PADRAO)
    alerta = _lista_env("FISCALIZAGOV_ALERTA", ALERTA_PADRAO)
    secoes = _lista_env("FISCALIZAGOV_SECOES", dou.DEFAULT_SECOES_DOU)

    date_env = os.getenv("FISCALIZAGOV_DATE", "").strip()
    data = dt.datetime.strptime(date_env, "%Y-%m-%d").date() if date_env else dou.hoje_brasilia()

    df, erros = dou.coletar_dou(data, secoes)

    if erros:
        detalhe = "\n".join(f"• {html.escape(e)}" for e in erros)
        _enviar_ou_falhar(
            bot_token, chat_id,
            "⚠️ <b>FiscalizaGov — FALHA NA COLETA</b>\n"
            f"🕒 {dou.agora_brasilia_str()} | DOU de {data.strftime('%d/%m/%Y')}\n\n"
            f"{detalhe}\n\n"
            "Este aviso <b>não</b> significa ausência de atos: o DOU não pôde ser lido.",
        )
        print("Falha na coleta:", erros, file=sys.stderr)
        return 1

    total = len(df)
    if total == 0:
        _enviar_ou_falhar(
            bot_token, chat_id,
            "🔔 <b>FiscalizaGov</b>\n"
            f"🕒 {dou.agora_brasilia_str()}\n\n"
            f"📜 DOU de {data.strftime('%d/%m/%Y')}: nenhuma publicação nas seções {', '.join(secoes)} "
            "(dia sem edição, como domingo e feriado, ou edição ainda não publicada).",
        )
        return 0

    achados = dou.rankear(dou.filtrar(df, termos), alerta)
    if achados.empty:
        _enviar_ou_falhar(
            bot_token, chat_id,
            "🔔 <b>FiscalizaGov</b>\n"
            f"🕒 {dou.agora_brasilia_str()}\n\n"
            f"📜 DOU de {data.strftime('%d/%m/%Y')}: {total} atos analisados, "
            "nenhum com os termos monitorados.",
        )
        return 0

    _enviar_ou_falhar(bot_token, chat_id, montar_mensagem_achados(achados, data, total))
    return 0


if __name__ == "__main__":
    sys.exit(main())
