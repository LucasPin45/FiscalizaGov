#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
notificar_tramitacoes.py
========================================
Monitor de tramitações da Deputada Júlia Zanatta
Verifica novas movimentações e notifica via Telegram

Tipos monitorados: PL, PLP, PDL, RIC, REQ, PRL
Período: Desde 2023 (início do mandato)
Horário: 08:00 às 20:00 (Brasília) - Segunda a Sexta
"""

import os
import sys
import json
import requests
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ============================================================
# CONFIGURAÇÕES
# ============================================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"
HEADERS = {"User-Agent": "MonitorZanatta/24.0 (gabinete-julia-zanatta)"}

DEPUTADA_ID = 220559  # Júlia Zanatta
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Tipos de proposição a monitorar
TIPOS_MONITORADOS = ["PL", "PLP", "PDL", "RIC", "REQ", "PRL"]

# Data de início do mandato
DATA_INICIO_MANDATO = "2023-02-01"

# Arquivo para guardar estado entre execuções
ESTADO_FILE = Path("estado_monitor.json")

# Fuso horário de Brasília (UTC-3)
FUSO_BRASILIA = timezone(timedelta(hours=-3))

# ============================================================
# GERENCIAMENTO DE ESTADO
# ============================================================

def carregar_estado():
    """Carrega o estado da última execução"""
    try:
        if ESTADO_FILE.exists():
            with open(ESTADO_FILE, "r") as f:
                estado = json.load(f)
                print(f"📁 Estado carregado: {estado}")
                return estado
    except Exception as e:
        print(f"⚠️ Erro ao carregar estado: {e}")
    
    return {"ultima_novidade": True}


def salvar_estado(teve_novidade):
    """Salva o estado para a próxima execução"""
    estado = {"ultima_novidade": teve_novidade}
    try:
        with open(ESTADO_FILE, "w") as f:
            json.dump(estado, f)
        print(f"💾 Estado salvo: {estado}")
    except Exception as e:
        print(f"⚠️ Erro ao salvar estado: {e}")


# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def obter_data_hora_brasilia():
    """Retorna data e hora no fuso de Brasília"""
    agora_utc = datetime.now(timezone.utc)
    agora_brasilia = agora_utc.astimezone(FUSO_BRASILIA)
    return agora_brasilia.strftime("%d/%m/%Y às %H:%M")


def buscar_proposicoes_por_tipo(deputado_id, sigla_tipo):
    """Busca TODAS as proposições de um tipo específico desde o início do mandato"""
    
    proposicoes = []
    pagina = 1
    
    while True:
        url = f"{BASE_URL}/proposicoes"
        params = {
            "idDeputadoAutor": deputado_id,
            "siglaTipo": sigla_tipo,
            "dataInicio": DATA_INICIO_MANDATO,
            "ordem": "DESC",
            "ordenarPor": "id",
            "pagina": pagina,
            "itens": 100
        }
        
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if not data.get("dados"):
                break
                
            proposicoes.extend(data["dados"])
            
            links = data.get("links", [])
            tem_proxima = any(link.get("rel") == "next" for link in links)
            
            if not tem_proxima:
                break
                
            pagina += 1
            time.sleep(0.2)
            
        except Exception as e:
            print(f"   ⚠️ Erro ao buscar {sigla_tipo}: {e}")
            break
    
    return proposicoes


def buscar_todas_proposicoes(deputado_id):
    """Busca proposições de todos os tipos monitorados desde 2023"""
    
    print(f"🔍 Buscando proposições dos tipos: {', '.join(TIPOS_MONITORADOS)}")
    print(f"📅 Período: desde {DATA_INICIO_MANDATO} (início do mandato)")
    print()
    
    todas_proposicoes = []
    
    for tipo in TIPOS_MONITORADOS:
        props = buscar_proposicoes_por_tipo(deputado_id, tipo)
        print(f"   {tipo}: {len(props)} proposições")
        todas_proposicoes.extend(props)
        time.sleep(0.3)
    
    print(f"\n✅ Total de proposições a verificar: {len(todas_proposicoes)}")
    return todas_proposicoes


def buscar_ultima_tramitacao(proposicao_id):
    """Busca a última tramitação de uma proposição"""
    
    url = f"{BASE_URL}/proposicoes/{proposicao_id}/tramitacoes"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        
        tramitacoes = data.get("dados", [])
        
        if tramitacoes:
            tramitacoes_ordenadas = sorted(
                tramitacoes,
                key=lambda x: x.get("dataHora", ""),
                reverse=True
            )
            return tramitacoes_ordenadas[0]
            
    except Exception:
        pass
    
    return None


def tramitacao_recente(tramitacao, horas=48):
    """Verifica se a tramitação é das últimas X horas"""
    
    if not tramitacao or not tramitacao.get("dataHora"):
        return False
    
    try:
        data_tram = tramitacao["dataHora"][:10]
        agora_brasilia = datetime.now(FUSO_BRASILIA)
        data_corte = (agora_brasilia - timedelta(hours=horas)).strftime("%Y-%m-%d")
        return data_tram >= data_corte
    except Exception:
        return False


def formatar_mensagem_novidade(proposicao, tramitacao):
    """Formata mensagem de nova tramitação"""
    
    sigla = proposicao.get("siglaTipo", "")
    numero = proposicao.get("numero", "")
    ano = proposicao.get("ano", "")
    ementa = proposicao.get("ementa", "")
    
    if len(ementa) > 200:
        ementa = ementa[:197] + "..."
    
    data_tram = tramitacao.get("dataHora", "")
    if data_tram:
        try:
            dt = datetime.fromisoformat(data_tram.replace("Z", ""))
            data_formatada = dt.strftime("%d/%m/%Y")
        except:
            data_formatada = data_tram[:10]
    else:
        data_formatada = "Data não disponível"
    
    descricao = tramitacao.get("despacho", "") or tramitacao.get("descricaoTramitacao", "")
    link = f"https://www.camara.leg.br/proposicoesWeb/fichadetramitacao?idProposicao={proposicao['id']}"
    
    data_hora_varredura = obter_data_hora_brasilia()
    
    mensagem = f"""📢 <b>Monitor Parlamentar Informa:</b>

Houve nova movimentação!

📄 <b>{sigla} {numero}/{ano}</b>
{ementa}

📅 {data_formatada} → {descricao}

🔗 <a href="{link}">Ver tramitação completa</a>

⏰ <i>Varredura realizada em {data_hora_varredura}</i>"""
    
    return mensagem


def formatar_mensagem_sem_novidades_completa():
    """Formata mensagem completa quando não há novidades"""
    
    data_hora = obter_data_hora_brasilia()
    
    mensagem = f"""🔍 <b>Monitor Parlamentar Informa:</b>

Na última varredura não foram encontradas tramitações recentes em matérias da Dep. Júlia Zanatta.

Mas continue atento! 👀

⏰ <i>Varredura realizada em {data_hora}</i>"""
    
    return mensagem


def formatar_mensagem_sem_novidades_curta():
    """Formata mensagem curta quando não há novidades"""
    
    data_hora = obter_data_hora_brasilia()
    
    mensagem = f"""🔍 Ainda sem novidades em matérias da Dep. Júlia Zanatta.

⏰ <i>{data_hora}</i>"""
    
    return mensagem


def enviar_telegram(mensagem):
    """Envia mensagem para o Telegram"""
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ TELEGRAM_BOT_TOKEN ou TELEGRAM_CHAT_ID não configurados!")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": mensagem,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        print("✅ Mensagem enviada com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")
        return False


# ============================================================
# FUNÇÃO PRINCIPAL
# ============================================================

def main():
    """Verifica novas tramitações e notifica via Telegram"""
    
    data_hora_brasilia = obter_data_hora_brasilia()
    
    print("=" * 60)
    print("🔔 MONITOR DE TRAMITAÇÕES - DEPUTADA JÚLIA ZANATTA")
    print("=" * 60)
    print(f"📅 Data/Hora (Brasília): {data_hora_brasilia}")
    print()
    
    # Verificar variáveis de ambiente
    if not TELEGRAM_BOT_TOKEN:
        print("❌ ERRO: TELEGRAM_BOT_TOKEN não configurado!")
        sys.exit(1)
    if not TELEGRAM_CHAT_ID:
        print("❌ ERRO: TELEGRAM_CHAT_ID não configurado!")
        sys.exit(1)
    
    print(f"✅ Bot Token: {TELEGRAM_BOT_TOKEN[:10]}...")
    print(f"✅ Chat ID: {TELEGRAM_CHAT_ID}")
    print()
    
    # Carregar estado da última execução
    estado = carregar_estado()
    ultima_teve_novidade = estado.get("ultima_novidade", True)
    
    # 1. Buscar proposições
    proposicoes = buscar_todas_proposicoes(DEPUTADA_ID)
    
    if not proposicoes:
        print("⚠️ Nenhuma proposição encontrada")
        if ultima_teve_novidade:
            enviar_telegram(formatar_mensagem_sem_novidades_completa())
        else:
            enviar_telegram(formatar_mensagem_sem_novidades_curta())
        salvar_estado(False)
        return
    
    # 2. Verificar tramitações recentes
    print("\n🔍 Verificando tramitações das últimas 48h...\n")
    
    props_com_novidade = []
    erros = 0
    
    for i, prop in enumerate(proposicoes, 1):
        sigla_prop = f"{prop['siglaTipo']} {prop['numero']}/{prop['ano']}"
        
        if i % 25 == 0 or i == 1:
            print(f"📊 Progresso: {i}/{len(proposicoes)}...")
        
        tramitacao = buscar_ultima_tramitacao(prop["id"])
        
        if tramitacao is None:
            erros += 1
            continue
        
        if tramitacao_recente(tramitacao, horas=48):
            print(f"   ✅ NOVA! {sigla_prop}")
            props_com_novidade.append({
                "proposicao": prop,
                "tramitacao": tramitacao
            })
        
        time.sleep(0.15)
    
    # 3. Resumo
    print(f"\n{'=' * 60}")
    print(f"📊 RESUMO:")
    print(f"   Total verificadas: {len(proposicoes)}")
    print(f"   Com novidades: {len(props_com_novidade)}")
    print(f"   Erros de API: {erros}")
    print(f"{'=' * 60}")
    
    # 4. Enviar notificações
    if props_com_novidade:
        print(f"\n📤 Enviando {len(props_com_novidade)} notificação(ões)...\n")
        
        enviadas = 0
        for item in props_com_novidade:
            mensagem = formatar_mensagem_novidade(item["proposicao"], item["tramitacao"])
            if enviar_telegram(mensagem):
                enviadas += 1
            time.sleep(1)
        
        salvar_estado(True)
        print(f"\n✅ Processo concluído! {enviadas} mensagens enviadas.")
    
    else:
        print("\n📤 Enviando mensagem de status...")
        
        if ultima_teve_novidade:
            print("   → Mensagem COMPLETA (primeira do ciclo)")
            enviar_telegram(formatar_mensagem_sem_novidades_completa())
        else:
            print("   → Mensagem CURTA (continuação)")
            enviar_telegram(formatar_mensagem_sem_novidades_curta())
        
        salvar_estado(False)
        print("\n✅ Processo concluído!")


if __name__ == "__main__":
    main()