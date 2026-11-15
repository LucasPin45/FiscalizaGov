# relogio.py - painel "relogio do Governo Federal": quanto arrecadou e quanto gastou no ano, contando por segundo
# ============================================================
# O contador roda no NAVEGADOR, em JavaScript (componente HTML): atualizar a cada segundo pelo Python obrigaria o
# Streamlit a reexecutar a pagina no servidor a cada segundo, para cada visitante (pesado e a tela pisca).
# O Python calcula os valores de partida e o ritmo (painel.py); o JavaScript so soma o tempo que passou.
#
# Os valores sao uma ESTIMATIVA: acumulado ate a data do arquivo da CGU + ritmo medio do ano.
# ============================================================
import json
from typing import Optional

import streamlit as st
import streamlit.components.v1 as components

import dou
import painel as pn


@st.cache_data(ttl=3600, show_spinner=False)
def _painel_cache(ano: int) -> dict:
    p = pn.ler()
    if p is not None and p.get("ano") == ano:
        return p
    return pn.calcular_ao_vivo(ano)  # reserva: levanta ErroColeta (nao fica em cache) se a CGU nao responder


def carregar(ano: int) -> Optional[dict]:
    try:
        return _painel_cache(ano)
    except Exception:  # noqa: BLE001 - qualquer falha de rede/arquivo vira aviso, nunca quebra a Apresentacao
        return None


def dados_js(p: dict) -> dict:
    r, g = p["receita"], p["gasto"]
    return {
        "ano": p["ano"], "inicio": p["inicio_ano_ms"],
        "rec": {"acum": r["arrecadacao"], "asof": r["asof_ms"], "asofTxt": r["asof"],
                "anterior": r.get("mesmo_periodo_ano_anterior"), "credito": r.get("credito", 0),
                "refin": r.get("refinanciamento", 0)},
        "gas": {"acum": g["gasto_liquidado"], "asof": g["asof_ms"], "asofTxt": g["asof"], "juros": g.get("juros_liquidado", 0),
                "amort": g.get("amortizacao_liquidada", 0)},
    }


HTML = """<!doctype html><html lang="pt-BR"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
*{box-sizing:border-box}
body{margin:0;font-family:"Source Sans Pro",system-ui,-apple-system,Segoe UI,Roboto,sans-serif;color:#fff}
.wrap{background:linear-gradient(135deg,#0f2742 0%,#1d4a7a 100%);border-radius:14px;padding:18px 20px 14px}
.topo{display:flex;flex-wrap:wrap;justify-content:space-between;align-items:baseline;gap:6px;margin-bottom:12px}
.topo h2{margin:0;font-size:1.15rem;font-weight:700}
.topo .ao-vivo{font-size:.78rem;opacity:.85}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;background:#4cd964;margin-right:6px;animation:p 1.2s infinite}
@keyframes p{0%{opacity:1}50%{opacity:.25}100%{opacity:1}}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:12px}
.card{background:rgba(255,255,255,.08);border-radius:12px;padding:14px 16px;border-left:5px solid var(--c)}
.card h3{margin:0 0 4px;font-size:.95rem;font-weight:600;opacity:.95}
.big{font-size:1.75rem;font-weight:800;line-height:1.15;font-variant-numeric:tabular-nums;letter-spacing:-.01em;word-break:break-word}
.sub{font-size:.74rem;opacity:.78;margin:2px 0 8px}
.lin{display:flex;justify-content:space-between;gap:10px;font-size:.84rem;padding:3px 0;border-top:1px solid rgba(255,255,255,.12)}
.lin b{font-variant-numeric:tabular-nums;font-weight:700}
.cmp{margin-top:6px;font-size:.78rem;opacity:.9}
.rod{margin-top:12px;background:rgba(255,255,255,.07);border-radius:10px;padding:10px 14px;font-size:.88rem}
.rod b{font-variant-numeric:tabular-nums}
.aviso{margin-top:9px;font-size:.72rem;line-height:1.4;opacity:.78}
</style></head><body><div class="wrap">
<div class="topo"><h2>⏱️ Relógio do Governo Federal em __ANO__</h2><span class="ao-vivo"><span class="dot"></span>contando por segundo (estimativa)</span></div>
<div class="grid">
 <section class="card" style="--c:#4cd964"><h3>💰 Arrecadou</h3><div class="big" id="r_ano">—</div>
  <div class="sub" id="r_sub"></div>
  <div class="lin"><span>Hoje, desde 0h</span><b id="r_hoje">—</b></div>
  <div class="lin"><span>Por dia</span><b id="r_dia">—</b></div>
  <div class="lin"><span>Por hora</span><b id="r_hora">—</b></div>
  <div class="lin"><span>Por segundo</span><b id="r_seg">—</b></div>
  <div class="cmp" id="r_cmp"></div></section>
 <section class="card" style="--c:#ff6b6b"><h3>💸 Gastou</h3><div class="big" id="g_ano">—</div>
  <div class="sub" id="g_sub"></div>
  <div class="lin"><span>Hoje, desde 0h</span><b id="g_hoje">—</b></div>
  <div class="lin"><span>Por dia</span><b id="g_dia">—</b></div>
  <div class="lin"><span>Por hora</span><b id="g_hora">—</b></div>
  <div class="lin"><span>Por segundo</span><b id="g_seg">—</b></div>
  <div class="cmp" id="g_cmp"></div></section>
</div>
<div class="rod" id="rod"></div>
<div class="aviso" id="aviso"></div>
</div>
<script>
const D = __DADOS__;
const nf0 = new Intl.NumberFormat('pt-BR', {maximumFractionDigits: 0});
const nf1 = new Intl.NumberFormat('pt-BR', {maximumFractionDigits: 1});
const brl = v => 'R$ ' + nf0.format(Math.round(v));
const curto = v => {
  const a = Math.abs(v);
  if (a >= 1e12) return 'R$ ' + nf1.format(v / 1e12) + ' tri';
  if (a >= 1e9) return 'R$ ' + nf1.format(v / 1e9) + ' bi';
  if (a >= 1e6) return 'R$ ' + nf1.format(v / 1e6) + ' mi';
  return brl(v);
};
const dataBR = iso => iso.split('-').reverse().join('/');
const ritmo = (s) => s.acum / Math.max((s.asof - D.inicio) / 1000, 1);   // R$ por segundo
const agora = () => Date.now();
const estima = (s, t) => s.acum + ritmo(s) * Math.max(t - s.asof, 0) / 1000;
const segDesdeMeiaNoite = t => { const d = new Date(t - 3 * 3600 * 1000); // Brasília = UTC-3
  return d.getUTCHours() * 3600 + d.getUTCMinutes() * 60 + d.getUTCSeconds() + d.getUTCMilliseconds() / 1000; };
const $ = id => document.getElementById(id);
function fixos() {
  for (const [p, s] of [['r', D.rec], ['g', D.gas]]) {
    const r = ritmo(s);
    $(p + '_dia').textContent = curto(r * 86400);
    $(p + '_hora').textContent = curto(r * 3600);
    $(p + '_seg').textContent = brl(r);
    $(p + '_sub').textContent = 'acumulado no ano (estimativa agora) · arquivo da CGU até ' + dataBR(s.asofTxt);
  }
  if (D.rec.anterior) {
    const v = (D.rec.acum / D.rec.anterior - 1) * 100;
    $('r_cmp').textContent = (v >= 0 ? '▲ ' : '▼ ') + nf1.format(Math.abs(v)) + '% sobre o mesmo período de ' + (D.ano - 1) + ' (' + curto(D.rec.anterior) + ')';
  }
  $('g_cmp').textContent = 'Inclui R$ ' + nf1.format(D.gas.juros / 1e9) + ' bi de juros da dívida; não inclui amortização nem refinanciamento.';
  $('aviso').innerHTML = 'Os contadores são <b>estimativas</b>: valor acumulado até a data do arquivo oficial da CGU + o ritmo médio de ' + D.ano +
    ' (sem sazonalidade). <b>Arrecadou</b> = receitas correntes e de capital, sem operações de crédito e sem receitas intra-orçamentárias. ' +
    '<b>Gastou</b> = despesa liquidada, sem amortização e refinanciamento da dívida. Dívida contratada no ano (operações de crédito): ' +
    curto(D.rec.credito) + ', dos quais ' + curto(D.rec.refin) + ' são refinanciamento (rolagem). Fonte: Portal da Transparência / CGU.';
}
function tick() {
  const t = agora(), h = segDesdeMeiaNoite(t);
  const ra = estima(D.rec, t), ga = estima(D.gas, t);
  $('r_ano').textContent = brl(ra); $('g_ano').textContent = brl(ga);
  $('r_hoje').textContent = brl(ritmo(D.rec) * h); $('g_hoje').textContent = brl(ritmo(D.gas) * h);
  const dif = ga - ra;
  $('rod').innerHTML = dif > 0
    ? '🧮 No ano, o governo gastou <b>' + brl(dif) + '</b> a mais do que arrecadou (a diferença é coberta com endividamento).'
    : '🧮 No ano, o governo arrecadou <b>' + brl(-dif) + '</b> a mais do que gastou.';
}
fixos(); tick(); setInterval(tick, 100);
</script></body></html>"""


def html_relogio(p: dict) -> str:
    return HTML.replace("__ANO__", str(p["ano"])).replace("__DADOS__", json.dumps(dados_js(p)))


def render_apresentacao() -> None:
    """Painel destacado no topo da Apresentacao. Sem dados, mostra aviso (nunca quebra a pagina)."""
    ano = dou.hoje_brasilia().year
    p = carregar(ano)
    if p is None:
        st.warning("Não consegui carregar os totais de arrecadação e gasto agora (arquivos da CGU). "
                   "Clique em **Atualizar tudo** em instantes.")
        return
    components.html(html_relogio(p), height=400, scrolling=True)
