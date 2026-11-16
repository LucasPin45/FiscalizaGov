# ui.py - componentes visuais do FiscalizaGov (mesmo padrao do Monitor Zanatta)
# Cards com borda colorida, selos de relevancia, "ver mais" na ementa,
# cards de KPI e seletores. Cores com transparencia: funcionam em tema claro e escuro.

import datetime as dt
import html
import io
import re
from typing import Callable, List

import pandas as pd
import streamlit as st

import dou

COR_ALTA = "#d32f2f"
COR_MEDIA = "#b7791f"
COR_BAIXA = "rgba(128,128,128,.55)"
COR_ALERTA = "#1a6ef5"
COR_NIVEL = {"Alta": COR_ALTA, "Média": COR_MEDIA, "Baixa": COR_BAIXA}

EMENTA_CURTA = 240
PERIODOS = ["Último dia com edição", "Hoje", "Ontem", "Últimos 3 dias", "Personalizado"]

CSS = """
<style>
/* ---------- fundo e largura ---------- */
.stApp{background:linear-gradient(180deg,#dcefe3 0,#eef6f1 280px,#f3f8f5 100%)}
.main .block-container{max-width:1240px;padding-left:2rem;padding-right:2rem;padding-top:1.2rem}
h1,h2,h3{color:#0f3d2b}
a{color:#13734a}

/* ---------- cabeçalho (hero) ---------- */
.fg-hero{background:linear-gradient(120deg,#0b3d2e 0%,#146b46 55%,#27a06b 100%);color:#fff;border-radius:18px;
         padding:20px 26px;margin:0 0 14px;display:flex;flex-wrap:wrap;gap:14px 28px;align-items:center;
         justify-content:space-between;box-shadow:0 6px 18px rgba(11,61,46,.22)}
.fg-hero-l{display:flex;align-items:center;gap:16px}
.fg-hero-ico{font-size:2.6rem;background:rgba(255,255,255,.14);border-radius:16px;width:68px;height:68px;
             display:flex;align-items:center;justify-content:center}
.fg-hero-t{font-size:1.9rem;font-weight:800;letter-spacing:-.02em;line-height:1.1}
.fg-hero-s{font-size:.98rem;opacity:.92;margin-top:3px}
.fg-hero-r{display:flex;flex-wrap:wrap;gap:6px;max-width:560px;justify-content:flex-end}
.fg-hero-chip{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.22);border-radius:999px;
              padding:3px 12px;font-size:.8rem;white-space:nowrap}

/* ---------- abas ---------- */
.stTabs [data-baseweb="tab-list"]{overflow-x:auto;overflow-y:hidden;flex-wrap:nowrap;scrollbar-width:thin;
                                   gap:6px;padding-bottom:6px;border-bottom:2px solid #bfdccb}
.stTabs [data-baseweb="tab"]{white-space:nowrap;flex-shrink:0;background:#e1f0e7;border-radius:10px 10px 0 0;
                              padding:8px 15px;height:auto}
.stTabs [data-baseweb="tab"] p{font-weight:600;color:#14532d}
.stTabs [data-baseweb="tab"][aria-selected="true"]{background:#1b8a5a}
.stTabs [data-baseweb="tab"][aria-selected="true"] p{color:#fff}
.stTabs [data-baseweb="tab-highlight"]{background:#0f5a3a}
/* as quatro abas ligadas ao Diário Oficial (6 a 9) ficam em um tom azulado, para ler como um grupo */
.stTabs [data-baseweb="tab-list"] [data-baseweb="tab"]:nth-child(n+6):nth-child(-n+9){background:#dde8f3}
.stTabs [data-baseweb="tab-list"] [data-baseweb="tab"]:nth-child(n+6):nth-child(-n+9) p{color:#1c3f66}
.stTabs [data-baseweb="tab-list"] [data-baseweb="tab"]:nth-child(n+6):nth-child(-n+9)[aria-selected="true"]{background:#1f5f99}
.stTabs [data-baseweb="tab-list"] [data-baseweb="tab"]:nth-child(n+6):nth-child(-n+9)[aria-selected="true"] p{color:#fff}

/* ---------- botões e campos ---------- */
.stButton>button,.stDownloadButton>button{border-radius:10px;border:1px solid #1b8a5a;font-weight:600}
.stButton>button:hover,.stDownloadButton>button:hover{border-color:#0f5a3a;color:#0f5a3a;background:#e3f1e9}
.stButton>button[kind="primary"]{background:#1b8a5a;color:#fff}

/* ---------- cartões de indicador (KPI) ---------- */
.fg-kpi{background:#fff;border:1px solid #d6e8dc;border-left:5px solid var(--fg-cor);border-radius:12px;
        padding:12px 16px;margin-bottom:10px;min-height:92px;box-shadow:0 1px 4px rgba(15,61,43,.08)}
.fg-kpi-l{font-size:.82rem;color:#4b6455}
.fg-kpi-v{font-size:1.8rem;font-weight:800;line-height:1.25;color:#0f3d2b}
.fg-kpi-s{font-size:.75rem;color:#5f7a6b}

/* ---------- cartões de módulo (Apresentação) ---------- */
.fg-mod{background:#fff;border:1px solid #d6e8dc;border-radius:16px;padding:16px 18px;margin-bottom:14px;min-height:236px;
        box-shadow:0 2px 8px rgba(15,61,43,.09);border-top:5px solid var(--fg-cor)}
.fg-mod-h{display:flex;align-items:center;gap:9px;font-weight:700;font-size:1rem;color:#0f3d2b}
.fg-mod-ico{font-size:1.4rem}
.fg-mod-v{font-size:1.65rem;font-weight:800;line-height:1.2;margin:8px 0 2px;color:#0f3d2b;word-break:break-word}
.fg-mod-s{font-size:.8rem;color:#4b6455;margin-bottom:6px}
.fg-mod-l{font-size:.84rem;color:#27412f;padding:3px 0;border-top:1px dashed #d6e8dc}
.fg-mod-go{margin-top:8px;font-size:.78rem;font-weight:700;color:var(--fg-cor)}
.fg-mod-off{color:#6b7d72;font-size:.88rem;margin-top:10px}

/* ---------- cartões de achado ---------- */
.fg-card{background:#fff;border:1px solid #d6e8dc;border-left:4px solid var(--fg-cor);
         border-radius:10px;padding:10px 14px;margin:0 0 8px;box-shadow:0 1px 3px rgba(15,61,43,.06)}
.fg-top{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin-bottom:3px}
.fg-badge{font-size:10.5px;font-weight:700;padding:1px 8px;border-radius:999px;color:#fff;letter-spacing:.03em}
.fg-tipo{font-size:11px;padding:0 8px;border-radius:999px;border:1px solid currentColor;opacity:.8;white-space:nowrap}
.fg-sec{font-size:.78rem;opacity:.65}
.fg-tit{font-weight:700;font-size:.98rem}
.fg-tit a{color:inherit;text-decoration:none}
.fg-tit a:hover{text-decoration:underline}
.fg-org{font-size:.82rem;opacity:.78;margin:1px 0 4px}
.fg-em{font-size:.88rem;line-height:1.5;margin:2px 0 6px;opacity:.92}
details.fg-em summary{cursor:pointer;list-style:none}
details.fg-em summary::-webkit-details-marker{display:none}
details.fg-em summary b{font-size:.78rem;opacity:.8;text-decoration:underline}
details.fg-em[open] summary{display:none}
.fg-chips{display:flex;flex-wrap:wrap;gap:4px;margin-top:2px}
.fg-chip{font-size:11px;padding:0 8px;border-radius:999px;background:rgba(15,61,43,.1)}
.fg-chip-a{background:rgba(26,110,245,.15);color:#1a6ef5;font-weight:600}
.fg-chip-w{background:rgba(183,121,31,.2);color:#8a5a10;font-weight:600}
.fg-dia{font-size:1.1rem;font-weight:700;margin:1rem 0 .4rem}
.fg-sec-t{font-size:1.15rem;font-weight:800;color:#0f3d2b;margin:18px 0 8px;display:flex;align-items:center;gap:8px}
.fg-sec-t:after{content:"";flex:1;height:2px;background:linear-gradient(90deg,#bfdccb,transparent)}
</style>
"""


def hero(titulo: str, subtitulo: str, chips) -> str:
    """Faixa verde do topo: nome do painel, uma frase e as áreas que ele acompanha."""
    cs = "".join(f'<span class="fg-hero-chip">{e(c)}</span>' for c in chips)
    return (f'<div class="fg-hero"><div class="fg-hero-l"><div class="fg-hero-ico">🔎</div><div>'
            f'<div class="fg-hero-t">{e(titulo)}</div><div class="fg-hero-s">{e(subtitulo)}</div></div></div>'
            f'<div class="fg-hero-r">{cs}</div></div>')


def secao(titulo: str) -> None:
    st.markdown(f'<div class="fg-sec-t">{e(titulo)}</div>', unsafe_allow_html=True)


def modulo(icone: str, titulo: str, valor: str, sub: str, linhas, destino: str, cor: str = "#1b8a5a",
           indisponivel: str = "") -> str:
    """Cartão de módulo da Apresentação (HTML). `destino` é o nome da aba para onde o usuário deve ir."""
    corpo = ""
    if indisponivel:
        corpo = f'<div class="fg-mod-off">{e(indisponivel)}</div>'
    else:
        corpo = (f'<div class="fg-mod-v">{e(valor)}</div><div class="fg-mod-s">{e(sub)}</div>'
                 + "".join(f'<div class="fg-mod-l">{e(l)}</div>' for l in linhas if l))
    return (f'<div class="fg-mod" style="--fg-cor:{cor}"><div class="fg-mod-h"><span class="fg-mod-ico">{e(icone)}</span>'
            f'{e(titulo)}</div>{corpo}<div class="fg-mod-go">→ aba {e(destino)}</div></div>')


def e(txt) -> str:
    """Escapa texto externo para HTML; quebras de linha viram espaco (linha em branco quebra o st.markdown)."""
    return html.escape(re.sub(r"\s*[\r\n]+\s*", " ", str(txt or "")), quote=True)


def fmt_num(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def kpi(label: str, valor, cor: str = COR_ALERTA, sub: str = "") -> None:
    sub_html = f'<div class="fg-kpi-s">{e(sub)}</div>' if sub else ""
    st.markdown(
        f'<div class="fg-kpi" style="--fg-cor:{cor}"><div class="fg-kpi-l">{e(label)}</div>'
        f'<div class="fg-kpi-v">{e(valor)}</div>{sub_html}</div>',
        unsafe_allow_html=True,
    )


def html_card(row) -> str:
    score = row.get("Score")
    nivel = dou.nivel_relevancia(score)
    cor = COR_NIVEL[nivel]

    titulo = e(row.get("Título", "")) or "(sem título)"
    if row.get("Link"):
        titulo = f'<a href="{e(row["Link"])}" target="_blank" title="Abrir no Diário Oficial">{titulo} ↗</a>'

    selo = f'<span class="fg-badge" style="background:{cor}">{nivel.upper()} · {e(score)}</span>' if score is not None else ""
    tipo = f'<span class="fg-tipo">{e(row.get("Tipo"))}</span>' if row.get("Tipo") else ""
    pag = f' · pág. {e(row.get("Página"))}' if row.get("Página") else ""
    sec = f'<span class="fg-sec">{e(row.get("Seção"))}{pag} · {e(row.get("Data"))}</span>'

    ementa = str(row.get("Ementa/Resumo") or "")
    ementa_html = ""
    if ementa:
        if len(ementa) > EMENTA_CURTA:
            ementa_html = (f'<details class="fg-em"><summary>{e(ementa[:EMENTA_CURTA])}… <b>ver mais</b></summary>'
                           f'{e(ementa)}</details>')
        else:
            ementa_html = f'<div class="fg-em">{e(ementa)}</div>'

    chips = ""
    motivos = [m.strip() for m in str(row.get("Motivos") or "").split(",") if m.strip()]
    if motivos:
        itens = []
        for m in motivos:
            if m.startswith("match:"):
                itens.append(f'<span class="fg-chip fg-chip-a">🔔 {e(m[6:])}</span>')
            elif f"match:{m}" not in motivos:
                itens.append(f'<span class="fg-chip">{e(m)}</span>')
        chips = f'<div class="fg-chips">{"".join(itens)}</div>'

    orgao = dou.orgao_curto(row.get("Órgão", ""))
    org_html = f'<div class="fg-org" title="{e(row.get("Órgão"))}">🏛️ {e(orgao)}</div>' if orgao else ""

    return (f'<div class="fg-card" style="--fg-cor:{cor}"><div class="fg-top">{selo}{tipo}{sec}</div>'
            f'<div class="fg-tit">{titulo}</div>{org_html}{ementa_html}{chips}</div>')


def card_generico(score, titulo: str, link: str, tipo: str, meta: str, linhas: List[str],
                  texto: str = "", chips: List[str] = ()) -> str:
    """Card no mesmo padrao do DOU, para outras fontes (contratos, dispensas...)."""
    nivel = dou.nivel_relevancia(score)
    cor = COR_NIVEL[nivel]
    tit = e(titulo) or "(sem título)"
    if link:
        tit = f'<a href="{e(link)}" target="_blank" title="Abrir na fonte oficial">{tit} ↗</a>'
    selo = f'<span class="fg-badge" style="background:{cor}">{nivel.upper()} · {e(score)}</span>'
    tipo_html = f'<span class="fg-tipo">{e(tipo)}</span>' if tipo else ""
    linhas_html = "".join(f'<div class="fg-org">{e(x)}</div>' for x in linhas if x)
    texto_html = ""
    if texto:
        if len(texto) > EMENTA_CURTA:
            texto_html = (f'<details class="fg-em"><summary>{e(texto[:EMENTA_CURTA])}… <b>ver mais</b></summary>'
                          f'{e(texto)}</details>')
        else:
            texto_html = f'<div class="fg-em">{e(texto)}</div>'
    chips_html = ""
    if chips:
        chips_html = '<div class="fg-chips">' + "".join(f'<span class="fg-chip fg-chip-w">⚠️ {e(c)}</span>' for c in chips) + "</div>"
    return (f'<div class="fg-card" style="--fg-cor:{cor}"><div class="fg-top">{selo}{tipo_html}'
            f'<span class="fg-sec">{e(meta)}</span></div><div class="fg-tit">{tit}</div>'
            f'{linhas_html}{texto_html}{chips_html}</div>')


def render_cards(df: pd.DataFrame, limite: int, key: str, card=html_card) -> None:
    """Mostra `limite` cards e um botao para ver mais. `card(row)` devolve o HTML."""
    if df.empty:
        return
    mostrar = st.session_state.get(f"{key}_n", limite)
    for _, row in df.head(mostrar).iterrows():
        st.markdown(card(row), unsafe_allow_html=True)
    if len(df) > mostrar:
        if st.button(f"Mostrar mais ({len(df) - mostrar} restantes)", key=f"{key}_mais"):
            st.session_state[f"{key}_n"] = mostrar + limite
            st.rerun()


def escolha_unica(label: str, opcoes: List[str], default: str, key: str) -> str:
    if hasattr(st, "segmented_control"):
        v = st.segmented_control(label, opcoes, default=default, key=key, label_visibility="collapsed")
        return v or default
    return st.radio(label, opcoes, index=opcoes.index(default), key=key, horizontal=True, label_visibility="collapsed")


def escolha_multipla(label: str, opcoes: List[str], key: str) -> List[str]:
    if hasattr(st, "pills"):
        return st.pills(label, opcoes, selection_mode="multi", default=opcoes, key=key) or []
    return st.multiselect(label, opcoes, default=opcoes, key=key)


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";").encode("utf-8-sig")


def to_xlsx_bytes(df: pd.DataFrame, sheet_name: str = "FiscalizaGov") -> bytes:
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name[:31])
        ws = writer.sheets[sheet_name[:31]]
        for i, col in enumerate(df.columns):
            largura = max([len(str(col))] + [len(str(v)) for v in df[col].astype(str).head(200)])
            ws.set_column(i, i, min(largura + 2, 65))
    out.seek(0)
    return out.read()


def botoes_download(df: pd.DataFrame, nome: str, key: str) -> None:
    c1, c2 = st.columns(2)
    c1.download_button("⬇️ Excel", to_xlsx_bytes(df, "DOU"), file_name=f"{nome}.xlsx", key=f"{key}_x",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True)
    c2.download_button("⬇️ CSV", to_csv_bytes(df), file_name=f"{nome}.csv", key=f"{key}_c",
                       mime="text/csv", use_container_width=True)


def fmt_data(d: dt.date) -> str:
    return d.strftime("%d/%m/%Y")
