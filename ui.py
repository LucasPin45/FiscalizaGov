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
.main .block-container{max-width:1200px;padding-left:2rem;padding-right:2rem}
.stTabs [data-baseweb="tab-list"]{overflow-x:auto;overflow-y:hidden;flex-wrap:nowrap;scrollbar-width:thin;padding-bottom:5px}
.stTabs [data-baseweb="tab"]{white-space:nowrap;flex-shrink:0}
.fg-kpi{border:1px solid rgba(128,128,128,.25);border-left:5px solid var(--fg-cor);border-radius:10px;
        padding:12px 16px;margin-bottom:10px;background:rgba(128,128,128,.05);min-height:92px}
.fg-kpi-l{font-size:.82rem;opacity:.7}
.fg-kpi-v{font-size:1.8rem;font-weight:700;line-height:1.25}
.fg-kpi-s{font-size:.75rem;opacity:.65}
.fg-card{border:1px solid rgba(128,128,128,.25);border-left:4px solid var(--fg-cor);
         border-radius:8px;padding:10px 14px;margin:0 0 8px;background:rgba(128,128,128,.05)}
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
.fg-chip{font-size:11px;padding:0 8px;border-radius:999px;background:rgba(128,128,128,.18)}
.fg-chip-a{background:rgba(26,110,245,.18);color:#1a6ef5;font-weight:600}
.fg-dia{font-size:1.1rem;font-weight:700;margin:1rem 0 .4rem}
</style>
"""


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


def render_cards(df: pd.DataFrame, limite: int, key: str) -> None:
    """Mostra `limite` cards e um botao para ver mais."""
    if df.empty:
        return
    mostrar = st.session_state.get(f"{key}_n", limite)
    for _, row in df.head(mostrar).iterrows():
        st.markdown(html_card(row), unsafe_allow_html=True)
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
