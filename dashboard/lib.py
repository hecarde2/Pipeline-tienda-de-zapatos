"""Utilidades compartidas de las páginas del dashboard.

Estilo (blanco hueso + acento cobre), carga de datos Gold, filtros y
exportación a CSV. Ninguna página debe tumbarse si falta una tabla: se
muestra un aviso y se continúa.
"""

import html
import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit.components.v1 import html as components_html

LAKE = Path(os.environ.get("LAKE_ROOT", "/data/lake"))
GOLD = LAKE / "gold"
METADATA = LAKE / "_metadata"

AUTO_REFRESH = os.environ.get("AUTO_REFRESH", "1") == "1"
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "30"))

CSS = """
<style>
:root {
  --bone:      #F6F3EC;
  --surface:   #FFFFFF;
  --line:      #E6E0D2;
  --ink:       #1C1917;
  --muted:     #78716C;
  --accent:    #B45309;
  --accent-2:  #FEF3C7;
  --teal:      #0F766E;
  --danger:    #B91C1C;
}

.stApp {
  background-color: var(--bone);
  color: var(--ink);
  font-family: "Inter", "Segoe UI", system-ui, -apple-system, sans-serif;
}

header[data-testid="stHeader"] { background: transparent; }

section[data-testid="stSidebar"] {
  background: var(--surface);
  border-right: 1px solid var(--line);
}

/* --- Cabecera ------------------------------------------------------------ */
.hero {
  background: linear-gradient(135deg, #FFFFFF 0%, #FBF9F4 100%);
  border: 1px solid var(--line);
  border-left: 6px solid var(--accent);
  border-radius: 18px;
  padding: 24px 30px;
  box-shadow: 0 2px 6px rgba(28, 25, 23, .06);
  margin-bottom: 6px;
}
.hero h1 {
  font-size: 2.0rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  color: var(--ink);
  margin: 0 0 6px 0;
}
.hero h1 span { color: var(--accent); }
.hero p {
  color: var(--muted);
  font-size: 0.95rem;
  margin: 0;
}
.badge {
  display: inline-block;
  background: var(--accent-2);
  color: var(--accent);
  border: 1px solid #F0D9A8;
  border-radius: 999px;
  padding: 4px 14px;
  font-size: 0.75rem;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
}
.badge-live { background: #ECFDF5; color: var(--teal); border-color: #A7F3D0; }
.badge-live::before {
  content: "";
  display: inline-block;
  width: 7px; height: 7px;
  margin-right: 7px;
  border-radius: 50%;
  background: var(--teal);
  vertical-align: middle;
  animation: pulse 1.6s infinite;
}
@keyframes pulse {
  0%, 100% { opacity: 1; }
  50%      { opacity: .25; }
}

/* --- Tarjetas KPI -------------------------------------------------------- */
.kpi {
  background: var(--surface);
  border: 1px solid var(--line);
  border-top: 4px solid var(--accent);
  border-radius: 16px;
  padding: 18px 20px 16px 20px;
  box-shadow: 0 2px 6px rgba(28, 25, 23, .06);
  min-height: 118px;
}
.kpi-label {
  font-size: 0.7rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 8px;
}
.kpi-value {
  font-size: 1.7rem;
  font-weight: 800;
  color: var(--ink);
  line-height: 1.15;
  word-break: break-word;
}
.kpi-value.kpi-value-sm { font-size: 1.15rem; }
.kpi-note {
  font-size: 0.78rem;
  color: var(--muted);
  margin-top: 6px;
}
.kpi-alert { border-top-color: var(--danger); }
.kpi-alert .kpi-value { color: var(--danger); }

/* --- Secciones ----------------------------------------------------------- */
.sec {
  font-size: 1.05rem;
  font-weight: 800;
  color: var(--ink);
  letter-spacing: -0.01em;
  border-left: 4px solid var(--accent);
  padding-left: 12px;
  margin: 4px 0 10px 0;
}
.sec em {
  display: block;
  font-size: 0.75rem;
  font-weight: 500;
  font-style: normal;
  color: var(--muted);
  letter-spacing: 0;
  margin-top: 2px;
}

/* --- Contenedores con borde (tarjetas de seccion) ------------------------ */
[data-testid="stVerticalBlockBorderWrapper"] {
  background: var(--surface);
  border: 1px solid var(--line) !important;
  border-radius: 16px !important;
  box-shadow: 0 2px 6px rgba(28, 25, 23, .05);
}

/* --- Graficos y tablas --------------------------------------------------- */
[data-testid="stVegaLiteChart"],
[data-testid="stLineChart"],
[data-testid="stBarChart"] { background: transparent; }

[data-testid="stDataFrame"],
[data-testid="stArrowTable"] {
  border: 1px solid var(--line);
  border-radius: 12px;
  overflow: hidden;
}

/* --- Botones ------------------------------------------------------------- */
.stButton > button {
  background: var(--ink);
  color: #FFFFFF;
  border: none;
  border-radius: 10px;
  font-weight: 600;
  padding: 8px 18px;
  transition: background .15s ease;
}
.stButton > button:hover {
  background: var(--accent);
  color: #FFFFFF;
  border: none;
}

/* --- Filtros (date_input / multiselect) ---------------------------------- */
[data-testid="stDateInput"],
[data-testid="stMultiSelect"] {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 10px;
}

/* --- Mensajes ------------------------------------------------------------ */
[data-testid="stInfo"]    { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--teal);  border-radius: 10px; }
[data-testid="stSuccess"] { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--teal);  border-radius: 10px; }
[data-testid="stWarning"] { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--accent);  border-radius: 10px; }
[data-testid="stError"]   { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--danger);  border-radius: 10px; }

/* --- Separadores y titulos internos -------------------------------------- */
hr { border-color: var(--line); }
h2, h3 { color: var(--ink); letter-spacing: -0.01em; }

/* --- Sidebar permanente --------------------------------------------------- */
@media (min-width: 768px) {
  /* En escritorio el menu lateral no se puede contraer */
  [data-testid="stSidebarCollapseButton"] { display: none !important; }
}

/* --- Footer -------------------------------------------------------------- */
.footer {
  color: var(--muted);
  font-size: 0.78rem;
  text-align: center;
  padding: 14px 0 6px 0;
  border-top: 1px solid var(--line);
  margin-top: 10px;
}
</style>
"""


def con_refresco(func):
    """Refresco automático de la página cada REFRESH_SECONDS."""
    return st.fragment(run_every=REFRESH_SECONDS if AUTO_REFRESH else None)(func)


# Si el navegador guardó "sidebar contraido", lo limpia y lo reabre (una vez).
_SIDEBAR_JS = """
<script>
(function () {
  try {
    var p = window.parent;
    Object.keys(p.localStorage).forEach(function (k) {
      if (k.indexOf("stSidebarCollapsed") === 0) p.localStorage.removeItem(k);
    });
    var d = p.document;
    var sb = d.querySelector('[data-testid="stSidebar"]');
    var btn = d.querySelector('[data-testid="stSidebarCollapseButton"]');
    if (sb && btn && sb.getBoundingClientRect().width < 120) btn.click();
  } catch (e) {}
})();
</script>
"""


def _sidebar_permanente() -> None:
    components_html(_SIDEBAR_JS, height=1)


def inicio_pagina(titulo: str, subtitulo: str) -> None:
    """Estilo + cabecera de la página (llamar como primer comando st)."""
    _sidebar_permanente()
    st.markdown(CSS, unsafe_allow_html=True)
    hb1, hb2 = st.columns([5, 1])
    with hb1:
        st.markdown(
            f'<div class="hero">'
            f"<h1>{html.escape(titulo)} <span>&mdash; {html.escape(subtitulo)}</span></h1>"
            f"<p>Arquitectura Medallion (Bronze &rarr; Silver &rarr; Gold) con Spark "
            f"batch + streaming &middot; datos en <b>{html.escape(str(GOLD))}</b> "
            f"&middot; lectura {time.strftime('%H:%M:%S')}</p>"
            "</div>",
            unsafe_allow_html=True,
        )
    with hb2:
        st.markdown(
            '<div style="text-align:right; padding-top:24px;">'
            f'{badge("En vivo", vivo=True)}</div>',
            unsafe_allow_html=True,
        )
        if st.button("Actualizar datos", width="stretch"):
            st.cache_data.clear()
    st.markdown(
        '<div style="margin: 2px 0 14px 0;">'
        + badge("Bronze")
        + "&nbsp;" + badge("Silver")
        + "&nbsp;" + badge("Gold")
        + "&nbsp;" + badge("Streaming")
        + "</div>",
        unsafe_allow_html=True,
    )


def section(titulo: str, nota: str = "") -> None:
    nota_html = f"<em>{html.escape(nota)}</em>" if nota else ""
    st.markdown(
        f'<div class="sec">{html.escape(titulo)}{nota_html}</div>',
        unsafe_allow_html=True,
    )


def kpi(label: str, valor: str, nota: str = "", alerta: bool = False) -> None:
    cls = "kpi kpi-alert" if alerta else "kpi"
    cls_valor = "kpi-value kpi-value-sm" if len(valor) > 16 else "kpi-value"
    st.markdown(
        f'<div class="{cls}">'
        f'<div class="kpi-label">{html.escape(label)}</div>'
        f'<div class="{cls_valor}">{html.escape(valor)}</div>'
        f'<div class="kpi-note">{html.escape(nota)}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


def badge(texto: str, vivo: bool = False) -> str:
    cls = "badge badge-live" if vivo else "badge"
    return f'<span class="{cls}">{html.escape(texto)}</span>'


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def cargar(tabla: str):
    path = GOLD / tabla
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception as exc:  # noqa: BLE001 - el dashboard no debe tumbarse
        return f"ERROR: {exc}"


def _df(tabla: str) -> pd.DataFrame | None:
    res = cargar(tabla)
    if isinstance(res, str):
        st.warning(f"No se pudo leer {tabla}: {res}")
        return None
    return res


@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def cargar_runs() -> pd.DataFrame | None:
    """Últimas ejecuciones del batch (lake/_metadata/runs.json)."""
    ruta = METADATA / "runs.json"
    if not ruta.exists():
        return None
    try:
        df = pd.read_json(ruta)
        return None if df.empty else df
    except Exception:  # noqa: BLE001
        return None


# ---------------------------------------------------------------------------
# Filtros, formato y exportación
# ---------------------------------------------------------------------------

def fmt_eur(x: float) -> str:
    return f"{x:,.0f} EUR".replace(",", ".")


def tabla(df, mensaje: str) -> bool:
    if df is None or df.empty:
        st.info(mensaje)
        return False
    st.dataframe(df, width="stretch", hide_index=True)
    return True


def filtro_fechas(df: pd.DataFrame | None, col: str = "fecha",
                  key: str = "rango_fechas") -> pd.DataFrame | None:
    """Selector de rango de fechas aplicado a la columna `col`."""
    if df is None or df.empty or col not in df.columns:
        return df
    serie = pd.to_datetime(df[col])
    mn, mx = serie.min().date(), serie.max().date()
    sel = st.date_input("Rango de fechas", value=(mn, mx),
                        min_value=mn, max_value=mx, key=key,
                        label_visibility="collapsed")
    if not isinstance(sel, tuple) or len(sel) != 2:
        return df
    mascara = (serie >= pd.Timestamp(sel[0])) & (serie <= pd.Timestamp(sel[1]))
    filtrado = df[mascara]
    if filtrado.empty:
        st.info("Sin datos en el rango seleccionado.")
    return filtrado


def descarga_csv(df: pd.DataFrame | None, nombre: str,
                 etiqueta: str = "Descargar CSV") -> None:
    if df is None or df.empty:
        return
    st.download_button(
        etiqueta,
        df.to_csv(index=False).encode("utf-8"),
        file_name=f"{nombre}.csv",
        mime="text/csv",
        key=f"dl_{nombre}",
    )
