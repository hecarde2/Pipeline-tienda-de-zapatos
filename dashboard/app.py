"""Dashboard de la tienda de zapatos (Streamlit).

Lee las tablas GOLD del data lake (Parquet) y muestra:
  - KPIs de ventas (ingresos, unidades, producto estrella, stock critico)
  - Evolucion de ventas diarias y top productos
  - Ventas por talla y color
  - Embudo de conversion y carritos abandonados
  - Stock critico, tasas de devolucion y clientes mas valiosos
  - Actividad del streaming y alertas de stock en vivo

Diseno: fondo blanco hueso (#F6F3EC), tarjetas blancas con borde suave y
acento cobre (#B45309). Sin emojis.

Se actualiza solo cada REFRESH_SECONDS (default 30 s, AUTO_REFRESH=0 para
desactivarlo) o con el boton "Actualizar datos".
"""

import html
import os
import time
from pathlib import Path

import pandas as pd
import streamlit as st

LAKE = Path(os.environ.get("LAKE_ROOT", "/data/lake"))
GOLD = LAKE / "gold"

AUTO_REFRESH = os.environ.get("AUTO_REFRESH", "1") == "1"
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "30"))

st.set_page_config(
    page_title="Zapatos BI | Pipeline Medallion",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Estilo: blanco hueso + tarjetas + acento cobre
# ---------------------------------------------------------------------------

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
  padding: 26px 30px;
  box-shadow: 0 2px 6px rgba(28, 25, 23, .06);
  margin-bottom: 6px;
}
.hero h1 {
  font-size: 2.1rem;
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

/* --- Mensajes ------------------------------------------------------------ */
[data-testid="stInfo"]    { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--teal);  border-radius: 10px; }
[data-testid="stSuccess"] { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--teal);  border-radius: 10px; }
[data-testid="stWarning"] { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--accent); border-radius: 10px; }
[data-testid="stError"]   { background: #FFFFFF; border: 1px solid var(--line); border-left: 4px solid var(--danger); border-radius: 10px; }

/* --- Separadores y titulos internos -------------------------------------- */
hr { border-color: var(--line); }
h2, h3 { color: var(--ink); letter-spacing: -0.01em; }

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


# ---------------------------------------------------------------------------
# Utilidades de estilo
# ---------------------------------------------------------------------------

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


def fmt_eur(x: float) -> str:
    return f"{x:,.0f} EUR".replace(",", ".")


def tabla(df, mensaje: str) -> bool:
    if df is None or df.empty:
        st.info(mensaje)
        return False
    st.dataframe(df, width="stretch", hide_index=True)
    return True


# ---------------------------------------------------------------------------
# Contenido (fragmento con auto-refresco)
# ---------------------------------------------------------------------------

@st.fragment(run_every=REFRESH_SECONDS if AUTO_REFRESH else None)
def panel() -> None:
    st.markdown(CSS, unsafe_allow_html=True)

    # --- Cabecera ----------------------------------------------------------
    ventas = _df("gold_ventas_diarias")
    top = _df("gold_top_productos")
    tallas = _df("gold_ventas_por_talla")
    embudo = _df("gold_embudo_conversion")
    carritos = _df("gold_carritos_abandonados")
    stock = _df("gold_stock_critico")
    devol = _df("gold_tasa_devolucion")
    clientes = _df("gold_clientes_valor")
    actividad = _df("gold_actividad_stream")
    alertas = _df("gold_alertas")

    hb1, hb2 = st.columns([4, 1])
    with hb1:
        st.markdown(
            '<div class="hero">'
            "<h1>Tienda de zapatos <span>&mdash; Panel de metricas</span></h1>"
            "<p>Arquitectura Medallion (Bronze &rarr; Silver &rarr; Gold) con Spark "
            f"batch + streaming &middot; datos en <b>{html.escape(str(GOLD))}</b> "
            f"&middot; lectura {time.strftime('%H:%M:%S')}</p>"
            "</div>",
            unsafe_allow_html=True,
        )
    with hb2:
        st.markdown(
            f'<div style="text-align:right; padding-top:26px;">'
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

    if ventas is None or ventas.empty:
        st.error(
            "Aun no hay datos Gold. Ejecuta el pipeline batch: "
            "`docker compose --profile tools run --rm seed` y despues "
            "`docker compose --profile tools run --rm batch` (o `./start`)."
        )
        return

    # --- KPIs --------------------------------------------------------------
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        kpi("Ingresos totales", fmt_eur(float(ventas["ingresos"].sum())),
            "ventas confirmadas + historico")
    with k2:
        kpi("Unidades vendidas",
            f"{int(ventas['unidades_vendidas'].sum()):,}".replace(",", "."),
            "todas las fuentes")
    with k3:
        kpi("Dias con ventas", str(int(len(ventas))), "cobertura del calendario")
    with k4:
        if top is not None and not top.empty:
            estrella = str(top.sort_values("ingresos", ascending=False).iloc[0]["nombre"])
            kpi("Producto estrella", estrella, "por ingresos")
        else:
            kpi("Producto estrella", "Sin datos", "ejecuta el batch")
    with k5:
        n_stock = 0 if stock is None else int(len(stock))
        kpi("Stock critico", str(n_stock), "variantes bajo umbral", alerta=n_stock > 0)

    # --- Ventas ------------------------------------------------------------
    section("Evolucion de ventas", "ingresos diarios y unidades")
    c1, c2 = st.columns([3, 2])
    with c1:
        with st.container(border=True):
            serie = ventas.sort_values("fecha").set_index("fecha")
            st.line_chart(serie[["ingresos"]], height=250, color="#B45309")
            st.bar_chart(serie[["unidades_vendidas"]], height=190, color="#0F766E")
    with c2:
        with st.container(border=True):
            section("Ingresos por mes")
            por_mes = (
                ventas.assign(mes=pd.to_datetime(ventas["fecha"]).dt.to_period("M").astype(str))
                .groupby("mes", as_index=False)[["ingresos", "unidades_vendidas"]].sum()
            )
            st.bar_chart(por_mes.set_index("mes")["ingresos"], height=330, color="#B45309")

    # --- Productos y tallas ------------------------------------------------
    section("Catalogo y demanda", "top productos, tallas y colores")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            section("Top productos por ingresos")
            if top is not None and not top.empty:
                mostrar = (
                    top.sort_values("ingresos", ascending=False).head(10)
                    [["nombre", "marca", "categoria", "unidades", "ingresos"]].copy()
                )
                mostrar["ingresos"] = mostrar["ingresos"].map(fmt_eur)
                st.dataframe(mostrar, width="stretch", hide_index=True)
                por_categoria = top.groupby("categoria", as_index=False)[["ingresos"]].sum()
                st.bar_chart(por_categoria.set_index("categoria")["ingresos"],
                             height=200, color="#B45309")
            else:
                st.info("Sin datos de productos todavia.")
    with c2:
        with st.container(border=True):
            section("Demanda por talla y color")
            if tallas is not None and not tallas.empty:
                ts = tallas[tallas["tipo"] == "talla"].sort_values("unidades", ascending=False)
                st.bar_chart(ts.set_index("valor")["unidades"], height=210, color="#B45309")
                cs = tallas[tallas["tipo"] == "color"].sort_values("unidades", ascending=False)
                st.bar_chart(cs.set_index("valor")["unidades"], height=210, color="#0F766E")
            else:
                st.info("Sin datos de tallas todavia.")

    # --- Conversión --------------------------------------------------------
    section("Conversion y carritos", "embudo de navegacion y abandono")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            section("Embudo de conversion")
            if embudo is not None and not embudo.empty:
                ev = embudo.sort_values("orden")
                st.bar_chart(ev.set_index("etapa_nombre")["sesiones"], height=240,
                             color="#B45309")
                tbl = ev[["etapa_nombre", "sesiones", "eventos", "tasa_sobre_vistas"]].copy()
                tbl["tasa_sobre_vistas"] = (
                    (tbl["tasa_sobre_vistas"] * 100).round(1).astype(str) + " %"
                )
                tbl.columns = ["Etapa", "Sesiones", "Eventos", "% sobre vistas"]
                st.dataframe(tbl, width="stretch", hide_index=True)
            else:
                st.info("Sin eventos de navegacion todavia.")
    with c2:
        with st.container(border=True):
            section("Carritos abandonados", "productos sin compra")
            tabla(
                carritos.head(10) if carritos is not None else None,
                "Sin carritos abandonados todavia.",
            )

    # --- Operaciones -------------------------------------------------------
    section("Operaciones", "stock, devoluciones y clientes")
    c1, c2, c3 = st.columns(3)
    with c1:
        with st.container(border=True):
            section("Stock critico", "umbral configurable")
            if stock is not None and not stock.empty:
                st.dataframe(
                    stock[["sku", "nombre", "talla", "color", "stock_disponible", "bodega"]]
                    .head(15),
                    width="stretch", hide_index=True,
                )
            else:
                st.success("Ninguna variante bajo el umbral.")
    with c2:
        with st.container(border=True):
            section("Tasa de devolucion", "por producto")
            if devol is not None and not devol.empty:
                tbl = devol.head(10)[["nombre", "unidades_vendidas", "devoluciones",
                                      "tasa_devolucion", "motivo_mas_frecuente"]].copy()
                tbl["tasa_devolucion"] = (tbl["tasa_devolucion"].fillna(0) * 100).round(1)
                tbl.columns = ["Producto", "Vendidas", "Devoluciones", "% dev.", "Motivo top"]
                st.dataframe(tbl, width="stretch", hide_index=True)
            else:
                st.info("Sin devoluciones todavia.")
    with c3:
        with st.container(border=True):
            section("Clientes mas valiosos", "frecuencia y ticket medio")
            if clientes is not None and not clientes.empty:
                tbl = clientes.head(10)[["nombre", "ciudad", "pedidos", "gasto_total",
                                         "ticket_promedio", "segmento"]].copy()
                tbl["gasto_total"] = tbl["gasto_total"].map(fmt_eur)
                tbl["ticket_promedio"] = tbl["ticket_promedio"].map(fmt_eur)
                tbl.columns = ["Cliente", "Ciudad", "Pedidos", "Gasto",
                               "Ticket medio", "Segmento"]
                st.dataframe(tbl, width="stretch", hide_index=True)
            else:
                st.info("Sin compras todavia.")

    # --- Streaming en vivo -------------------------------------------------
    section("Streaming en vivo", "Kafka -> Spark Structured Streaming -> Gold")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            section("Actividad del stream", "eventos y sesiones por tipo")
            tabla(
                actividad.sort_values("eventos", ascending=False).head(10)
                if actividad is not None else None,
                "Sin actividad de streaming todavia (el servicio `streaming` la "
                "generara en cuanto lleguen eventos de Kafka).",
            )
    with c2:
        with st.container(border=True):
            section("Alertas de stock", "severidad: bajo / critico / agotado")
            if alertas is not None and not alertas.empty:
                vis = (
                    alertas.sort_values("ts", ascending=False)
                    .head(10)[["ts", "sku", "stock_actual", "umbral", "severidad"]].copy()
                )
                colores = {
                    "agotado": "color: #B91C1C; font-weight: 700;",
                    "critico": "color: #B45309; font-weight: 700;",
                    "bajo": "color: #0F766E;",
                }
                styler = vis.style.map(
                    lambda v: colores.get(v, ""),
                    subset=["severidad"],
                )
                st.dataframe(styler, width="stretch", hide_index=True)
                agotados = int((alertas["severidad"] == "agotado").sum())
                if agotados:
                    st.error(f"{agotados} alertas de producto agotado.")
            else:
                st.info("Sin alertas todavia.")

    st.markdown(
        '<div class="footer">Pipeline de datos &middot; PostgreSQL + archivos '
        "&rarr; Bronze &rarr; Silver &rarr; Gold &middot; Kafka &rarr; Spark "
        "streaming &middot; dashboard actualizado cada "
        f"{REFRESH_SECONDS} s</div>",
        unsafe_allow_html=True,
    )


panel()
