"""Dashboard de la tienda de zapatos (Streamlit).

Lee las tablas GOLD del data lake (Parquet) y muestra:
  - KPIs de ventas (ingresos, unidades, ticket medio)
  - Evolución de ventas diarias y top productos
  - Ventas por talla y color
  - Embudo de conversión y carritos abandonados
  - Stock crítico, tasas de devolución y clientes más valiosos
  - Actividad del streaming y alertas de stock en vivo

Se actualiza solo cada REFRESH_SECONDS (default 30 s, AUTO_REFRESH=0 para
desactivarlo) o con el botón "Actualizar datos".
"""

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
    page_icon="👟",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Carga de datos
# ---------------------------------------------------------------------------

@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def cargar(tabla: str) -> pd.DataFrame | None:
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
    return f"{x:,.0f} €".replace(",", ".")


def tabla_o_mensaje(df, mensaje: str) -> bool:
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
    st.title("👟 Tienda de zapatos — Panel de métricas")
    st.caption(
        "Arquitectura Medallion (Bronze → Silver → Gold) con Spark batch + streaming · "
        f"datos en `{GOLD}` · lectura: {time.strftime('%H:%M:%S')}"
    )
    if st.button("🔄 Actualizar datos"):
        st.cache_data.clear()

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

    if ventas is None or ventas.empty:
        st.error(
            "Aún no hay datos Gold. Ejecuta el pipeline batch: "
            "`docker compose --profile tools run --rm seed` y después "
            "`docker compose --profile tools run --rm batch` (o `./start`)."
        )
        return

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Ingresos", fmt_eur(float(ventas["ingresos"].sum())))
    k2.metric("Unidades vendidas",
              f"{int(ventas['unidades_vendidas'].sum()):,}".replace(",", "."))
    k3.metric("Días con ventas", int(len(ventas)))
    if top is not None and not top.empty:
        k4.metric("Producto estrella",
                  str(top.sort_values("ingresos", ascending=False).iloc[0]["nombre"]))
    else:
        k4.metric("Producto estrella", "—")
    k5.metric("Stock crítico", 0 if stock is None else int(len(stock)))

    st.divider()

    # --- Ventas ------------------------------------------------------------
    c1, c2 = st.columns([3, 2])
    with c1:
        st.subheader("Evolución de ingresos diarios")
        serie = ventas.sort_values("fecha").set_index("fecha")
        st.line_chart(serie["ingresos"], height=260)
        st.line_chart(serie["unidades_vendidas"], height=200)
    with c2:
        st.subheader("Ingresos por mes")
        por_mes = (
            ventas.assign(mes=pd.to_datetime(ventas["fecha"]).dt.to_period("M").astype(str))
            .groupby("mes", as_index=False)[["ingresos", "unidades_vendidas"]].sum()
        )
        st.bar_chart(por_mes.set_index("mes")["ingresos"], height=300)

    st.divider()

    # --- Productos y tallas ------------------------------------------------
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top productos por ingresos")
        if top is not None and not top.empty:
            mostrar = (
                top.sort_values("ingresos", ascending=False).head(10)
                [["nombre", "marca", "categoria", "unidades", "ingresos"]].copy()
            )
            mostrar["ingresos"] = mostrar["ingresos"].map(fmt_eur)
            st.dataframe(mostrar, width="stretch", hide_index=True)
            por_categoria = top.groupby("categoria", as_index=False)[["ingresos"]].sum()
            st.bar_chart(por_categoria.set_index("categoria")["ingresos"], height=200)
        else:
            st.info("Sin datos de productos todavía.")
    with c2:
        st.subheader("Demanda por talla y color")
        if tallas is not None and not tallas.empty:
            ts = tallas[tallas["tipo"] == "talla"].sort_values("unidades", ascending=False)
            st.bar_chart(ts.set_index("valor")["unidades"], height=220)
            cs = tallas[tallas["tipo"] == "color"].sort_values("unidades", ascending=False)
            st.bar_chart(cs.set_index("valor")["unidades"], height=220)
        else:
            st.info("Sin datos de tallas todavía.")

    st.divider()

    # --- Conversión --------------------------------------------------------
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Embudo de conversión")
        if embudo is not None and not embudo.empty:
            ev = embudo.sort_values("orden")
            st.bar_chart(ev.set_index("etapa_nombre")["sesiones"], height=260)
            tbl = ev[["etapa_nombre", "sesiones", "eventos", "tasa_sobre_vistas"]].copy()
            tbl["tasa_sobre_vistas"] = (tbl["tasa_sobre_vistas"] * 100).round(1).astype(str) + " %"
            tbl.columns = ["Etapa", "Sesiones", "Eventos", "% sobre vistas"]
            st.dataframe(tbl, width="stretch", hide_index=True)
        else:
            st.info("Sin eventos de navegación todavía.")
    with c2:
        st.subheader("Carritos abandonados (por producto)")
        tabla_o_mensaje(
            carritos.head(10) if carritos is not None else None,
            "Sin carritos abandonados todavía.",
        )

    st.divider()

    # --- Operaciones -------------------------------------------------------
    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("⚠️ Stock crítico")
        if stock is not None and not stock.empty:
            st.dataframe(
                stock[["sku", "nombre", "talla", "color", "stock_disponible", "bodega"]]
                .head(15),
                width="stretch", hide_index=True,
            )
        else:
            st.success("Ninguna variante bajo el umbral.")
    with c2:
        st.subheader("Tasa de devolución")
        if devol is not None and not devol.empty:
            tbl = devol.head(10)[["nombre", "unidades_vendidas", "devoluciones",
                                  "tasa_devolucion", "motivo_mas_frecuente"]].copy()
            tbl["tasa_devolucion"] = (tbl["tasa_devolucion"].fillna(0) * 100).round(1)
            tbl.columns = ["Producto", "Vendidas", "Devoluciones", "% dev.", "Motivo top"]
            st.dataframe(tbl, width="stretch", hide_index=True)
        else:
            st.info("Sin devoluciones todavía.")
    with c3:
        st.subheader("Clientes más valiosos")
        if clientes is not None and not clientes.empty:
            tbl = clientes.head(10)[["nombre", "ciudad", "pedidos", "gasto_total",
                                     "ticket_promedio", "segmento"]].copy()
            tbl["gasto_total"] = tbl["gasto_total"].map(fmt_eur)
            tbl["ticket_promedio"] = tbl["ticket_promedio"].map(fmt_eur)
            tbl.columns = ["Cliente", "Ciudad", "Pedidos", "Gasto", "Ticket medio", "Segmento"]
            st.dataframe(tbl, width="stretch", hide_index=True)
        else:
            st.info("Sin compras todavía.")

    st.divider()

    # --- Streaming en vivo -------------------------------------------------
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📡 Actividad streaming (eventos por tipo)")
        tabla_o_mensaje(
            actividad.sort_values("eventos", ascending=False).head(10)
            if actividad is not None else None,
            "Sin actividad de streaming todavía (el servicio `streaming` la generará "
            "en cuanto lleguen eventos de Kafka).",
        )
    with c2:
        st.subheader("🚨 Alertas de stock (en vivo)")
        if alertas is not None and not alertas.empty:
            st.dataframe(
                alertas.sort_values("ts", ascending=False)
                .head(10)[["ts", "sku", "stock_actual", "umbral", "severidad"]],
                width="stretch", hide_index=True,
            )
            agotados = int((alertas["severidad"] == "agotado").sum())
            if agotados:
                st.error(f"{agotados} alertas de producto agotado.")
        else:
            st.info("Sin alertas todavía.")


panel()
