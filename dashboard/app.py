"""Página de inicio del dashboard.

KPIs globales, evolución de ventas (con filtro de fechas y exportación),
resumen de top productos y últimas ejecuciones del batch (runs.json).
"""

import streamlit as st

st.set_page_config(page_title="Zapatos BI | Inicio", layout="wide",
                   initial_sidebar_state="expanded")

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import (  # noqa: E402
    REFRESH_SECONDS,
    _df,
    cargar_runs,
    con_refresco,
    descarga_csv,
    filtro_fechas,
    fmt_eur,
    inicio_pagina,
    kpi,
    section,
    tabla,
)


@con_refresco
def panel() -> None:
    inicio_pagina("Tienda de zapatos", "Panel de metricas")

    ventas = _df("gold_ventas_diarias")
    top = _df("gold_top_productos")
    stock = _df("gold_stock_critico")

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

    # --- Evolución con filtro + export -------------------------------------
    section("Evolucion de ventas", "ingresos diarios y unidades · usa el selector "
            "para acotar el rango")
    sel = filtro_fechas(ventas, col="fecha", key="fechas_inicio")
    if sel is not None and not sel.empty:
        c1, c2 = st.columns([3, 2])
        with c1:
            with st.container(border=True):
                serie = sel.sort_values("fecha").set_index("fecha")
                st.line_chart(serie[["ingresos"]], height=250, color="#B45309")
                st.bar_chart(serie[["unidades_vendidas"]], height=190, color="#0F766E")
        with c2:
            with st.container(border=True):
                section("Resumen del rango")
                k1b, k2b = st.columns(2)
                with k1b:
                    kpi("Ingresos", fmt_eur(float(sel["ingresos"].sum())),
                        "rango seleccionado")
                with k2b:
                    kpi("Unidades",
                        f"{int(sel['unidades_vendidas'].sum()):,}".replace(",", "."),
                        "rango seleccionado")
                descarga_csv(
                    sel.sort_values("fecha")[
                        ["fecha", "ingresos", "unidades_vendidas"]
                    ],
                    "ventas_rango",
                    "Descargar ventas del rango (CSV)",
                )

    # --- Top productos (resumen) -------------------------------------------
    section("Top productos", "los 5 con mas ingresos")
    tabla(
        top.sort_values("ingresos", ascending=False).head(5)
        [["nombre", "marca", "categoria", "unidades", "ingresos"]].copy()
        if top is not None and not top.empty else None,
        "Sin datos de productos todavia.",
    )

    # --- Últimas ejecuciones del batch --------------------------------------
    section("Ultimas ejecuciones del batch", "registro en lake/_metadata/runs.json")
    runs = cargar_runs()
    if runs is None:
        st.info(
            "Sin ejecuciones registradas todavia. Ejecuta `./start` o "
            "`docker compose --profile tools run --rm batch` y vuelve a cargar."
        )
    else:
        df = runs.copy()
        for col_dict in ("silver", "gold"):
            if col_dict in df.columns:
                df[col_dict] = df[col_dict].map(
                    lambda v: int(sum(v.values())) if isinstance(v, dict) else None
                )
        df = df.rename(columns={
            "inicio": "Inicio",
            "fin": "Fin",
            "duracion_s": "Duracion (s)",
            "estado": "Estado",
            "bronze": "Filas Bronze",
            "silver": "Filas Silver",
            "gold": "Filas Gold",
            "error": "Error",
        })
        cols = [c for c in ["Inicio", "Fin", "Duracion (s)", "Estado",
                            "Filas Bronze", "Filas Silver", "Filas Gold", "Error"]
                if c in df.columns]
        vis = df[cols].sort_values("Inicio", ascending=False).head(8)
        colores = {"ok": "color: #0F766E; font-weight: 700;",
                   "error": "color: #B91C1C; font-weight: 700;"}
        if "Estado" in vis.columns:
            styler = vis.style.map(lambda v: colores.get(v, ""), subset=["Estado"])
            st.dataframe(styler, width="stretch", hide_index=True)
        else:
            st.dataframe(vis, width="stretch", hide_index=True)
        n_err = int((df["Estado"] == "error").sum()) if "Estado" in df.columns else 0
        if n_err:
            st.warning(f"{n_err} ejecuciones con error en el historial.")

    st.markdown(
        '<div class="footer">Pipeline de datos &middot; PostgreSQL + archivos '
        "&rarr; Bronze &rarr; Silver &rarr; Gold &middot; Kafka &rarr; Spark "
        "streaming &middot; dashboard actualizado cada "
        f"{REFRESH_SECONDS} s</div>",
        unsafe_allow_html=True,
    )


panel()
