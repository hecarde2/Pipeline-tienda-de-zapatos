"""Página de ventas: evolución diaria y mensual, embudo de conversión."""

import streamlit as st

st.set_page_config(page_title="Zapatos BI | Ventas", layout="wide",
                   initial_sidebar_state="expanded")

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

import pandas as pd  # noqa: E402

from lib import (  # noqa: E402
    _df,
    con_refresco,
    descarga_csv,
    filtro_fechas,
    fmt_eur,
    inicio_pagina,
    kpi,
    section,
)


@con_refresco
def panel() -> None:
    inicio_pagina("Ventas", "evolucion y conversion")

    ventas = _df("gold_ventas_diarias")
    embudo = _df("gold_embudo_conversion")

    if ventas is None or ventas.empty:
        st.error("Sin datos de ventas todavia: ejecuta el batch (`./start`).")
        return

    section("Filtro", "acota todas las graficas de esta pagina")
    sel = filtro_fechas(ventas, col="fecha", key="fechas_ventas")
    sel = ventas if sel is None else sel

    # --- Resumen del rango --------------------------------------------------
    k1, k2, k3 = st.columns(3)
    with k1:
        kpi("Ingresos", fmt_eur(float(sel["ingresos"].sum())), "rango seleccionado")
    with k2:
        kpi("Unidades",
            f"{int(sel['unidades_vendidas'].sum()):,}".replace(",", "."),
            "rango seleccionado")
    with k3:
        kpi("Ticket medio/dia",
            fmt_eur(float(sel["ingresos"].sum()) / max(len(sel), 1)),
            "ingresos por dia con ventas")

    # --- Diario -------------------------------------------------------------
    section("Evolucion diaria", "ingresos y unidades")
    c1, c2 = st.columns([3, 2])
    with c1:
        with st.container(border=True):
            serie = sel.sort_values("fecha").set_index("fecha")
            st.line_chart(serie[["ingresos"]], height=250, color="#B45309")
            st.bar_chart(serie[["unidades_vendidas"]], height=190, color="#0F766E")
    with c2:
        with st.container(border=True):
            section("Ingresos por mes")
            por_mes = (
                sel.assign(mes=pd.to_datetime(sel["fecha"])
                           .dt.to_period("M").astype(str))
                .groupby("mes", as_index=False)[["ingresos", "unidades_vendidas"]].sum()
            )
            st.bar_chart(por_mes.set_index("mes")["ingresos"], height=330,
                         color="#B45309")

    descarga_csv(
        sel.sort_values("fecha")[["fecha", "ingresos", "unidades_vendidas"]],
        "ventas_diarias",
        "Descargar serie diaria (CSV)",
    )

    # --- Embudo -------------------------------------------------------------
    section("Embudo de conversion", "de la vista a la compra")
    with st.container(border=True):
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
            descarga_csv(
                ev[["etapa_nombre", "sesiones", "eventos", "tasa_sobre_vistas"]],
                "embudo_conversion",
                "Descargar embudo (CSV)",
            )
        else:
            st.info("Sin eventos de navegacion todavia.")


panel()
