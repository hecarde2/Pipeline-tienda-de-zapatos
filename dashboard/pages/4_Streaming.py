"""Página de streaming: actividad en vivo y alertas de stock."""

import streamlit as st

st.set_page_config(page_title="Zapatos BI | Streaming", layout="wide",
                   initial_sidebar_state="expanded")

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import (  # noqa: E402
    _df,
    con_refresco,
    descarga_csv,
    inicio_pagina,
    section,
    tabla,
)


@con_refresco
def panel() -> None:
    inicio_pagina("Streaming", "Kafka a Gold en vivo")

    actividad = _df("gold_actividad_stream")
    alertas = _df("gold_alertas")

    k1, k2 = st.columns(2)
    with k1:
        n_eventos = 0 if actividad is None else int(actividad["eventos"].sum())
        st.markdown(
            '<div class="kpi">'
            '<div class="kpi-label">Eventos agregados</div>'
            f'<div class="kpi-value">{n_eventos:,}</div>'
            '<div class="kpi-note">por tipo de evento</div></div>'.replace(",", "."),
            unsafe_allow_html=True,
        )
    with k2:
        n_alertas = 0 if alertas is None else int(len(alertas))
        cls = "kpi kpi-alert" if n_alertas else "kpi"
        st.markdown(
            f'<div class="{cls}">'
            '<div class="kpi-label">Alertas de stock</div>'
            f'<div class="kpi-value">{n_alertas}</div>'
            '<div class="kpi-note">historial de gold_alertas</div></div>',
            unsafe_allow_html=True,
        )

    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            section("Actividad del stream", "eventos y sesiones por tipo")
            if tabla(
                actividad.sort_values("eventos", ascending=False)
                if actividad is not None else None,
                "Sin actividad de streaming todavia (el servicio `streaming` la "
                "generara en cuanto lleguen eventos de Kafka).",
            ):
                descarga_csv(actividad, "actividad_stream", "Descargar actividad (CSV)")
    with c2:
        with st.container(border=True):
            section("Alertas de stock", "severidad: bajo / critico / agotado")
            if alertas is not None and not alertas.empty:
                vis = (
                    alertas.sort_values("ts", ascending=False)
                    .head(20)[["ts", "sku", "stock_actual", "umbral",
                               "severidad"]].copy()
                )
                colores = {
                    "agotado": "color: #B91C1C; font-weight: 700;",
                    "critico": "color: #B45309; font-weight: 700;",
                    "bajo": "color: #0F766E;",
                }
                styler = vis.style.map(
                    lambda v: colores.get(v, ""), subset=["severidad"]
                )
                st.dataframe(styler, width="stretch", hide_index=True)
                descarga_csv(alertas, "alertas_stock", "Descargar alertas (CSV)")
                agotados = int((alertas["severidad"] == "agotado").sum())
                if agotados:
                    st.error(f"{agotados} alertas de producto agotado.")
            else:
                st.info("Sin alertas todavia.")

    st.markdown(
        '<div class="footer">Kafka (eventos-web) &rarr; Spark Structured '
        f"Streaming &rarr; Gold &middot; refresco cada pagina automatico</div>",
        unsafe_allow_html=True,
    )


panel()
