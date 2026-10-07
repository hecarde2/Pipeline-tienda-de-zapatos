"""Página de clientes: valor por cliente y carritos abandonados."""

import html
import streamlit as st

st.set_page_config(page_title="Zapatos BI | Clientes", layout="wide",
                   initial_sidebar_state="expanded")

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import (  # noqa: E402
    _df,
    con_refresco,
    descarga_csv,
    fmt_eur,
    inicio_pagina,
    section,
    tabla,
)


def tarjeta(label: str, valor: str, nota: str = "segmento seleccionado") -> None:
    st.markdown(
        '<div class="kpi">'
        f'<div class="kpi-label">{html.escape(label)}</div>'
        f'<div class="kpi-value kpi-value-sm">{html.escape(valor)}</div>'
        f'<div class="kpi-note">{html.escape(nota)}</div></div>',
        unsafe_allow_html=True,
    )


@con_refresco
def panel() -> None:
    inicio_pagina("Clientes", "valor y carritos abandonados")

    clientes = _df("gold_clientes_valor")
    carritos = _df("gold_carritos_abandonados")

    # --- Clientes mas valiosos ---------------------------------------------
    section("Clientes mas valiosos", "frecuencia, gasto y ticket medio")
    if clientes is None or clientes.empty:
        st.info("Sin compras todavia.")
    else:
        segmentos = st.multiselect(
            "Segmentos", sorted(clientes["segmento"].dropna().unique()),
            default=sorted(clientes["segmento"].dropna().unique()),
            key="segmentos",
        )
        vis = clientes[clientes["segmento"].isin(segmentos)]

        k1, k2, k3 = st.columns(3)
        with k1:
            tarjeta("Clientes", f"{len(vis):,}".replace(",", "."))
        with k2:
            tarjeta("Gasto total", fmt_eur(float(vis["gasto_total"].sum())))
        with k3:
            ticket = float(vis["gasto_total"].sum()) / max(int(vis["pedidos"].sum()), 1)
            tarjeta("Ticket medio", fmt_eur(ticket), "gasto por pedido")

        c1, c2 = st.columns([2, 1])
        with c1:
            with st.container(border=True):
                mostrar = vis.sort_values("gasto_total", ascending=False).head(15)[
                    ["nombre", "ciudad", "pedidos", "gasto_total",
                     "ticket_promedio", "segmento"]
                ].copy()
                mostrar["gasto_total"] = mostrar["gasto_total"].map(fmt_eur)
                mostrar["ticket_promedio"] = mostrar["ticket_promedio"].map(fmt_eur)
                mostrar.columns = ["Cliente", "Ciudad", "Pedidos", "Gasto",
                                   "Ticket medio", "Segmento"]
                st.dataframe(mostrar, width="stretch", hide_index=True)
                descarga_csv(vis.sort_values("gasto_total", ascending=False),
                             "clientes_valor", "Descargar clientes (CSV)")
        with c2:
            with st.container(border=True):
                section("Gasto por segmento")
                if not vis.empty:
                    por_seg = (vis.groupby("segmento", as_index=False)
                               ["gasto_total"].sum())
                    st.bar_chart(por_seg.set_index("segmento")["gasto_total"],
                                 height=280, color="#B45309")
                else:
                    st.info("Sin clientes en los segmentos elegidos.")

    # --- Carritos abandonados ----------------------------------------------
    section("Carritos abandonados", "productos en carrito sin compra")
    with st.container(border=True):
        if tabla(carritos.head(15) if carritos is not None else None,
                 "Sin carritos abandonados todavia."):
            descarga_csv(carritos, "carritos_abandonados",
                         "Descargar carritos (CSV)")


panel()
