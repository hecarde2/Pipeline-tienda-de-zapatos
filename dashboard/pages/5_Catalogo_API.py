"""Página de catálogo API: benchmark del catálogo consumido de una API externa.

Muestra `gold_catalogo_externo`, generado a partir del catálogo ingerido de la
API pública (DummyJSON) en Bronze/Silver.
"""

import streamlit as st

st.set_page_config(page_title="Zapatos BI | Catálogo API", layout="wide",
                   initial_sidebar_state="expanded")

import sys  # noqa: E402
from pathlib import Path  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import (  # noqa: E402
    _df,
    con_refresco,
    descarga_csv,
    inicio_pagina,
    kpi,
    section,
)


@con_refresco
def panel() -> None:
    inicio_pagina("Catálogo API", "benchmark del catálogo externo")

    cat = _df("gold_catalogo_externo")
    if cat is None or cat.empty:
        st.info(
            "Sin datos del catálogo externo. Habilita el consumo de la API "
            "(`API_ENABLED=1`) y ejecuta el batch con salida a Internet: "
            "`docker compose --profile tools run --rm batch`."
        )
        return

    total_productos = int(cat["productos"].sum())
    precio_medio = float((cat["precio_medio"] * cat["productos"]).sum() / total_productos)
    total_marcas = int(cat["marca"].nunique())

    # --- KPIs --------------------------------------------------------------
    k1, k2, k3 = st.columns(3)
    with k1:
        kpi("Productos externos", f"{total_productos}", "catálogo de la API")
    with k2:
        kpi("Precio medio", f"{precio_medio:.2f} EUR", "ponderado por producto")
    with k3:
        kpi("Marcas", str(total_marcas), "distintas en el catálogo")

    # --- Benchmark por categoría/marca -------------------------------------
    section("Benchmark por categoría y marca", "precios y stock del catálogo externo")

    c1, c2 = st.columns([3, 2])
    with c1:
        with st.container(border=True):
            vis = cat.copy()
            vis.columns = ["Categoría origen", "Categoría", "Marca", "Productos",
                           "Precio medio", "Precio mín.", "Precio máx.",
                           "Rating medio", "Stock total"]
            for col in ("Precio medio", "Precio mín.", "Precio máx."):
                vis[col] = vis[col].map(lambda x: f"{x:.2f} EUR")
            st.dataframe(vis, width="stretch", hide_index=True)
            descarga_csv(cat, "catalogo_externo", "Descargar benchmark (CSV)")
    with c2:
        with st.container(border=True):
            section("Precio medio por marca")
            por_marca = (
                cat.assign(_ingresos=cat["precio_medio"] * cat["productos"])
                .groupby("marca", as_index=False)[["_ingresos", "productos"]].sum()
            )
            por_marca["precio_medio"] = (
                por_marca["_ingresos"] / por_marca["productos"]
            )
            por_marca = por_marca.sort_values("precio_medio", ascending=False)
            st.bar_chart(por_marca.set_index("marca")["precio_medio"], height=280,
                         color="#B45309")

    st.markdown(
        '<div class="footer">Catálogo consumido de la API externa mediante '
        "<code>app/api.py</code> (requests + reintentos + paginación) &middot; "
        "Bronze &rarr; Silver &rarr; Gold</div>",
        unsafe_allow_html=True,
    )


panel()
