"""Página de producto: top ventas, demanda por talla/color, stock y devoluciones."""

import streamlit as st

st.set_page_config(page_title="Zapatos BI | Producto", layout="wide",
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
)


@con_refresco
def panel() -> None:
    inicio_pagina("Producto", "catalogo, demanda, stock y devoluciones")

    top = _df("gold_top_productos")
    tallas = _df("gold_ventas_por_talla")
    stock = _df("gold_stock_critico")
    devol = _df("gold_tasa_devolucion")

    # --- Top productos con filtros -----------------------------------------
    section("Top productos", "por ingresos")
    if top is None or top.empty:
        st.info("Sin datos de productos todavia.")
    else:
        f1, f2, f3 = st.columns([2, 2, 1])
        with f1:
            marcas = st.multiselect(
                "Marcas", sorted(top["marca"].dropna().unique()),
                default=sorted(top["marca"].dropna().unique()), key="marcas",
            )
        with f2:
            cats = st.multiselect(
                "Categorias", sorted(top["categoria"].dropna().unique()),
                default=sorted(top["categoria"].dropna().unique()), key="categorias",
            )
        filtrado = top[top["marca"].isin(marcas) & top["categoria"].isin(cats)]
        with f3:
            st.markdown(
                '<div style="padding-top:28px;">'
                f'<span class="badge">{len(filtrado)} productos</span></div>',
                unsafe_allow_html=True,
            )

        c1, c2 = st.columns([3, 2])
        with c1:
            with st.container(border=True):
                mostrar = (
                    filtrado.sort_values("ingresos", ascending=False).head(15)
                    [["nombre", "marca", "categoria", "unidades", "ingresos"]].copy()
                )
                mostrar["ingresos"] = mostrar["ingresos"].map(fmt_eur)
                st.dataframe(mostrar, width="stretch", hide_index=True)
                descarga_csv(
                    filtrado.sort_values("ingresos", ascending=False)[
                        ["nombre", "marca", "categoria", "unidades", "ingresos"]
                    ],
                    "top_productos",
                    "Descargar ranking completo (CSV)",
                )
        with c2:
            with st.container(border=True):
                section("Ingresos por categoria")
                por_categoria = (
                    filtrado.groupby("categoria", as_index=False)["ingresos"].sum()
                )
                st.bar_chart(por_categoria.set_index("categoria")["ingresos"],
                             height=260, color="#B45309")

    # --- Tallas y colores ---------------------------------------------------
    section("Demanda por talla y color", "unidades vendidas")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            if tallas is not None and not tallas.empty:
                ts = (tallas[tallas["tipo"] == "talla"]
                      .sort_values("unidades", ascending=False))
                st.bar_chart(ts.set_index("valor")["unidades"], height=240,
                             color="#B45309")
                descarga_csv(ts, "demanda_tallas", "Descargar tallas (CSV)")
            else:
                st.info("Sin datos de tallas todavia.")
    with c2:
        with st.container(border=True):
            if tallas is not None and not tallas.empty:
                cs = (tallas[tallas["tipo"] == "color"]
                      .sort_values("unidades", ascending=False))
                st.bar_chart(cs.set_index("valor")["unidades"], height=240,
                             color="#0F766E")
                descarga_csv(cs, "demanda_colores", "Descargar colores (CSV)")
            else:
                st.info("Sin datos de colores todavia.")

    # --- Stock y devoluciones ----------------------------------------------
    section("Operaciones", "stock critico y devoluciones")
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            section("Stock critico", "umbral configurable (STOCK_UMBRAL)")
            if stock is not None and not stock.empty:
                vis = stock[["sku", "nombre", "talla", "color",
                             "stock_disponible", "bodega"]]
                st.dataframe(vis, width="stretch", hide_index=True)
                descarga_csv(stock, "stock_critico", "Descargar stock (CSV)")
            else:
                st.success("Ninguna variante bajo el umbral.")
    with c2:
        with st.container(border=True):
            section("Tasa de devolucion", "por producto")
            if devol is not None and not devol.empty:
                tbl = devol[["nombre", "unidades_vendidas", "devoluciones",
                             "tasa_devolucion", "motivo_mas_frecuente"]].copy()
                tbl["tasa_devolucion"] = (tbl["tasa_devolucion"].fillna(0) * 100).round(1)
                tbl.columns = ["Producto", "Vendidas", "Devoluciones", "% dev.",
                               "Motivo top"]
                st.dataframe(tbl, width="stretch", hide_index=True)
                descarga_csv(devol, "tasa_devolucion", "Descargar devoluciones (CSV)")
            else:
                st.info("Sin devoluciones todavia.")


panel()
