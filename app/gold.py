"""Capa GOLD: métricas listas para análisis (alimentan el dashboard).

| gold_tabla                | Qué responde                                    |
|---------------------------|-------------------------------------------------|
| gold_ventas_diarias       | Ingresos por día, semana y mes                  |
| gold_top_productos        | Zapatos más vendidos por marca y categoría      |
| gold_ventas_por_talla     | Tallas y colores con más demanda                |
| gold_embudo_conversion    | Vistas, carrito, checkout y compra              |
| gold_carritos_abandonados | Productos que se quedan en el carrito           |
| gold_stock_critico        | Variantes con poco inventario                   |
| gold_tasa_devolucion      | Productos con más devoluciones y sus motivos    |
| gold_clientes_valor       | Clientes frecuentes, ticket promedio            |
| gold_catalogo_externo     | Benchmark de precios del catálogo de la API     |

Uso:  python -m app.gold
"""

from pyspark.sql import DataFrame, Window, functions as F

from app import config
from app.spark_utils import get_spark


def _read_silver(spark, nombre: str) -> DataFrame:
    path = config.silver_path(nombre)
    if not path.exists():
        raise FileNotFoundError(
            f"Silver/{nombre} no existe. Ejecuta antes: python -m app.silver"
        )
    return spark.read.parquet(config.spath(path))


def _ventas_dataframe(spark) -> DataFrame:
    """Ventas unificadas: pedidos confirmados de PostgreSQL + histórico CSV.
    Mismo esquema para ambos orígenes: fecha, sku, unidades, ingresos."""
    det = _read_silver(spark, "silver_pedido_detalle")
    hist = _read_silver(spark, "silver_historico_ventas")

    transaccional = (
        det
        .filter(
            F.col("estado").isin(list(config.VENTA_ESTADOS))
            & (F.col("estado_pago") == "confirmado")
        )
        .groupBy(F.to_date("fecha").alias("fecha"), F.col("sku"))
        .agg(
            F.sum("cantidad").alias("unidades"),
            F.sum("subtotal").alias("ingresos"),
        )
        .withColumn("fuente", F.lit("transaccional"))
    )
    historico = (
        hist
        .groupBy("fecha", "sku")
        .agg(
            F.sum("cantidad").alias("unidades"),
            F.sum("subtotal").alias("ingresos"),
        )
        .withColumn("fuente", F.lit("historico"))
    )
    return transaccional.unionByName(historico)


# ---------------------------------------------------------------------------
# 1. gold_ventas_diarias
# ---------------------------------------------------------------------------

def gold_ventas_diarias(spark) -> DataFrame:
    ventas = _ventas_dataframe(spark)
    return (
        ventas
        .groupBy("fecha")
        .agg(
            F.sum("unidades").alias("unidades_vendidas"),
            F.sum("ingresos").alias("ingresos"),
            F.countDistinct("sku").alias("skus_distintos"),
        )
        .withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("semana", F.weekofyear("fecha"))
        .withColumn("dia_semana", F.dayofweek("fecha"))
        .withColumn("es_fin_de_semana", F.dayofweek("fecha").isin([1, 7]))
        .select(
            "fecha", "anio", "mes", "semana", "dia_semana", "es_fin_de_semana",
            "unidades_vendidas", "ingresos", "skus_distintos",
        )
    )


# ---------------------------------------------------------------------------
# 2. gold_top_productos
# ---------------------------------------------------------------------------

def gold_top_productos(spark) -> DataFrame:
    ventas = _ventas_dataframe(spark)
    productos = _read_silver(spark, "silver_productos")
    variantes = _read_silver(spark, "silver_variantes")

    agg = (
        ventas
        .filter(F.col("sku").isNotNull() & (F.col("sku") != ""))
        .groupBy("sku")
        .agg(F.sum("unidades").alias("unidades"), F.sum("ingresos").alias("ingresos"))
    )
    enriquecido = (
        agg
        .join(variantes.select("sku", "producto_id"), "sku", "left")
        .join(
            productos.select(
                F.col("id").alias("producto_id"),
                "nombre", "marca", "categoria",
            ),
            "producto_id", "left",
        )
    )
    w_global = Window.orderBy(F.col("ingresos").desc())
    w_categoria = Window.partitionBy("categoria").orderBy(F.col("ingresos").desc())
    return (
        enriquecido
        .withColumn("ranking_global", F.rank().over(w_global))
        .withColumn("ranking_categoria", F.rank().over(w_categoria))
        .select(
            "sku", "producto_id",
            # si el producto fue rechazado en Silver (p. ej. precio 0) se
            # muestra el sku como nombre en lugar de dejarlo en blanco
            F.coalesce("nombre", F.col("sku")).alias("nombre"),
            F.coalesce("marca", F.lit("—")).alias("marca"),
            F.coalesce("categoria", F.lit("—")).alias("categoria"),
            "unidades", "ingresos", "ranking_global", "ranking_categoria",
        )
    )


# ---------------------------------------------------------------------------
# 3. gold_ventas_por_talla
# ---------------------------------------------------------------------------

def gold_ventas_por_talla(spark) -> DataFrame:
    ventas = _ventas_dataframe(spark)
    variantes = _read_silver(spark, "silver_variantes")
    joined = ventas.join(variantes.select("sku", "talla", "color"), "sku", "inner")
    por_talla = (
        joined
        .groupBy("talla")
        .agg(
            F.sum("unidades").alias("unidades"),
            F.sum("ingresos").alias("ingresos"),
        )
        .withColumn("tipo", F.lit("talla"))
        .select(F.col("talla").alias("valor"), "tipo", "unidades", "ingresos")
    )
    por_color = (
        joined
        .groupBy("color")
        .agg(
            F.sum("unidades").alias("unidades"),
            F.sum("ingresos").alias("ingresos"),
        )
        .withColumn("tipo", F.lit("color"))
        .select(F.col("color").alias("valor"), "tipo", "unidades", "ingresos")
    )
    w = Window.partitionBy("tipo").orderBy(F.col("unidades").desc())
    return (
        por_talla.unionByName(por_color)
        .withColumn("ranking", F.rank().over(w))
        .select("valor", "tipo", "unidades", "ingresos", "ranking")
    )


# ---------------------------------------------------------------------------
# 4. gold_embudo_conversion
# ---------------------------------------------------------------------------

def gold_embudo_conversion(spark) -> DataFrame:
    eventos = _read_silver(spark, "silver_eventos_navegacion")
    embudo = (
        eventos
        .filter(F.col("event_type").isin(
            "product_viewed", "added_to_cart", "checkout_started", "order_placed",
        ))
        .groupBy("event_type")
        .agg(
            F.countDistinct("session_id").alias("sesiones"),
            F.count("*").alias("eventos"),
        )
    )
    orden = [
        ("product_viewed", "Vistas de producto"),
        ("added_to_cart", "Añadidos al carrito"),
        ("checkout_started", "Checkouts iniciados"),
        ("order_placed", "Compras finalizadas"),
    ]
    filas = []
    for etapa, nombre in orden:
        filas.append((etapa, nombre))
    orden_df = spark.createDataFrame(filas, "etapa string, etapa_nombre string")
    salida = orden_df.join(embudo, orden_df.etapa == embudo.event_type, "left").drop("event_type")
    w = Window.orderBy("etapa")
    return (
        salida
        .withColumn("orden", F.row_number().over(w))
        .withColumn("sesiones", F.coalesce(F.col("sesiones"), F.lit(0)))
        .withColumn("eventos", F.coalesce(F.col("eventos"), F.lit(0)))
        .withColumn("tasa_sobre_vistas",
                    F.when(F.col("sesiones") > 0, F.col("sesiones") / F.first("sesiones").over(w))
                    .otherwise(F.lit(0.0)))
        .select("orden", "etapa", "etapa_nombre", "sesiones", "eventos", "tasa_sobre_vistas")
    )


# ---------------------------------------------------------------------------
# 5. gold_carritos_abandonados
# ---------------------------------------------------------------------------

def gold_carritos_abandonados(spark) -> DataFrame:
    eventos = _read_silver(spark, "silver_eventos_navegacion")
    variantes = _read_silver(spark, "silver_variantes")
    productos = _read_silver(spark, "silver_productos")

    carritos = eventos.filter(F.col("event_type") == "added_to_cart")
    compras = (
        eventos
        .filter(F.col("event_type") == "order_placed")
        .select("session_id")
        .distinct()
    )
    abandonados = carritos.join(compras, "session_id", "left_anti")
    agg = (
        abandonados
        .groupBy("session_id", "sku")
        .agg(
            F.max("producto_id").alias("producto_id"),
            F.sum(F.coalesce(F.col("cantidad"), F.lit(1))).alias("unidades"),
            F.max("ts").alias("ultima_actividad"),
        )
    )
    por_producto = (
        agg
        .groupBy("sku")
        .agg(
            F.max("producto_id").alias("producto_id"),
            F.countDistinct("session_id").alias("carritos_abandonados"),
            F.sum("unidades").alias("unidades_estimadas"),
            F.max("ultima_actividad").alias("ultima_actividad"),
        )
        .join(variantes.select("sku", "talla", "color"), "sku", "left")
        .join(
            productos.select(
                F.col("id").alias("producto_id"), "nombre", "marca", "categoria",
            ),
            "producto_id", "left",
        )
        .select(
            "sku", "producto_id", "nombre", "marca", "categoria", "talla", "color",
            "carritos_abandonados", "unidades_estimadas", "ultima_actividad",
        )
    )
    return por_producto.orderBy(F.col("carritos_abandonados").desc())


# ---------------------------------------------------------------------------
# 6. gold_stock_critico
# ---------------------------------------------------------------------------

def gold_stock_critico(spark) -> DataFrame:
    inventario = _read_silver(spark, "silver_inventario")
    variantes = _read_silver(spark, "silver_variantes")
    productos = _read_silver(spark, "silver_productos")
    return (
        inventario
        .filter(F.col("stock_disponible") <= F.lit(config.STOCK_UMBRAL))
        .join(variantes, "variante_id", "inner")
        .join(
            productos.select(
                F.col("id").alias("producto_id"), "nombre", "marca", "categoria", "precio",
            ),
            "producto_id", "left",
        )
        .withColumn("valor_riesgo", F.col("stock_disponible") * F.col("precio"))
        .select(
            "variante_id", "sku", "nombre", "marca", "categoria",
            "talla", "color", "stock_disponible", "bodega", "valor_riesgo",
        )
        .orderBy("stock_disponible")
    )


# ---------------------------------------------------------------------------
# 7. gold_tasa_devolucion
# ---------------------------------------------------------------------------

def gold_tasa_devolucion(spark) -> DataFrame:
    det = _read_silver(spark, "silver_pedido_detalle")
    dev = _read_silver(spark, "silver_devoluciones")
    productos = _read_silver(spark, "silver_productos")

    vendidas = (
        det
        .filter(F.col("estado").isin(list(config.VENTA_ESTADOS)))
        .groupBy("producto_id")
        .agg(
            F.countDistinct("pedido_id").alias("pedidos_con_producto"),
            F.sum("cantidad").alias("unidades_vendidas"),
        )
    )
    con_dev = dev.join(det.select("pedido_id", "producto_id").distinct(), "pedido_id", "inner")
    agg_dev = (
        con_dev
        .groupBy("producto_id", "motivo")
        .agg(F.countDistinct("pedido_id").alias("devoluciones"))
    )
    w_motivo = Window.partitionBy("producto_id").orderBy(F.col("devoluciones").desc())
    motivo_top = (
        agg_dev
        .withColumn("rn", F.row_number().over(w_motivo))
        .filter(F.col("rn") == 1)
        .select("producto_id", F.col("motivo").alias("motivo_mas_frecuente"))
    )
    totales = (
        agg_dev
        .groupBy("producto_id")
        .agg(F.sum("devoluciones").alias("devoluciones"))
    )
    return (
        totales
        .join(vendidas, "producto_id", "left")
        .join(motivo_top, "producto_id", "left")
        .join(
            productos.select(
                F.col("id").alias("producto_id"), "nombre", "marca", "categoria",
            ),
            "producto_id", "left",
        )
        .withColumn(
            "tasa_devolucion",
            F.when(F.col("unidades_vendidas") > 0,
                   F.col("devoluciones") / F.col("unidades_vendidas"))
            .otherwise(F.lit(None).cast("double")),
        )
        .select(
            "producto_id",
            F.coalesce("nombre", F.lit("—")).alias("nombre"),
            F.coalesce("marca", F.lit("—")).alias("marca"),
            F.coalesce("categoria", F.lit("—")).alias("categoria"),
            "pedidos_con_producto", "unidades_vendidas",
            "devoluciones", "tasa_devolucion", "motivo_mas_frecuente",
        )
        .orderBy(F.col("devoluciones").desc())
    )


# ---------------------------------------------------------------------------
# 8. gold_clientes_valor
# ---------------------------------------------------------------------------

def gold_clientes_valor(spark) -> DataFrame:
    det = _read_silver(spark, "silver_pedido_detalle")
    clientes = _read_silver(spark, "silver_clientes")

    compras = det.filter(
        F.col("estado").isin(list(config.VENTA_ESTADOS))
        & (F.col("estado_pago") == "confirmado")
    )
    agg = (
        compras
        .groupBy("cliente_id")
        .agg(
            F.countDistinct("pedido_id").alias("pedidos"),
            F.sum("subtotal").alias("gasto_total"),
            F.min("fecha").alias("primera_compra"),
            F.max("fecha").alias("ultima_compra"),
        )
    )
    w = Window.orderBy(F.col("gasto_total").desc())
    return (
        agg
        .join(clientes, F.col("cliente_id") == F.col("id"), "left")
        .withColumn("ticket_promedio", F.col("gasto_total") / F.col("pedidos"))
        .withColumn("ranking", F.row_number().over(w))
        .withColumn(
            "segmento",
            F.when(F.col("pedidos") >= 5, "frecuente")
            .when(F.col("pedidos") >= 2, "recurrente")
            .otherwise("ocasional"),
        )
        .select(
            F.col("cliente_id"), F.col("nombre"), F.col("correo"), F.col("ciudad"),
            "pedidos", "gasto_total", "ticket_promedio",
            "primera_compra", "ultima_compra", "segmento", "ranking",
        )
    )


# ---------------------------------------------------------------------------
# 9. gold_catalogo_externo
# ---------------------------------------------------------------------------

def gold_catalogo_externo(spark) -> DataFrame:
    """Benchmark del catálogo consumido de la API externa por categoría y marca."""
    cat = _read_silver(spark, "silver_catalogo_externo")
    return (
        cat
        .groupBy("categoria_origen", "categoria", "marca")
        .agg(
            F.count("*").alias("productos"),
            F.round(F.avg("precio"), 2).alias("precio_medio"),
            F.round(F.min("precio"), 2).alias("precio_min"),
            F.round(F.max("precio"), 2).alias("precio_max"),
            F.round(F.avg("rating"), 2).alias("rating_medio"),
            F.sum("stock").alias("stock_total"),
        )
        .orderBy(F.col("productos").desc(), F.col("categoria").asc())
    )


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------

BUILDERS = {
    "gold_ventas_diarias": gold_ventas_diarias,
    "gold_top_productos": gold_top_productos,
    "gold_ventas_por_talla": gold_ventas_por_talla,
    "gold_embudo_conversion": gold_embudo_conversion,
    "gold_carritos_abandonados": gold_carritos_abandonados,
    "gold_stock_critico": gold_stock_critico,
    "gold_tasa_devolucion": gold_tasa_devolucion,
    "gold_clientes_valor": gold_clientes_valor,
    "gold_catalogo_externo": gold_catalogo_externo,
}

# Tablas que dependen de la ingesta de la API: si no hay datos, se omiten.
OPTIONAL_BUILDERS = {"gold_catalogo_externo"}


def run_gold(spark) -> dict:
    conteos = {}
    for nombre, builder in BUILDERS.items():
        if nombre in OPTIONAL_BUILDERS and not config.silver_path(
            "silver_catalogo_externo"
        ).exists():
            print(f"[gold] {nombre}: omitido (sin catálogo de la API externa)")
            continue
        df = builder(spark).repartition(1)
        df.write.mode("overwrite").parquet(config.spath(config.gold_path(nombre)))
        conteos[nombre] = spark.read.parquet(config.spath(config.gold_path(nombre))).count()
        print(f"[gold] {nombre}: {conteos[nombre]} filas")
    return conteos


def main() -> int:
    spark = get_spark("gold-metrics")
    try:
        run_gold(spark)
        print("[gold] OK")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
