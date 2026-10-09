"""Capa SILVER: datos limpios y validados.

Reglas aplicadas:
- Deduplicación (por ejemplo, el mismo order_placed recibido dos veces).
- Estandarización de formatos: fechas, monedas, tallas y colores.
- Validación de campos obligatorios (correo, precio > 0, talla válida...).
- Corrección de tipos y manejo de nulos.
- Registros inválidos -> silver/rechazados/<tabla>/
- Unión de pedidos + detalle + pagos en una vista consistente (silver_pedido_detalle).

Uso:  python -m app.silver
"""

import sys

from pyspark.sql import Column, DataFrame, Window, functions as F

from app import config
from app.schemas import EVENT_SCHEMA
from app.spark_utils import get_spark

# ---------------------------------------------------------------------------
# Helpers de limpieza
# ---------------------------------------------------------------------------

def talla_estandar(col_name: str = "talla"):
    """Expresión que normaliza una talla a formato '36' | '36.5'.
    Admite 'EU 36', ' T38 ', '37,5', '36.5', 'TALLA 40'."""
    s = F.upper(F.trim(F.col(col_name)))
    # quita el prefijo solo después de recortar espacios (el ancla ^ lo exige)
    s = F.trim(F.regexp_replace(s, r"^(EU|TALLA|T|ESP)\.?\s*", ""))
    s = F.regexp_replace(s, r",", ".")
    num = F.regexp_extract(s, r"^(\d{1,2}(?:\.5)?)$", 1)
    return F.when(num != "", num).otherwise(None)


def fecha_multi(col_name: str):
    """Soporta 'YYYY-MM-DD', 'DD/MM/YYYY', 'DD-MM-YYYY', 'DD.MM.YYYY'."""
    s = F.trim(F.col(col_name).cast("string"))
    return (
        F.coalesce(
            F.to_date(s, "yyyy-MM-dd"),
            F.to_date(s, "dd/MM/yyyy"),
            F.to_date(s, "dd-MM-yyyy"),
            F.to_date(s, "dd.MM.yyyy"),
        )
    )


def _write(df: DataFrame, path, mode: str = "overwrite") -> None:
    df.write.mode(mode).option("mergeSchema", "true").parquet(config.spath(path))


def _write_rejects(df: DataFrame, tabla: str) -> int:
    if df.rdd.isEmpty():
        return 0
    n = df.count()
    _write(df, config.rejects_path(tabla))
    print(f"[silver] rechazados/{tabla}: {n} filas")
    return n


# ---------------------------------------------------------------------------
# Lecturas de Bronze
# ---------------------------------------------------------------------------

def read_postgres(spark, tabla: str) -> DataFrame:
    path = config.bronze_path("postgres", tabla)
    if not path.exists():
        raise FileNotFoundError(f"Bronze no contiene '{tabla}'. Ejecuta: python -m app.bronze")
    return spark.read.parquet(config.spath(path))


# ---------------------------------------------------------------------------
# Transformaciones por tabla
# ---------------------------------------------------------------------------

def silver_clientes(spark) -> DataFrame:
    df = read_postgres(spark, "clientes")
    # dedupe por id conservando la última ingesta
    w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    limpio = (
        df
        .withColumn("nombre", F.trim(F.col("nombre")))
        .withColumn("correo", F.lower(F.trim(F.col("correo"))))
        .withColumn("ciudad", F.initcap(F.lower(F.trim(F.col("ciudad")))))
        .withColumn("fecha_registro", F.to_date("fecha_registro"))
    )
    validos = limpio.filter(F.col("correo").rlike(config.EMAIL_REGEX))
    invalidos = limpio.filter(~F.col("correo").rlike(config.EMAIL_REGEX))
    _write_rejects(invalidos.select("id", "nombre", "correo", "ciudad", "fecha_registro",
                                     F.lit("correo_inválido").alias("motivo_rechazo")), "clientes")
    # dedupe por correo (mismo cliente registrado dos veces)
    w2 = Window.partitionBy("correo").orderBy("fecha_registro", "id")
    validos = validos.withColumn("_rn", F.row_number().over(w2)).filter(F.col("_rn") == 1).drop("_rn")
    return validos.select("id", "nombre", "correo", "ciudad", "fecha_registro")


def silver_productos(spark) -> DataFrame:
    df = read_postgres(spark, "productos")
    w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    limpio = (
        df
        .withColumn("nombre", F.trim(F.col("nombre")))
        .withColumn("marca", F.initcap(F.lower(F.trim(F.col("marca")))))
        .withColumn("categoria", F.lower(F.trim(F.col("categoria"))))
        .withColumn("precio", F.col("precio").cast("double"))
    )
    validos = limpio.filter(
        (F.col("precio") > 0)
        & F.col("nombre").isNotNull() & (F.col("nombre") != "")
        & F.col("categoria").isin(list(config.CATEGORIAS))
    )
    invalidos = limpio.filter(~(
        (F.col("precio") > 0)
        & F.col("nombre").isNotNull() & (F.col("nombre") != "")
        & F.col("categoria").isin(list(config.CATEGORIAS))
    ))
    _write_rejects(invalidos.select(
        "id", "nombre", "marca", "categoria", "precio",
        F.lit("precio_o_categoria_invalido").alias("motivo_rechazo")), "productos")
    return validos.drop("_ingested_at", "_source", "_ingest_date")


def silver_variantes(spark) -> DataFrame:
    df = read_postgres(spark, "variantes")
    w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    limpio = (
        df
        .withColumn("talla_std", talla_estandar("talla"))
        .withColumn("sku", F.upper(F.trim(F.col("sku"))))
        .withColumn("color", F.lower(F.trim(F.col("color"))))
    )
    valido = limpio.filter(F.col("talla_std").isNotNull() & (F.col("sku") != ""))
    invalido = limpio.filter(F.col("talla_std").isNull() | F.col("sku").isNull() | (F.col("sku") == ""))
    _write_rejects(invalido.select(
        "id", "producto_id", "talla", "color", "sku",
        F.lit("talla_invalida").alias("motivo_rechazo")), "variantes")
    return valido.select(
        F.col("id").alias("variante_id"), "producto_id", "talla_std", "color", "sku"
    ).withColumnRenamed("talla_std", "talla")


def silver_inventario(spark, variantes: DataFrame) -> DataFrame:
    df = read_postgres(spark, "inventario")
    w = Window.partitionBy("variante_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    limpio = (
        df
        .withColumn("stock_disponible", F.col("stock_disponible").cast("int"))
        .withColumn("bodega", F.upper(F.trim(F.col("bodega"))))
        .filter(F.col("stock_disponible").isNotNull() & (F.col("stock_disponible") >= 0))
    )
    return (
        limpio.join(variantes.select("variante_id"), "variante_id", "inner")
        .drop("_ingested_at", "_source", "_ingest_date")
    )


def silver_pedidos(spark) -> DataFrame:
    df = read_postgres(spark, "pedidos")
    w = Window.partitionBy("id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    limpio = (
        df
        .withColumn("fecha", F.to_timestamp("fecha"))
        .withColumn("estado", F.lower(F.trim(F.col("estado"))))
        .withColumn("total", F.col("total").cast("double"))
    )
    validos = limpio.filter(
        F.col("fecha").isNotNull()
        & (F.col("total") >= 0)
        & F.col("estado").isin(list(config.ESTADOS_PEDIDO))
    )
    invalidos = limpio.filter(~(
        F.col("fecha").isNotNull()
        & (F.col("total") >= 0)
        & F.col("estado").isin(list(config.ESTADOS_PEDIDO))
    ))
    _write_rejects(invalidos.select(
        "id", "cliente_id", "fecha", "estado", "total",
        F.lit("estado_fecha_total_invalido").alias("motivo_rechazo")), "pedidos")
    return validos.select(
        F.col("id").alias("pedido_id"), "cliente_id", "fecha", "estado", "total"
    )


def silver_pedido_detalle(spark, pedidos: DataFrame, variantes: DataFrame) -> DataFrame:
    """Unión de pedidos + detalle + pagos: la vista consistente de un pedido."""
    det = read_postgres(spark, "detalle_pedido")
    pag = read_postgres(spark, "pagos")

    wdet = Window.partitionBy("pedido_id", "variante_id").orderBy(F.col("_ingested_at").desc())
    det = det.withColumn("_rn", F.row_number().over(wdet)).filter(F.col("_rn") == 1).drop("_rn")
    wpag = Window.partitionBy("pedido_id").orderBy(F.col("_ingested_at").desc())
    pag = pag.withColumn("_rn", F.row_number().over(wpag)).filter(F.col("_rn") == 1).drop("_rn")

    det = (
        det
        .withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("precio", F.col("precio").cast("double"))
        .withColumn("subtotal", F.col("cantidad") * F.col("precio"))
        .filter((F.col("cantidad") > 0) & (F.col("precio") > 0))
    )
    pag = (
        pag
        .withColumn("monto", F.col("monto").cast("double"))
        .withColumn("estado", F.lower(F.trim(F.col("estado"))))
        .withColumn("metodo", F.lower(F.trim(F.col("metodo"))))
        .select(
            F.col("pedido_id"),
            F.col("metodo").alias("metodo_pago"),
            F.col("estado").alias("estado_pago"),
            "monto",
        )
    )
    return (
        pedidos
        .join(det, "pedido_id", "inner")
        .join(pag, "pedido_id", "left")
        .join(variantes.select("variante_id", "producto_id", "talla", "color", "sku"),
              "variante_id", "left")
        .drop("_ingested_at", "_source", "_ingest_date")
    )


def silver_devoluciones(spark, pedidos: DataFrame) -> DataFrame:
    df = read_postgres(spark, "devoluciones")
    w = Window.partitionBy("pedido_id").orderBy(F.col("_ingested_at").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    limpio = (
        df
        .withColumn("fecha", F.to_date("fecha"))
        .withColumn("motivo", F.lower(F.trim(F.col("motivo"))))
    )
    validos = limpio.filter(F.col("fecha").isNotNull() & F.col("motivo").isNotNull())
    invalidos = limpio.filter(F.col("fecha").isNull() | F.col("motivo").isNull())
    _write_rejects(invalidos.select(
        "pedido_id", "motivo", "fecha",
        F.lit("motivo_o_fecha_invalido").alias("motivo_rechazo")), "devoluciones")
    return (
        validos.join(pedidos.select("pedido_id"), "pedido_id", "inner")
        .drop("_ingested_at", "_source", "_ingest_date")
    )


def silver_historico_ventas(spark) -> DataFrame:
    path = config.bronze_path("historico_ventas")
    if not path.exists():
        raise FileNotFoundError("Bronze/historico_ventas no existe. Ejecuta: python -m app.bronze")
    df = spark.read.option("header", True).option("sep", ",").csv(config.spath(path))

    limpio = (
        df
        .withColumn("fecha", fecha_multi("fecha"))
        .withColumn("cantidad", F.col("cantidad").cast("int"))
        .withColumn("precio_unitario", _precio(F.col("precio_unitario")))
        .withColumn("sku", F.upper(F.trim(F.col("sku"))))
        .withColumn("canal", F.lower(F.trim(F.col("canal"))))
        .withColumn("ciudad", F.initcap(F.lower(F.trim(F.col("ciudad")))))
        .withColumn("subtotal", F.col("cantidad") * F.col("precio_unitario"))
    )
    validos = limpio.filter(
        F.col("fecha").isNotNull()
        & F.col("cantidad").isNotNull() & (F.col("cantidad") > 0)
        & F.col("precio_unitario").isNotNull() & (F.col("precio_unitario") > 0)
        & (F.col("sku") != "")
    )
    invalidos = limpio.filter(~(
        F.col("fecha").isNotNull()
        & F.col("cantidad").isNotNull() & (F.col("cantidad") > 0)
        & F.col("precio_unitario").isNotNull() & (F.col("precio_unitario") > 0)
        & (F.col("sku") != "")
    ))
    _write_rejects(invalidos.select(
        "fecha", "orden_ref", "sku", "cantidad", "precio_unitario",
        F.lit("fecha_cantidad_precio_invalido").alias("motivo_rechazo")),
        "historico_ventas")
    return validos


def silver_historico_devoluciones(spark) -> DataFrame:
    path = config.bronze_path("historico_devoluciones", "devoluciones_historicas.csv")
    if not path.exists():
        raise FileNotFoundError("Bronze/historico_devoluciones no existe. Ejecuta: python -m app.bronze")
    df = spark.read.option("header", True).option("sep", ",").csv(config.spath(path.parent))
    limpio = (
        df
        .withColumn("fecha", fecha_multi("fecha"))
        .withColumn("motivo", F.lower(F.trim(F.col("motivo"))))
        .withColumn("orden_ref", F.trim(F.col("orden_ref")))
    )
    return limpio.filter(F.col("fecha").isNotNull() & (F.col("orden_ref") != ""))


def silver_proveedores(spark) -> DataFrame:
    path = config.bronze_path("proveedores", "proveedores.json")
    if not path.exists():
        raise FileNotFoundError("Bronze/proveedores no existe. Ejecuta: python -m app.bronze")
    df = spark.read.option("multiLine", True).json(config.spath(path))
    return (
        df
        .withColumn("proveedor_id", F.trim("proveedor_id"))
        .withColumn("nombre", F.trim("nombre"))
        .withColumn("pais", F.trim("pais"))
        .withColumn("plazo_entrega_dias", F.col("plazo_entrega_dias").cast("int"))
        .withColumn("categorias", F.concat_ws(",", F.col("categorias")))
    )


def silver_eventos(spark) -> DataFrame:
    """Eventos web de Bronze (Kafka streaming + históricos) limpios y deduplicados."""
    stream_path = config.bronze_path("eventos_web", "stream")
    hist_path = config.bronze_path("eventos_web", "historico")
    paths = [config.spath(p) for p in (hist_path, stream_path) if p.exists()]
    if not paths:
        raise FileNotFoundError("No hay eventos en Bronze. Ejecuta app.batch o app.streaming.")

    df = spark.read.schema(EVENT_SCHEMA).json(paths)
    limpio = (
        df
        .withColumn("ts", F.to_timestamp("ts"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("session_id", F.trim(F.col("session_id")))
        .withColumn("sku", F.upper(F.trim(F.col("sku"))))
        .withColumn("canal", F.lower(F.trim(F.col("canal"))))
    )
    validos = limpio.filter(
        F.col("event_id").isNotNull() & (F.col("event_id") != "")
        & F.col("session_id").isNotNull() & (F.col("session_id") != "")
        & F.col("ts").isNotNull()
        & F.col("event_type").isin(list(config.EVENTOS_CONOCIDOS))
    )
    invalidos = limpio.filter(~(
        F.col("event_id").isNotNull() & (F.col("event_id") != "")
        & F.col("session_id").isNotNull() & (F.col("session_id") != "")
        & F.col("ts").isNotNull()
        & F.col("event_type").isin(list(config.EVENTOS_CONOCIDOS))
    ))
    _write_rejects(invalidos.select(
        "event_id", "event_type", "ts", "session_id",
        F.lit("evento_invalido").alias("motivo_rechazo")), "eventos")

    # dedupe global por event_id (el mismo order_placed llega dos veces)
    w = Window.partitionBy("event_id")
    return (
        validos
        .withColumn("_rn", F.row_number().over(w.orderBy("ts")))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def silver_catalogo_externo(spark) -> DataFrame | None:
    """Catálogo de zapatos consumido de la API externa (bronze/api_catalogo).

    Devuelve None si aún no se ha ingerido nada (la API es opcional y el batch
    debe funcionar sin conexión)."""
    path = config.bronze_path("api_catalogo")
    if not path.exists() or not any(path.glob("*.jsonl")):
        return None
    df = spark.read.json(config.spath(path))
    limpio = (
        df
        .withColumn("api_id", F.col("id").cast("int"))
        .withColumn("titulo", F.trim(F.col("title")))
        .withColumn("marca", F.initcap(F.lower(F.trim(F.col("brand")))))
        .withColumn("categoria", F.lower(F.trim(F.col("category"))))
        .withColumn("precio", F.col("price").cast("double"))
        .withColumn("descuento_pct", F.col("discountPercentage").cast("double"))
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("stock", F.col("stock").cast("int"))
        .withColumn("sku", F.upper(F.trim(F.col("sku"))))
        .withColumn("etiquetas", F.concat_ws(",", F.col("tags")))
    )
    valido = (
        (F.col("precio") > 0)
        & F.col("titulo").isNotNull() & (F.col("titulo") != "")
        & F.col("sku").isNotNull() & (F.col("sku") != "")
    )
    validos = limpio.filter(valido)
    invalidos = limpio.filter(~valido)
    _write_rejects(invalidos.select(
        "api_id", "titulo", "marca", "precio", "sku",
        F.lit("catalogo_api_invalido").alias("motivo_rechazo")), "catalogo_externo")
    return validos.select(
        "api_id", "titulo", "marca", "categoria", "precio", "descuento_pct",
        "rating", "stock", "sku", "etiquetas",
        F.col("_categoria_api").alias("categoria_origen"),
        "_source", "_ingested_at",
    )


# ---------------------------------------------------------------------------
# UDF de precio (se crea bajo demanda: el decorador necesita Spark activo)
# ---------------------------------------------------------------------------

from pyspark.sql.functions import pandas_udf  # noqa: E402
import pandas as pd  # noqa: E402


def _precio(col: Column) -> Column:
    """'€89,90' / '1.234,56' / '89.90 EUR' / '89,9' -> 89.9 / 1234.56 / 89.9 / 89.9"""

    @pandas_udf("double")
    def parse(s: pd.Series) -> pd.Series:
        def _one(v):
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            t = str(v).replace("€", "").replace("$", "").upper()
            t = t.replace("EUR", "").strip().replace(" ", "")
            if "," in t and "." in t:
                # 1.234,56 -> decimal español; 1,234.56 -> decimal inglés
                if t.rfind(",") > t.rfind("."):
                    t = t.replace(".", "").replace(",", ".")
                else:
                    t = t.replace(",", "")
            elif "," in t:
                t = t.replace(",", ".")
            try:
                return float(t)
            except ValueError:
                return None

        return s.map(_one)

    return parse(col)


# ---------------------------------------------------------------------------
# Orquestador
# ---------------------------------------------------------------------------

def run_silver(spark) -> dict:
    """Ejecuta todas las transformaciones Silver y devuelve conteos por tabla."""
    clientes = silver_clientes(spark)
    productos = silver_productos(spark)
    variantes = silver_variantes(spark)
    inventario = silver_inventario(spark, variantes)
    pedidos = silver_pedidos(spark)
    detalle = silver_pedido_detalle(spark, pedidos, variantes)
    devoluciones = silver_devoluciones(spark, pedidos)
    hist_ventas = silver_historico_ventas(spark)
    hist_dev = silver_historico_devoluciones(spark)
    proveedores = silver_proveedores(spark)
    eventos = silver_eventos(spark)
    catalogo = silver_catalogo_externo(spark)

    tablas = {
        "silver_clientes": clientes,
        "silver_productos": productos,
        "silver_variantes": variantes,
        "silver_inventario": inventario,
        "silver_pedidos": pedidos,
        "silver_pedido_detalle": detalle,
        "silver_devoluciones": devoluciones,
        "silver_historico_ventas": hist_ventas,
        "silver_historico_devoluciones": hist_dev,
        "silver_proveedores": proveedores,
        "silver_eventos_navegacion": eventos,
    }
    if catalogo is not None:
        tablas["silver_catalogo_externo"] = catalogo
    else:
        print("[silver] silver_catalogo_externo: omitido (sin ingesta de la API)")
    conteos = {}
    for nombre, df in tablas.items():
        out = df.repartition(1)
        _write(out, config.silver_path(nombre))
        conteos[nombre] = spark.read.parquet(config.spath(config.silver_path(nombre))).count()
        print(f"[silver] {nombre}: {conteos[nombre]} filas")
    print("[silver] OK")
    return conteos


def main() -> int:
    spark = get_spark("silver-transform")
    try:
        run_silver(spark)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
