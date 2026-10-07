"""Esquemas compartidos entre productor, batch y streaming."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Esquema de los eventos web (JSON) que llegan por Kafka o que se guardan
# como archivo histórico. Los campos opcionales pueden venir ausentes.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("ts", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("user_id", LongType(), True),
        StructField("producto_id", LongType(), True),
        StructField("variante_id", LongType(), True),
        StructField("sku", StringType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("monto", DoubleType(), True),
        StructField("search_query", StringType(), True),
        StructField("stock_actual", IntegerType(), True),
        StructField("motivo", StringType(), True),
        StructField("canal", StringType(), True),
    ]
)
