"""Pipeline STREAMING: Kafka -> Bronze (raw) + Gold (métricas en vivo).

Una única Spark Structured Streaming (foreachBatch, checkpoint propio) que:
  1. Escribe cada micro-batch crudo en Bronze: lake/bronze/eventos_web/stream/
  2. Acumula en Gold:
       - gold_actividad_stream: eventos y sesiones por tipo (por micro-batch).
       - gold_alertas: alertas stock_low generadas en vivo.

Los históricos y el resto de capas las hace el pipeline batch (app.batch).

Uso:  python -m app.streaming
"""

import os

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import from_json

from app import config
from app.schemas import EVENT_SCHEMA
from app.spark_utils import get_spark

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": config.KAFKA_BOOTSTRAP,
    "subscribe": config.KAFKA_TOPIC,
    "startingOffsets": config.KAFKA_STARTING_OFFSETS,
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "5000",
}


def _leer_eventos(spark) -> DataFrame:
    raw = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()
    parsed = (
        raw
        .withColumn("payload", from_json(F.col("value").cast("string"), EVENT_SCHEMA))
        .select("payload.*", F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
        .withColumn("ts", F.to_timestamp("ts"))
        .filter(F.col("event_id").isNotNull())
    )
    return parsed


def _actividad(batch: DataFrame) -> DataFrame:
    """Conteo de eventos y sesiones por tipo para este micro-batch."""
    return (
        batch
        .groupBy("event_type")
        .agg(
            F.count("*").alias("eventos"),
            F.countDistinct("session_id").alias("sesiones"),
            F.min("ts").alias("primero_ts"),
            F.max("ts").alias("ultimo_ts"),
        )
        .withColumn("procesado_en", F.current_timestamp())
    )


def _alertas(batch: DataFrame) -> DataFrame:
    """Alertas de stock bajo generadas en este micro-batch."""
    return (
        batch
        .filter(F.col("event_type") == "stock_low")
        .select(
            "ts", "sku", "producto_id", "variante_id", "stock_actual",
        )
        .withColumn("umbral", F.lit(config.STOCK_UMBRAL))
        .withColumn(
            "severidad",
            F.when(F.col("stock_actual") <= 0, "agotado")
            .when(F.col("stock_actual") <= F.lit(max(config.STOCK_UMBRAL // 2, 1)), "critico")
            .otherwise("bajo"),
        )
        .withColumn("procesado_en", F.current_timestamp())
    )


def _procesar_lote(batch: DataFrame, _batch_id: int) -> None:
    batch.cache()
    try:
        n = batch.count()
        if n == 0:
            return
        # 1. Bronze: copia cruda de los eventos del micro-batch
        (batch
         .select("event_id", "event_type", "ts", "session_id", "user_id",
                 "producto_id", "variante_id", "sku", "cantidad", "monto",
                 "search_query", "stock_actual", "motivo", "canal")
         .write
         .mode("append")
         .json(config.spath(config.bronze_path("eventos_web", "stream"))))

        # 2. Gold: actividad del micro-batch
        act = _actividad(batch)
        (act.write.mode("append")
         .parquet(config.spath(config.gold_path("gold_actividad_stream"))))

        # 3. Gold: alertas de stock bajo
        al = _alertas(batch)
        n_al = al.count()
        if n_al > 0:
            (al.write.mode("append")
             .parquet(config.spath(config.gold_path("gold_alertas"))))
            print(f"[streaming] {n_al} alerta(s) de stock_low")
        print(f"[streaming] lote {_batch_id}: {n} eventos -> bronze + gold")
    finally:
        batch.unpersist()


def main() -> int:
    spark = get_spark("streaming-eventos")
    try:
        eventos = _leer_eventos(spark)
        query = (
            eventos.writeStream
            .foreachBatch(_procesar_lote)
            .option("checkpointLocation", config.spath(config.checkpoint_path("eventos_web")))
            .trigger(processingTime=os.environ.get("STREAM_TRIGGER", "10 seconds"))
            .start()
        )
        print(f"[streaming] Escuchando '{config.KAFKA_TOPIC}' en {config.KAFKA_BOOTSTRAP} "
              f"(Spark UI en 4040). Ctrl+C para detener.")
        query.awaitTermination()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
