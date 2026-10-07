"""Pipeline BATCH: Bronze -> Silver -> Gold con Apache Spark.

Secuencia:
  1. PostgreSQL y archivos históricos -> Bronze (crudos + metadatos de ingesta).
  2. Bronze -> Silver (limpieza, validación, dedupe, rechazados).
  3. Silver -> Gold (métricas de negocio).

Uso:  python -m app.batch
"""

from app import bronze, config, gold, silver
from app.spark_utils import get_spark


def main() -> int:
    if not (config.RAW_ROOT / "historico_ventas").exists():
        print("[batch] No existe data/raw — ejecuta antes: python -m app.seed")
        return 1

    spark = get_spark("batch-pipeline")
    try:
        bronze.ingest_postgres(spark)
        bronze.ingest_historicos()
        silver.run_silver(spark)
        gold.run_gold(spark)
        print("[batch] Pipeline completado: Bronze -> Silver -> Gold")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
