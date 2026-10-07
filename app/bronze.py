"""Capa BRONZE: carga de datos originales sin modificar.

- PostgreSQL -> bronze/postgres/<tabla>/ingest_date=YYYY-MM-DD/ (Parquet)
  Solo se añaden metadatos de ingesta: _ingested_at, _source, _ingest_date.
  Se reemplaza únicamente la partición del día (reprocesable e idempotente).
- Archivos históricos (CSV/JSON) -> bronze/historico_ventas/, bronze/historico_devoluciones/,
  bronze/proveedores/ conservando el formato original.
- Los eventos de Kafka entran por streaming (app.streaming) en bronze/eventos_web/.

Uso:  python -m app.bronze
"""

import json
import shutil
import sys
from datetime import datetime

from pyspark.sql import functions as F

from app import config
from app.spark_utils import get_spark


def ingest_postgres(spark) -> int:
    hoy = datetime.now().strftime("%Y-%m-%d")
    total = 0
    for tabla in config.BRONZE_TABLES:
        df = (
            spark.read
            .jdbc(config.JDBC_URL, tabla, properties=config.JDBC_PROPS)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source", F.lit(f"postgresql:{tabla}"))
            .withColumn("_ingest_date", F.lit(hoy))
        )
        n = df.count()
        (
            df.write
            .mode("overwrite")
            .partitionBy("_ingest_date")
            .parquet(config.spath(config.bronze_path("postgres", tabla)))
        )
        print(f"[bronze] postgres/{tabla}: {n} filas")
        total += n
    return total


def ingest_historicos() -> int:
    """Copia los archivos crudos a Bronze tal cual (mismo contenido, mismo nombre)."""
    pares = [
        (config.RAW_ROOT / "historico_ventas", config.bronze_path("historico_ventas"), "*.csv"),
        (config.RAW_ROOT / "devoluciones_historicas.csv", config.bronze_path("historico_devoluciones"), None),
        (config.RAW_ROOT / "proveedores.json", config.bronze_path("proveedores"), None),
        (config.RAW_ROOT / "eventos_web", config.bronze_path("eventos_web", "historico"), "*.json"),
    ]
    copiados = 0
    for origen, destino, patron in pares:
        destino.mkdir(parents=True, exist_ok=True)
        if patron:
            archivos = sorted(origen.glob(patron)) if origen.exists() else []
        else:
            archivos = [origen] if origen.exists() else []
        for archivo in archivos:
            shutil.copy2(archivo, destino / archivo.name)
            copiados += 1
    # manifiesto de ingesta (únicos metadatos de Bronze)
    manifiesto = {
        "ingested_at": datetime.now().isoformat(timespec="seconds"),
        "origen": "data/raw",
        "archivos": sorted(p.name for p in config.bronze_path("historico_ventas").glob("*.csv"))
        + ["devoluciones_historicas.csv", "proveedores.json"],
    }
    config.bronze_path("historico_ventas").mkdir(parents=True, exist_ok=True)
    (config.bronze_path("_manifiesto.json")).write_text(
        json.dumps(manifiesto, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"[bronze] archivos históricos copiados: {copiados}")
    return copiados


def main() -> int:
    if not (config.RAW_ROOT / "historico_ventas").exists():
        print("[bronze] No existe data/raw — ejecuta antes: python -m app.seed", file=sys.stderr)
        return 1
    spark = get_spark("bronze-ingest")
    try:
        ingest_postgres(spark)
        ingest_historicos()
        print("[bronze] OK")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
