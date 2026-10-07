"""Pipeline BATCH: Bronze -> Silver -> Gold con Apache Spark.

Secuencia:
  1. PostgreSQL y archivos históricos -> Bronze (crudos + metadatos de ingesta).
  2. Bronze -> Silver (limpieza, validación, dedupe, rechazados).
  3. Silver -> Gold (métricas de negocio).

Cada ejecución deja un registro en `lake/_metadata/runs.json` (inicio, fin,
duración, estado y filas por capa) que se muestra en el dashboard.

Uso:  python -m app.batch
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

from app import bronze, config, gold, silver
from app.spark_utils import get_spark


def _runs_path() -> Path:
    return config.metadata_path("runs.json")


def _log_run(registro: dict) -> None:
    """Append de la ejecución a runs.json (conserva las últimas 50)."""
    ruta = _runs_path()
    ruta.parent.mkdir(parents=True, exist_ok=True)
    try:
        runs = json.loads(ruta.read_text(encoding="utf-8")) if ruta.exists() else []
        if not isinstance(runs, list):
            runs = []
    except (json.JSONDecodeError, OSError):
        runs = []
    runs.append(registro)
    ruta.write_text(
        json.dumps(runs[-50:], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def main() -> int:
    if not (config.RAW_ROOT / "historico_ventas").exists():
        print("[batch] No existe data/raw — ejecuta antes: python -m app.seed")
        return 1

    registro = {
        "inicio": datetime.now().isoformat(timespec="seconds"),
        "estado": "error",
    }
    t0 = time.time()
    ok = False
    spark = get_spark("batch-pipeline")
    try:
        registro["bronze"] = (
            bronze.ingest_postgres(spark) + bronze.ingest_historicos()
        )
        registro["silver"] = silver.run_silver(spark)
        registro["gold"] = gold.run_gold(spark)
        ok = True
        print("[batch] Pipeline completado: Bronze -> Silver -> Gold")
        return 0
    finally:
        if not ok:
            exc = sys.exc_info()[1]
            if exc is not None:
                registro["error"] = f"{type(exc).__name__}: {exc}"[:500]
            print("[batch] Pipeline con errores; ver registro en runs.json")
        spark.stop()
        registro["fin"] = datetime.now().isoformat(timespec="seconds")
        registro["duracion_s"] = round(time.time() - t0, 1)
        if ok:
            registro["estado"] = "ok"
        _log_run(registro)


if __name__ == "__main__":
    raise SystemExit(main())
