"""Constructor de SparkSession para los jobs batch y streaming."""

import glob
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    jars = ",".join(sorted(glob.glob("/opt/spark-jars/*.jar")))
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.jars", jars)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "768m"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.ansi.enabled", "false")
        # Reescritura dinámica de particiones: en Bronze solo se reemplaza la
        # partición del día que se está ingiriendo, sin borrar días anteriores.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", os.environ.get("TZ", "Europe/Madrid"))
    )
    return builder.getOrCreate()
