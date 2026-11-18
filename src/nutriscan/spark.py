"""Creación de la sesión de Spark con la configuración del proyecto."""

from pyspark.sql import SparkSession

from nutriscan.config import cargar_proyecto


def crear_spark(app_name: str | None = None) -> SparkSession:
    """Devuelve una sesión de Spark local que usa todos los núcleos del equipo."""
    cfg = cargar_proyecto()["spark"]
    return (
        SparkSession.builder.appName(app_name or cfg["app_name"])
        .master("local[*]")
        .config("spark.driver.memory", cfg["driver_memory"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        # Parquet comprimido con snappy (rápido y estándar)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
