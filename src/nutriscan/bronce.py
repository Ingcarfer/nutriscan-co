"""Paso 2 de la ingesta: CSV crudo → capa bronce (Parquet).

Uso (desde la raíz del repositorio):
    python -m nutriscan.bronce

La capa bronce guarda los datos TAL CUAL vienen (todas las columnas como texto),
solo cambia el formato a Parquet, que es columnar y comprimido: después se puede
leer solo las columnas necesarias, mucho más rápido que el CSV.

Genera reports/bronce.json con el volumen real (filas, columnas y tamaños).
"""

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from nutriscan.config import cargar_proyecto, ruta
from nutriscan.spark import crear_spark


def leer_csv_off(spark: SparkSession, archivo: str) -> DataFrame:
    """Lee el volcado de Open Food Facts.

    Aunque se llama "CSV", en realidad está separado por tabuladores (TSV) y no
    usa comillas, por eso se desactivan. Todas las columnas se leen como texto:
    la conversión de tipos se hace en la capa plata.
    """
    return (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("quote", "")  # sin comillas
        .option("mode", "PERMISSIVE")  # una fila dañada no detiene el proceso
        .option("inferSchema", False)
        .csv(archivo)
    )


def tamano_carpeta(carpeta: Path) -> int:
    return sum(p.stat().st_size for p in carpeta.rglob("*") if p.is_file())


def construir_bronce(spark: SparkSession, archivo: str, destino: Path) -> dict:
    inicio = time.time()
    df = leer_csv_off(spark, archivo).withColumn(
        "_fecha_ingesta", F.lit(datetime.now(timezone.utc).isoformat(timespec="seconds"))
    )
    # El .gz no se puede dividir: Spark lo lee con un solo núcleo. Al repartirlo
    # en 32 pedazos, los pasos siguientes ya usan todos los núcleos.
    df.repartition(32).write.mode("overwrite").parquet(str(destino))

    bronce = spark.read.parquet(str(destino))
    filas = bronce.count()
    return {
        "archivo_origen": Path(archivo).name,
        "filas": filas,
        "columnas_origen": len(bronce.columns) - 1,  # sin _fecha_ingesta
        "filas_sin_codigo": bronce.filter(F.col("code").isNull()).count(),
        "bytes_parquet": tamano_carpeta(destino),
        "segundos": round(time.time() - inicio, 1),
    }


def main():
    raw = ruta("raw")
    archivo = raw / cargar_proyecto()["fuente"]["csv_url"].rsplit("/", 1)[-1]
    spark = crear_spark("bronce")
    info = construir_bronce(spark, str(archivo), ruta("bronze") / "productos")
    info["bytes_csv_gz"] = archivo.stat().st_size
    info["gb_csv_gz"] = round(info["bytes_csv_gz"] / 1e9, 2)
    info["gb_parquet"] = round(info["bytes_parquet"] / 1e9, 2)
    salida = ruta("reportes") / "bronce.json"
    salida.write_text(json.dumps(info, indent=2, ensure_ascii=False))
    print(json.dumps(info, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
