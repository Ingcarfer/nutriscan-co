"""Paso 4: capa plata → capa oro (sellos + indicadores listos para analizar).

Uso (desde la raíz del repositorio):
    python -m nutriscan.oro

Genera en data/gold/:
- productos/        todos los productos limpios + sellos (Parquet, para la API)
- indicadores/*.csv tablas pequeñas para el EDA, el tablero y el informe
Y en reports/cobertura_colombia.json, la medición que pidió la docente.
"""

import json
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from nutriscan.config import ruta
from nutriscan.sellos import SELLOS, agregar_sellos_spark
from nutriscan.spark import crear_spark

MIN_PRODUCTOS_GRUPO = 20  # una categoría o marca con menos productos no se reporta


def pct(cond) -> F.Column:
    """Porcentaje de filas que cumplen una condición (los nulos cuentan como no)."""
    return F.round(F.avg(F.coalesce(cond.cast("double"), F.lit(0.0))) * 100, 1)


def preparar(plata: DataFrame) -> DataFrame:
    df = agregar_sellos_spark(plata)
    # Categoría: grupos PNNS (clasificación estándar de Open Food Facts, ~40 grupos).
    # main_category es demasiado detallada: casi ninguna llega a 20 productos en Colombia.
    pnns = F.col("pnns_groups_2")
    return df.withColumn("categoria", F.when(pnns.isNotNull() & (pnns != "unknown"), pnns)) \
             .withColumn("tiene_ingredientes", F.col("ingredients_text").isNotNull()) \
             .withColumn("tiene_nova", F.col("nova_group").isNotNull()) \
             .withColumn("con_sellos", F.col("n_sellos") >= 1)


def cobertura(df: DataFrame) -> DataFrame:
    """Completitud de las variables clave por grupo de país."""
    campos = {
        "nombre": "product_name", "marca": "marca", "categoria": "categoria",
        "ingredientes": "ingredients_text", "nova": "nova_group", "energia": "energia_kcal",
        "azucares": "sugars_100g", "grasa_saturada": "saturated_fat_100g",
        "grasa_trans": "trans_fat_100g", "sodio": "sodio_mg_100g", "aditivos": "aditivos",
    }
    return df.groupBy("pais_grupo").agg(
        F.count("*").alias("productos"),
        *[pct(F.col(c).isNotNull()).alias(f"pct_{k}") for k, c in campos.items()],
        pct(F.col("evaluable")).alias("pct_evaluable_sellos"),
    ).orderBy(F.desc("productos"))


def nova_por_pais(df: DataFrame) -> DataFrame:
    return (df.filter("tiene_nova").groupBy("pais_grupo", "nova_group").count()
            .withColumn("pct", F.round(F.col("count") * 100 / F.sum("count").over(
                Window.partitionBy("pais_grupo")), 1))
            .orderBy("pais_grupo", "nova_group"))


def sellos_por(df: DataFrame, grupo: str, minimo: int = 1) -> DataFrame:
    """% de productos con cada sello, sobre los productos evaluables a los que aplica la norma."""
    base = df.filter("evaluable AND aplica").filter(F.col(grupo).isNotNull())
    return (
        base.groupBy(grupo).agg(
            F.count("*").alias("productos"),
            pct(F.col("con_sellos")).alias("pct_con_algun_sello"),
            F.round(F.avg("n_sellos"), 2).alias("sellos_promedio"),
            *[pct(F.col(f"sello_{k}")).alias(f"pct_{k}") for k in SELLOS],
            pct(F.col("nova_group") == 4).alias("pct_nova4"),
        ).filter(F.col("productos") >= minimo).orderBy(F.desc("productos"))
    )


def guardar_csv(df: DataFrame, nombre: str):
    destino = ruta("gold") / "indicadores"
    destino.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(destino / f"{nombre}.csv", index=False)


def main():
    inicio = time.time()
    spark = crear_spark("oro")
    plata = spark.read.parquet(str(ruta("silver") / "productos"))
    df = preparar(plata)
    df.write.mode("overwrite").partitionBy("pais_grupo").parquet(str(ruta("gold") / "productos"))
    df = spark.read.parquet(str(ruta("gold") / "productos")).cache()

    co = df.filter("es_colombia")
    guardar_csv(cobertura(df), "cobertura_por_pais")
    guardar_csv(nova_por_pais(df), "nova_por_pais")
    guardar_csv(sellos_por(df, "pais_grupo"), "sellos_por_pais")
    guardar_csv(sellos_por(co, "categoria", MIN_PRODUCTOS_GRUPO), "sellos_colombia_por_categoria")
    guardar_csv(sellos_por(co, "marca", MIN_PRODUCTOS_GRUPO), "sellos_colombia_por_marca")
    guardar_csv(
        co.filter("evaluable AND aplica").groupBy("n_sellos").count().orderBy("n_sellos"),
        "colombia_numero_de_sellos")
    guardar_csv(
        co.filter("tiene_nova AND evaluable AND aplica").groupBy("nova_group")
        .agg(F.count("*").alias("productos"), pct(F.col("con_sellos")).alias("pct_con_algun_sello"))
        .orderBy("nova_group"), "colombia_sellos_por_nova")

    # Cobertura de Colombia (punto 2 de la retroalimentación)
    total = co.count()
    resumen = {
        "productos_colombia": total,
        "con_ingredientes": co.filter("tiene_ingredientes").count(),
        "con_nova": co.filter("tiene_nova").count(),
        "con_ingredientes_y_nova": co.filter("tiene_ingredientes AND tiene_nova").count(),
        "evaluables_para_sellos": co.filter("evaluable").count(),
        "evaluables_y_aplica": co.filter("evaluable AND aplica").count(),
        "productos_mundo": df.count(),
        "segundos": round(time.time() - inicio, 1),
    }
    (ruta("reportes") / "cobertura_colombia.json").write_text(
        json.dumps(resumen, indent=2, ensure_ascii=False))
    print(json.dumps(resumen, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
