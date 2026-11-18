"""Paso 6: segmentación de productos por perfil nutricional con K-means (Spark MLlib).

Uso (desde la raíz del repositorio):
    python -m nutriscan.segmentacion

- Variables: energía, grasa, grasa saturada, carbohidratos, azúcares, proteína y
  sodio por 100 g (solo productos con las 7 completas).
- Se aplica log(1 + x) porque los nutrientes son muy asimétricos (muchos valores
  bajos y pocos muy altos), y luego se estandariza (media 0, desviación 1) para
  que ninguna variable pese más solo por su escala (las kcal llegan a 900).
- Se prueba k = 2..10 y se elige el k con mejor coeficiente de silueta.

Salidas en reports/segmentacion/: silueta.csv, perfiles.csv, colombia_por_grupo.csv
"""

import json
import time

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import functions as F

from nutriscan.config import cargar_proyecto, ruta
from nutriscan.spark import crear_spark

VARIABLES = ["energia_kcal", "fat_100g", "saturated_fat_100g", "carbohydrates_100g",
             "sugars_100g", "proteins_100g", "sodio_mg_100g"]


def main():
    inicio = time.time()
    cfg = cargar_proyecto()["modelado"]
    km = cfg["kmeans"]
    salida = ruta("reportes") / "segmentacion"
    salida.mkdir(parents=True, exist_ok=True)
    spark = crear_spark("segmentacion")

    df = spark.read.parquet(str(ruta("gold") / "productos")).dropna(subset=VARIABLES)
    total = df.count()
    if total > km["max_productos"]:
        # Muestra aleatoria del mundo, pero TODOS los productos colombianos
        df = df.filter("es_colombia").unionByName(
            df.filter("NOT es_colombia").sample(km["max_productos"] / total, seed=cfg["semilla"]))
    for v in VARIABLES:
        df = df.withColumn(f"log_{v}", F.log1p(F.col(v)))
    preparar = Pipeline(stages=[
        VectorAssembler(inputCols=[f"log_{v}" for v in VARIABLES], outputCol="crudo"),
        StandardScaler(inputCol="crudo", outputCol="features", withMean=True, withStd=True),
    ]).fit(df)
    x = preparar.transform(df).cache()
    n = x.count()

    evaluador = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    resultados = []
    for k in range(km["k_min"], km["k_max"] + 1):
        modelo = KMeans(k=k, seed=cfg["semilla"], featuresCol="features").fit(x)
        sil = evaluador.evaluate(modelo.transform(x))
        resultados.append({"k": k, "silueta": round(sil, 4), "inercia": round(modelo.summary.trainingCost, 1)})
        print(f"k={k}: silueta={sil:.4f}")
    pd.DataFrame(resultados).to_csv(salida / "silueta.csv", index=False)

    mejor_k = max(resultados, key=lambda r: r["silueta"])["k"]
    final = KMeans(k=mejor_k, seed=cfg["semilla"], featuresCol="features").fit(x)
    asignado = final.transform(x).withColumnRenamed("prediction", "grupo")

    # Perfil de cada grupo en unidades reales (medianas), para poder nombrarlos
    perfiles = asignado.groupBy("grupo").agg(
        F.count("*").alias("productos"),
        *[F.round(F.percentile_approx(v, 0.5), 1).alias(v) for v in VARIABLES],
        F.round(F.avg((F.col("nova_group") == 4).cast("double")) * 100, 1).alias("pct_nova4"),
        F.round(F.avg(F.col("con_sellos").cast("double")) * 100, 1).alias("pct_con_sellos"),
    ).orderBy("grupo")
    perfiles.toPandas().to_csv(salida / "perfiles.csv", index=False)
    asignado.filter("es_colombia").groupBy("grupo").count().orderBy("grupo") \
        .toPandas().to_csv(salida / "colombia_por_grupo.csv", index=False)

    resumen = {"productos_con_perfil_completo": total, "productos_usados": n, "mejor_k": mejor_k,
               "silueta_mejor_k": max(r["silueta"] for r in resultados),
               "segundos": round(time.time() - inicio, 1)}
    (salida / "resumen.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False))
    print(json.dumps(resumen, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
