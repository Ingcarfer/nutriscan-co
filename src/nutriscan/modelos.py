"""Paso 5: clasificar productos ultraprocesados (NOVA 4 frente al resto) a partir
de sus ingredientes, con Spark MLlib.

Uso (desde la raíz del repositorio):
    python -m nutriscan.modelos

Modelos que se comparan:
  0. Línea base: siempre predice la clase más frecuente del entrenamiento.
  1. TF-IDF + regresión logística (modelo base del anteproyecto).
  2. TF-IDF + red neuronal (perceptrón multicapa de Spark MLlib).

Siguiendo la retroalimentación de la docente, se reportan POR SEPARADO:
  - "global":   prueba con productos del mundo nunca vistos (15 %).
  - "colombia": TODOS los productos colombianos. Ninguno se usa para entrenar,
                así se mide si el modelo generaliza al mercado colombiano.
  - "ruido_*":  la prueba global y la de Colombia con ingredientes dañados a
                propósito (errores de letras o lista cortada), porque las
                etiquetas NOVA de Open Food Facts se calculan con reglas sobre el
                texto limpio y un F1 alto con texto limpio dice poco.

Salidas en reports/modelos/: metricas.csv, matrices.json, palabras_clave.csv.
"""

import json
import time

import pandas as pd
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, MultilayerPerceptronClassifier
from pyspark.ml.feature import IDF, CountVectorizer, NGram, RegexTokenizer, SQLTransformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from nutriscan.config import RAIZ, cargar_proyecto, ruta
from nutriscan.ruido import degradar
from nutriscan.spark import crear_spark


def metricas(tp: int, fp: int, fn: int, tn: int) -> dict:
    """Métricas para la clase positiva (NOVA 4) y F1 promedio de ambas clases."""
    def f1(p, r):
        return 2 * p * r / (p + r) if p + r else 0.0
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    prec_neg = tn / (tn + fn) if tn + fn else 0.0
    rec_neg = tn / (tn + fp) if tn + fp else 0.0
    n = tp + fp + fn + tn
    return {
        "n": n, "precision": round(prec, 4), "recall": round(rec, 4), "f1": round(f1(prec, rec), 4),
        "f1_macro": round((f1(prec, rec) + f1(prec_neg, rec_neg)) / 2, 4),
        "exactitud": round((tp + tn) / n, 4) if n else 0.0,
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
    }


def evaluar(pred: DataFrame) -> dict:
    """pred necesita las columnas label y prediction (0/1)."""
    c = {(int(r["label"]), int(r["prediction"])): r["count"]
         for r in pred.groupBy("label", "prediction").count().collect()}
    return metricas(c.get((1, 1), 0), c.get((0, 1), 0), c.get((1, 0), 0), c.get((0, 0), 0))


def preparar_datos(spark) -> tuple[DataFrame, DataFrame, DataFrame, DataFrame]:
    m = cargar_proyecto()["modelado"]
    df = (
        spark.read.parquet(str(ruta("gold") / "productos"))
        .filter(F.col("ingredientes_norm").isNotNull() & F.col("nova_group").isNotNull())
        .filter(F.length("ingredientes_norm") >= 3)
        .select("code", "ingredientes_norm", "es_colombia", "pais_grupo",
                (F.col("nova_group") == 4).cast("double").alias("label"))
    )
    colombia = df.filter("es_colombia")
    mundo = df.filter("NOT es_colombia")
    p = m["particion"]
    entrena, valida, prueba = mundo.randomSplit(
        [p["entrenamiento"], p["validacion"], p["prueba"]], seed=m["semilla"])
    n = entrena.count()
    if n > m["max_entrenamiento"]:
        entrena = entrena.sample(fraction=m["max_entrenamiento"] / n, seed=m["semilla"])
    return entrena, valida, prueba, colombia


def pipeline_texto(m: dict) -> list:
    """Texto → palabras → pares de palabras → conteos → TF-IDF."""
    return [
        # \p{L} = cualquier letra (de cualquier idioma); \p{N} = cualquier número
        RegexTokenizer(inputCol="ingredientes_norm", outputCol="palabras",
                       pattern=r"[^\p{L}\p{N}]+", minTokenLength=2),
        NGram(n=2, inputCol="palabras", outputCol="pares"),
        # Une palabras sueltas y pares de palabras en una sola lista
        SQLTransformer(statement="SELECT *, concat(palabras, pares) AS terminos FROM __THIS__"),
        CountVectorizer(inputCol="terminos", outputCol="conteos",
                        vocabSize=m["vocabulario"], minDF=m["min_documentos"]),
        IDF(inputCol="conteos", outputCol="features"),
    ]


def con_ruido(df: DataFrame, nivel: float, tipo: str, semilla: int) -> DataFrame:
    dañar = F.udf(lambda t: degradar(t, nivel, tipo, semilla), T.StringType())
    return df.withColumn("ingredientes_norm", dañar("ingredientes_norm"))


def main():
    inicio = time.time()
    cfg = cargar_proyecto()["modelado"]
    salida = ruta("reportes") / "modelos"
    salida.mkdir(parents=True, exist_ok=True)
    carpeta_modelos = RAIZ / "models"
    spark = crear_spark("modelos")

    entrena, valida, prueba, colombia = preparar_datos(spark)
    entrena = entrena.cache()
    tamaños = {"entrenamiento": entrena.count(), "validacion": valida.count(),
               "prueba": prueba.count(), "colombia": colombia.count()}
    print("Tamaños:", tamaños)

    # Línea base: la clase más frecuente
    prop_nova4 = entrena.agg(F.avg("label")).first()[0]
    clase_mayoritaria = 1.0 if prop_nova4 >= 0.5 else 0.0

    # Vectorizar una sola vez (las mismas variables para los dos modelos)
    texto = Pipeline(stages=pipeline_texto(cfg)).fit(entrena)
    texto.write().overwrite().save(str(carpeta_modelos / "texto"))
    vocab = texto.stages[3].vocabulary
    n_vars = len(vocab)
    tr = texto.transform(entrena).select("features", "label").cache()

    lr_cfg, rn_cfg = cfg["regresion_logistica"], cfg["red_neuronal"]
    t0 = time.time()
    lr = LogisticRegression(maxIter=lr_cfg["iteraciones"], regParam=lr_cfg["regularizacion"]).fit(tr)
    seg_lr = time.time() - t0
    t0 = time.time()
    rn = MultilayerPerceptronClassifier(
        layers=[n_vars, *rn_cfg["capas_ocultas"], 2], maxIter=rn_cfg["iteraciones"],
        blockSize=rn_cfg["tam_bloque"], seed=cfg["semilla"]).fit(tr)
    seg_rn = time.time() - t0
    lr.write().overwrite().save(str(carpeta_modelos / "regresion_logistica"))
    rn.write().overwrite().save(str(carpeta_modelos / "red_neuronal"))

    # Conjuntos de evaluación: limpios y con ruido
    conjuntos = {"validacion": valida, "global": prueba, "colombia": colombia}
    for nivel in cfg["ruido"]["niveles"]:
        conjuntos[f"global_errores_{int(nivel * 100)}"] = con_ruido(prueba, nivel, "errores", cfg["semilla"])
        conjuntos[f"colombia_errores_{int(nivel * 100)}"] = con_ruido(colombia, nivel, "errores", cfg["semilla"])
    for nivel in cfg["ruido"]["truncado"]:
        conjuntos[f"global_truncado_{int(nivel * 100)}"] = con_ruido(prueba, nivel, "truncado", cfg["semilla"])
        conjuntos[f"colombia_truncado_{int(nivel * 100)}"] = con_ruido(colombia, nivel, "truncado", cfg["semilla"])

    filas, matrices = [], {}
    for nombre_conj, conj in conjuntos.items():
        x = texto.transform(conj).select("features", "label").cache()
        modelos = {
            "linea_base": x.withColumn("prediction", F.lit(clase_mayoritaria)),
            "regresion_logistica": lr.transform(x),
            "red_neuronal": rn.transform(x),
        }
        for nombre_mod, pred in modelos.items():
            met = evaluar(pred)
            filas.append({"modelo": nombre_mod, "conjunto": nombre_conj, **met})
            matrices[f"{nombre_mod}|{nombre_conj}"] = [[met["tn"], met["fp"]], [met["fn"], met["tp"]]]
            print(f"{nombre_conj:24s} {nombre_mod:20s} F1={met['f1']:.3f} F1macro={met['f1_macro']:.3f}")
        x.unpersist()

    pd.DataFrame(filas).to_csv(salida / "metricas.csv", index=False)
    (salida / "matrices.json").write_text(json.dumps(matrices, indent=2))

    # Palabras que más empujan hacia "ultraprocesado" y hacia "no ultraprocesado"
    coef = lr.coefficients.toArray()
    pal = pd.DataFrame({"termino": vocab, "coeficiente": coef}).sort_values("coeficiente")
    pd.concat([pal.head(25), pal.tail(25)]).to_csv(salida / "palabras_clave.csv", index=False)

    resumen = {
        "tamaños": tamaños, "variables_tfidf": n_vars,
        "proporcion_nova4_entrenamiento": round(prop_nova4, 4),
        "clase_mayoritaria": int(clase_mayoritaria),
        "segundos_entrenamiento": {"regresion_logistica": round(seg_lr, 1), "red_neuronal": round(seg_rn, 1)},
        "capas_red": [n_vars, *rn_cfg["capas_ocultas"], 2],
        "segundos_total": round(time.time() - inicio, 1),
    }
    (salida / "resumen.json").write_text(json.dumps(resumen, indent=2, ensure_ascii=False))
    print(json.dumps(resumen, indent=2, ensure_ascii=False))
    spark.stop()


def cargar_modelos(spark=None):
    """Para la API: devuelve (texto, regresion_logistica, red_neuronal)."""
    from pyspark.ml.classification import LogisticRegressionModel, MultilayerPerceptronClassificationModel
    base = RAIZ / "models"
    return (PipelineModel.load(str(base / "texto")),
            LogisticRegressionModel.load(str(base / "regresion_logistica")),
            MultilayerPerceptronClassificationModel.load(str(base / "red_neuronal")))


if __name__ == "__main__":
    main()
