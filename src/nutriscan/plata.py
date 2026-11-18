"""Paso 3: capa bronce → capa plata (datos limpios).

Uso (desde la raíz del repositorio):
    python -m nutriscan.plata

Qué hace, en orden (cada paso queda contado en reports/plata.json):
 1. Selecciona las ~35 columnas útiles de las 211 originales.
 2. Valida el código de barras (solo dígitos, 8 a 14) y descarta el resto.
 3. Elimina duplicados por código: conserva la versión modificada más reciente.
 4. Convierte los nutrientes a número. Si alguno es imposible (negativo,
    >100 g por 100 g o >900 kcal) se ANULA la tabla nutricional completa del
    producto; si una parte supera al todo (grasa saturada > grasa total) se
    anula solo esa parte. No se inventan valores: un dato malo pasa a ser nulo.
 5. Completa el sodio a partir de la sal (sodio = sal / 2,5) y la energía en
    kcal a partir de los kJ o, si no hay, con los factores de Atwater.
 6. Normaliza textos ("en:null" y vacíos → nulo; ingredientes en minúscula
    y sin tildes para el modelo) y marca el país de interés.
 7. Guarda en Parquet particionado por grupo de país.
"""

import json
import time

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from nutriscan.config import cargar_proyecto, cargar_sellos, ruta
from nutriscan.spark import crear_spark

TEXTOS = [
    "code", "product_name", "brands", "brands_tags", "brand_owner", "quantity",
    "categories_tags", "main_category", "countries_tags", "ingredients_text",
    "additives_tags", "pnns_groups_1", "pnns_groups_2", "food_groups_tags",
    "nutriscore_grade", "image_url",
]
ENTEROS = ["nova_group", "additives_n", "created_t", "last_modified_t"]

# Nutrientes por 100 g. Todos deben estar entre 0 y 100 g.
NUTRIENTES_G = [
    "fat_100g", "saturated-fat_100g", "trans-fat_100g", "carbohydrates_100g",
    "sugars_100g", "added-sugars_100g", "fiber_100g", "proteins_100g",
    "salt_100g", "sodium_100g",
]
ENERGIA = ["energy-kcal_100g", "energy-kj_100g"]

MAX_KCAL = 900  # 100 g de grasa pura = 900 kcal: nada puede tener más


def nombre(col: str) -> str:
    """'saturated-fat_100g' → 'saturated_fat_100g' (los guiones complican Spark SQL)."""
    return col.replace("-", "_")


def texto_limpio(c: Column) -> Column:
    t = F.trim(c)
    return F.when((t == "") | (t == "en:null") | (t == "null"), None).otherwise(t)


def sin_tildes(c: Column) -> Column:
    return F.translate(F.lower(c), "áàäâãéèëêíìïîóòöôõúùüûñç", "aaaaaeeeeiiiiooooouuuunc")


def limpiar(bronce: DataFrame) -> tuple[DataFrame, dict]:
    paises = cargar_proyecto()["paises"]
    conteos = {"filas_bronce": bronce.count()}

    # 1. Selección y tipos
    df = bronce.select(
        *[texto_limpio(F.col(c)).alias(c) for c in TEXTOS],
        *[texto_limpio(F.col(c)).cast("long").alias(c) for c in ENTEROS],
        *[texto_limpio(F.col(f"`{c}`")).cast("double").alias(nombre(c)) for c in NUTRIENTES_G + ENERGIA],
    )

    # 2. Código de barras válido
    df = df.withColumn("code", F.regexp_replace("code", r"\s", ""))
    valido = F.col("code").rlike(r"^[0-9]{8,14}$")
    conteos["codigo_invalido"] = df.filter(~valido | F.col("code").isNull()).count()
    df = df.filter(valido)

    # 3. Duplicados: quedarse con la última modificación de cada código
    antes = df.count()
    df = (
        df.withColumn("_rn", F.row_number().over(
            Window.partitionBy("code").orderBy(F.col("last_modified_t").desc_nulls_last())))
        .filter("_rn = 1").drop("_rn")
    )
    conteos["duplicados_eliminados"] = antes - df.count()

    # 4. Valores imposibles.
    # Si UN nutriente es imposible (p. ej. 272 g de carbohidratos en 100 g), lo más
    # probable es que toda la tabla se haya digitado por paquete y no por 100 g:
    # se anula la tabla nutricional completa de ese producto (el producto se
    # conserva para el análisis de ingredientes y NOVA).
    limites = {nombre(c): (0, 100) for c in NUTRIENTES_G}
    limites |= {"energy_kcal_100g": (0, MAX_KCAL), "energy_kj_100g": (0, MAX_KCAL * 4.184)}
    malo = {c: (F.col(c) < lo) | (F.col(c) > hi) for c, (lo, hi) in limites.items()}
    panel_invalido = F.lit(False)
    for cond in malo.values():
        panel_invalido = panel_invalido | F.coalesce(cond, F.lit(False))
    nova_mala = (F.col("nova_group") < 1) | (F.col("nova_group") > 4)
    incoherencias = {  # la parte no puede superar al todo (0,1 g de tolerancia por redondeo)
        f"{p}_mayor_que_{t}": (p, F.col(p) > F.col(t) + 0.1)
        for p, t in [("saturated_fat_100g", "fat_100g"), ("trans_fat_100g", "fat_100g"),
                     ("sugars_100g", "carbohydrates_100g")]
    }

    # Todos los conteos en una sola pasada por los datos
    def contar(cond):
        return F.sum(F.coalesce(cond, F.lit(False)).cast("int"))
    fila = df.agg(
        contar(panel_invalido).alias("tablas_nutricionales_anuladas"),
        *[contar(c).alias(f"{k}_fuera_de_rango") for k, c in malo.items()],
        contar(nova_mala).alias("nova_group_fuera_de_rango"),
        *[contar(c & ~panel_invalido).alias(k) for k, (_, c) in incoherencias.items()],
    ).first().asDict()
    conteos |= {k: int(v or 0) for k, v in fila.items()}

    df = df.withColumn("_panel_invalido", panel_invalido)
    for c in limites:
        df = df.withColumn(c, F.when(F.col("_panel_invalido"), None).otherwise(F.col(c)))
    df = df.drop("_panel_invalido")
    df = df.withColumn("nova_group", F.when(nova_mala, None).otherwise(F.col("nova_group")))
    for parte, cond in incoherencias.values():
        df = df.withColumn(parte, F.when(cond, None).otherwise(F.col(parte)))

    # 5. Derivar sodio y energía
    conteos["sodio_derivado_de_sal"] = df.filter(
        F.col("sodium_100g").isNull() & F.col("salt_100g").isNotNull()).count()
    df = df.withColumn("sodium_100g", F.coalesce("sodium_100g", F.col("salt_100g") / 2.5))
    df = df.withColumn("sodio_mg_100g", F.col("sodium_100g") * 1000)

    at = cargar_sellos()["energia_kcal_por_gramo"]
    atwater = (F.col("carbohydrates_100g") * at["carbohidratos"]
               + F.col("proteins_100g") * at["proteinas"] + F.col("fat_100g") * at["grasas"])
    df = df.withColumn(
        "energia_origen",
        F.when(F.col("energy_kcal_100g").isNotNull(), "reportada_kcal")
        .when(F.col("energy_kj_100g").isNotNull(), "convertida_kj")
        .when(atwater.isNotNull(), "calculada_atwater"),
    ).withColumn(
        "energia_kcal",
        F.coalesce("energy_kcal_100g", F.col("energy_kj_100g") / 4.184, atwater),
    )
    conteos["energia_por_origen"] = {
        r["energia_origen"] or "sin_dato": r["count"]
        for r in df.groupBy("energia_origen").count().collect()
    }

    # 6. Textos y país
    df = (
        df.withColumn("ingredientes_norm", sin_tildes(F.col("ingredients_text")))
        .withColumn("paises", F.split("countries_tags", ","))
        .withColumn("categorias", F.split("categories_tags", ","))
        .withColumn("aditivos", F.split("additives_tags", ","))
        .withColumn("marca", F.lower(F.trim(F.split("brands", ",").getItem(0))))
        .withColumn("es_bebida", F.coalesce(F.array_contains("categorias", "en:beverages"), F.lit(False)))
        .withColumn("es_colombia", F.coalesce(F.array_contains("paises", paises["objetivo"]), F.lit(False)))
    )
    grupo = F.lit("otros")
    for p in reversed([paises["objetivo"], *paises["comparacion"]]):  # prioridad: Colombia primero
        grupo = F.when(F.array_contains("paises", p), p.split(":")[1]).otherwise(grupo)
    df = df.withColumn("pais_grupo", F.coalesce(grupo, F.lit("otros")))

    conteos["filas_plata"] = df.count()
    return df, conteos


def main():
    inicio = time.time()
    spark = crear_spark("plata")
    bronce = spark.read.parquet(str(ruta("bronze") / "productos"))
    df, conteos = limpiar(bronce)
    df.write.mode("overwrite").partitionBy("pais_grupo").parquet(str(ruta("silver") / "productos"))
    conteos["segundos"] = round(time.time() - inicio, 1)
    (ruta("reportes") / "plata.json").write_text(json.dumps(conteos, indent=2, ensure_ascii=False))
    print(json.dumps(conteos, indent=2, ensure_ascii=False))
    spark.stop()


if __name__ == "__main__":
    main()
