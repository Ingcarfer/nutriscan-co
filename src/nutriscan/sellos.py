"""Motor de reglas de los sellos de advertencia (Resolución 2492 de 2022).

Hay dos versiones que usan EXACTAMENTE los mismos umbrales (config/sellos.yaml):
- calcular_sellos(): Python puro, para un solo producto (API / prototipo).
- agregar_sellos_spark(): Spark, para millones de productos a la vez.
Los tests verifican que ambas den el mismo resultado.

Supuestos (se declaran como limitaciones en el informe):
- Se usan azúcares TOTALES porque Open Food Facts no trae azúcares libres.
- Si falta grasa trans se asume 0 (no se puede afirmar exceso).
- Un sello de nutriente solo se evalúa si hay energía y ese nutriente; si no,
  queda como None ("no evaluable"), nunca como False.
- La norma aplica a productos procesados/ultraprocesados; los NOVA 1
  (sin procesar) se marcan como "no aplica".
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from nutriscan.config import cargar_sellos

SELLOS = ["azucares", "grasas_saturadas", "grasas_trans", "sodio", "edulcorantes"]


def _pct_energia(gramos, kcal_por_g, energia):
    return gramos * kcal_por_g / energia * 100


def calcular_sellos(p: dict, cfg: dict | None = None) -> dict:
    """p usa los nombres de la capa plata: energia_kcal, sugars_100g, saturated_fat_100g,
    trans_fat_100g, sodio_mg_100g, es_bebida, aditivos (lista), ingredientes_norm, nova_group."""
    s = (cfg or cargar_sellos())["sellos"]
    e = p.get("energia_kcal")
    r: dict = {k: None for k in SELLOS}

    def por_energia(clave, nutriente, defecto_cero=False):
        g = p.get(nutriente)
        if g is None and defecto_cero:
            g = 0.0
        if g is None or e is None or e <= 0:
            return None
        return _pct_energia(g, s[clave]["kcal_por_gramo"], e) >= s[clave]["porcentaje_energia_minimo"]

    r["azucares"] = por_energia("azucares", "sugars_100g")
    r["grasas_saturadas"] = por_energia("grasas_saturadas", "saturated_fat_100g")
    r["grasas_trans"] = por_energia("grasas_trans", "trans_fat_100g", defecto_cero=True)

    na, so = p.get("sodio_mg_100g"), s["sodio"]
    if na is not None and e is not None:
        if e > 0:
            r["sodio"] = na / e >= so["mg_por_kcal_minimo"] or na >= so["mg_por_100g_minimo"]
        elif p.get("es_bebida"):
            r["sodio"] = na >= so["bebidas_sin_calorias_mg_por_100ml_minimo"]
        else:
            r["sodio"] = na >= so["mg_por_100g_minimo"]

    ed = s["edulcorantes"]
    aditivos, texto = p.get("aditivos") or [], p.get("ingredientes_norm")
    if aditivos or texto:
        r["edulcorantes"] = any(a in ed["aditivos"] for a in aditivos) or any(
            k in (texto or "") for k in ed["palabras_clave"])

    r["aplica"] = p.get("nova_group") != 1
    evaluados = [r[k] for k in SELLOS if r[k] is not None]
    r["n_sellos"] = sum(evaluados) if r["aplica"] else 0
    r["evaluable"] = all(r[k] is not None for k in ["azucares", "grasas_saturadas", "sodio"])
    return r


def agregar_sellos_spark(df: DataFrame, cfg: dict | None = None) -> DataFrame:
    """Agrega las columnas sello_* , n_sellos, aplica y evaluable a un DataFrame de la capa plata."""
    s = (cfg or cargar_sellos())["sellos"]
    e = F.col("energia_kcal")
    hay_e = e.isNotNull() & (e > 0)

    def por_energia(clave, col: Column) -> Column:
        pct = col * s[clave]["kcal_por_gramo"] / e * 100
        return F.when(hay_e & col.isNotNull(), pct >= s[clave]["porcentaje_energia_minimo"])

    so = s["sodio"]
    na = F.col("sodio_mg_100g")
    sodio = (
        F.when(na.isNull() | e.isNull(), None)
        .when(e > 0, (na / e >= so["mg_por_kcal_minimo"]) | (na >= so["mg_por_100g_minimo"]))
        .when(F.col("es_bebida"), na >= so["bebidas_sin_calorias_mg_por_100ml_minimo"])
        .otherwise(na >= so["mg_por_100g_minimo"])
    )

    ed = s["edulcorantes"]
    por_aditivo = F.arrays_overlap(
        F.coalesce("aditivos", F.array().cast("array<string>")),
        F.array(*[F.lit(a) for a in ed["aditivos"]]))
    texto = F.coalesce(F.col("ingredientes_norm"), F.lit(""))
    por_texto = F.lit(False)
    for k in ed["palabras_clave"]:
        por_texto = por_texto | texto.contains(k)
    hay_info = F.col("aditivos").isNotNull() | F.col("ingredientes_norm").isNotNull()

    df = (
        df.withColumn("sello_azucares", por_energia("azucares", F.col("sugars_100g")))
        .withColumn("sello_grasas_saturadas", por_energia("grasas_saturadas", F.col("saturated_fat_100g")))
        .withColumn("sello_grasas_trans", por_energia("grasas_trans", F.coalesce("trans_fat_100g", F.lit(0.0))))
        .withColumn("sello_sodio", sodio)
        .withColumn("sello_edulcorantes", F.when(hay_info, por_aditivo | por_texto))
        .withColumn("aplica", F.coalesce(F.col("nova_group") != 1, F.lit(True)))
    )
    suma = sum(F.coalesce(F.col(f"sello_{k}").cast("int"), F.lit(0)) for k in SELLOS)
    return (
        df.withColumn("n_sellos", F.when(F.col("aplica"), suma).otherwise(0))
        .withColumn("evaluable", F.col("sello_azucares").isNotNull()
                    & F.col("sello_grasas_saturadas").isNotNull() & F.col("sello_sodio").isNotNull())
    )
