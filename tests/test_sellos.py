"""Motor de sellos: casos calculados a mano y equivalencia Python = Spark."""

import pytest

from nutriscan.sellos import SELLOS, agregar_sellos_spark, calcular_sellos
from nutriscan.spark import crear_spark

BASE = {
    "energia_kcal": 400.0, "sugars_100g": 0.0, "saturated_fat_100g": 0.0,
    "trans_fat_100g": 0.0, "sodio_mg_100g": 0.0, "es_bebida": False,
    "aditivos": [], "ingredientes_norm": "harina de trigo", "nova_group": 4,
}

CASOS = {
    # 10 g azúcar * 4 = 40 kcal = 10 % de 400 kcal → justo en el umbral: SÍ
    "azucar_en_umbral": ({"sugars_100g": 10.0, "ingredientes_norm": "harina, azucar"}, {"azucares": True}),
    # Mucho azúcar pero ningún azúcar añadido (leche entera: lactosa) → NO
    "azucar_intrinseco": ({"sugars_100g": 20.0, "ingredientes_norm": "leche entera"}, {"azucares": False}),
    # Sin ingredientes: se mantiene la aproximación con azúcares totales → SÍ
    "azucar_sin_ingredientes": ({"sugars_100g": 20.0, "ingredientes_norm": None}, {"azucares": True}),
    # 9 g → 9 %: NO
    "azucar_bajo": ({"sugars_100g": 9.0}, {"azucares": False}),
    # 5 g sat * 9 = 45 kcal = 11,25 %: SÍ
    "grasa_saturada": ({"saturated_fat_100g": 5.0}, {"grasas_saturadas": True}),
    # 0,5 g trans * 9 = 4,5 kcal = 1,1 %: SÍ
    "trans": ({"trans_fat_100g": 0.5}, {"grasas_trans": True}),
    # trans desconocida → se asume 0 → NO
    "trans_desconocida": ({"trans_fat_100g": None}, {"grasas_trans": False}),
    # sodio 350 mg con 400 kcal: 0,875 mg/kcal, pero ≥ 300 mg/100 g → SÍ
    "sodio_por_100g": ({"sodio_mg_100g": 350.0}, {"sodio": True}),
    # sodio 200 mg con 150 kcal: 1,33 mg/kcal → SÍ
    "sodio_por_kcal": ({"sodio_mg_100g": 200.0, "energia_kcal": 150.0}, {"sodio": True}),
    # bebida sin calorías con 50 mg de sodio → SÍ (umbral 40 mg/100 ml)
    "bebida_cero": ({"energia_kcal": 0.0, "sodio_mg_100g": 50.0, "es_bebida": True}, {"sodio": True}),
    # edulcorante por código de aditivo (sucralosa E955)
    "edulcorante_aditivo": ({"aditivos": ["en:e330", "en:e955"]}, {"edulcorantes": True}),
    # edulcorante por texto de ingredientes
    "edulcorante_texto": ({"ingredientes_norm": "agua, stevia, acido citrico"}, {"edulcorantes": True}),
    # sin energía: los sellos de nutrientes no se pueden evaluar
    "sin_energia": ({"energia_kcal": None, "sugars_100g": 50.0},
                    {"azucares": None, "sodio": None, "evaluable": False}),
    # NOVA 1 (sin procesar): no aplica, cero sellos aunque tenga mucho azúcar
    "nova1": ({"nova_group": 1, "sugars_100g": 50.0}, {"aplica": False, "n_sellos": 0}),
    # varios sellos a la vez
    "tres_sellos": ({"sugars_100g": 30.0, "saturated_fat_100g": 10.0, "sodio_mg_100g": 500.0,
                     "ingredientes_norm": "azucar, aceite, sal"},
                    {"n_sellos": 3, "evaluable": True}),
}


@pytest.mark.parametrize("nombre", CASOS)
def test_reglas_python(nombre):
    cambios, esperado = CASOS[nombre]
    r = calcular_sellos({**BASE, **cambios})
    for clave, valor in esperado.items():
        assert r[clave] == valor, f"{nombre}: {clave}={r[clave]}, se esperaba {valor}"


def test_spark_da_lo_mismo_que_python():
    spark = crear_spark("test-sellos")
    productos = [{"id": n, **BASE, **c} for n, (c, _) in CASOS.items()]
    esquema = ("id string, energia_kcal double, sugars_100g double, saturated_fat_100g double, "
               "trans_fat_100g double, sodio_mg_100g double, es_bebida boolean, "
               "aditivos array<string>, ingredientes_norm string, nova_group long")
    df = agregar_sellos_spark(spark.createDataFrame(productos, esquema))
    for fila in df.collect():
        py = calcular_sellos({**BASE, **CASOS[fila["id"]][0]})
        for k in SELLOS:
            assert fila[f"sello_{k}"] == py[k], f"{fila['id']}: sello {k}"
        assert fila["n_sellos"] == py["n_sellos"], fila["id"]
        assert fila["evaluable"] == py["evaluable"], fila["id"]
        assert fila["aplica"] == py["aplica"], fila["id"]
    spark.stop()
