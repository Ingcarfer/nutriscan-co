"""La limpieza (capa plata) corrige, anula o descarta cada tipo de error."""

import pytest

from nutriscan.bronce import leer_csv_off
from nutriscan.plata import limpiar
from nutriscan.spark import crear_spark

COLUMNAS = [
    "code", "product_name", "brands", "brands_tags", "brand_owner", "quantity",
    "categories_tags", "main_category", "countries_tags", "ingredients_text",
    "additives_tags", "pnns_groups_1", "pnns_groups_2", "food_groups_tags",
    "nutriscore_grade", "image_url", "nova_group", "additives_n", "created_t",
    "last_modified_t", "fat_100g", "saturated-fat_100g", "trans-fat_100g",
    "carbohydrates_100g", "sugars_100g", "added-sugars_100g", "fiber_100g",
    "proteins_100g", "salt_100g", "sodium_100g", "energy-kcal_100g", "energy-kj_100g",
]


def fila(**valores) -> str:
    return "\t".join(str(valores.get(c.replace("-", "_"), "")) for c in COLUMNAS)


FILAS = [
    # Producto colombiano normal, con sal pero sin sodio
    fila(code="7702011271662", countries_tags="en:colombia", nova_group=4, last_modified_t=200,
         fat_100g=20, saturated_fat_100g=10, carbohydrates_100g=60, sugars_100g=30,
         proteins_100g=6, salt_100g=1, energy_kcal_100g=450,
         ingredients_text="Harina de TRIGO, azúcar, aceite de palma", brands="Marca A,Otra"),
    # Versión vieja del mismo producto: debe eliminarse por duplicado
    fila(code="7702011271662", countries_tags="en:colombia", nova_group=4, last_modified_t=100),
    # Código inválido: debe descartarse
    fila(code="abc123", countries_tags="en:colombia"),
    # Azúcar > 100 g: se anula TODA la tabla nutricional (y NOVA 7 no existe)
    fila(code="7700000000017", countries_tags="en:peru,en:colombia", sugars_100g=150,
         fat_100g=5, energy_kcal_100g=300, categories_tags="en:beverages", nova_group=7),
    # Grasa saturada > grasa total: se anula solo la saturada. Energía en kJ.
    fila(code="7700000000024", countries_tags="en:peru", fat_100g=5, saturated_fat_100g=8,
         energy_kj_100g=418.4),
    # Sin energía reportada: se calcula con Atwater (10*4 + 5*4 + 2*9 = 78 kcal)
    fila(code="3017620422003", countries_tags="en:chile", carbohydrates_100g=10,
         proteins_100g=5, fat_100g=2, main_category="en:null"),
]


@pytest.fixture(scope="module")
def resultado(tmp_path_factory):
    spark = crear_spark("test-plata")
    archivo = tmp_path_factory.mktemp("plata") / "mini.csv"
    archivo.write_text("\t".join(COLUMNAS) + "\n" + "\n".join(FILAS) + "\n", encoding="utf-8")
    df, conteos = limpiar(leer_csv_off(spark, str(archivo)))
    filas = {r["code"]: r for r in df.collect()}
    yield filas, conteos
    spark.stop()


def test_descarta_codigo_invalido_y_duplicados(resultado):
    filas, conteos = resultado
    assert conteos["filas_bronce"] == 6
    assert conteos["codigo_invalido"] == 1
    assert conteos["duplicados_eliminados"] == 1
    assert conteos["filas_plata"] == 4
    assert filas["7702011271662"]["last_modified_t"] == 200  # quedó la versión más reciente


def test_anula_valores_imposibles(resultado):
    filas, conteos = resultado
    p = filas["7700000000017"]
    assert p["sugars_100g"] is None
    assert p["fat_100g"] is None and p["energy_kcal_100g"] is None  # tabla completa anulada
    assert p["energia_kcal"] is None
    assert p["nova_group"] is None
    assert conteos["sugars_100g_fuera_de_rango"] == 1
    assert conteos["tablas_nutricionales_anuladas"] == 1
    q = filas["7700000000024"]
    assert q["saturated_fat_100g"] is None and q["fat_100g"] == 5  # solo la parte
    assert conteos["saturated_fat_100g_mayor_que_fat_100g"] == 1


def test_deriva_sodio_y_energia(resultado):
    filas, _ = resultado
    assert filas["7702011271662"]["sodio_mg_100g"] == pytest.approx(400)  # 1 g sal / 2,5
    assert filas["7702011271662"]["energia_origen"] == "reportada_kcal"
    assert filas["7700000000024"]["energia_kcal"] == pytest.approx(100)  # 418,4 kJ
    assert filas["3017620422003"]["energia_kcal"] == pytest.approx(78)
    assert filas["3017620422003"]["energia_origen"] == "calculada_atwater"


def test_textos_y_pais(resultado):
    filas, _ = resultado
    co = filas["7702011271662"]
    assert co["ingredientes_norm"] == "harina de trigo, azucar, aceite de palma"
    assert co["marca"] == "marca a"
    assert co["es_colombia"] and co["pais_grupo"] == "colombia"
    assert filas["7700000000017"]["pais_grupo"] == "colombia"  # Colombia tiene prioridad
    assert filas["7700000000017"]["es_bebida"]
    assert filas["3017620422003"]["pais_grupo"] == "chile"
    assert filas["3017620422003"]["main_category"] is None  # "en:null" → nulo
