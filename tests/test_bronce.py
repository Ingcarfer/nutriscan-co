"""La capa bronce lee el TSV de Open Food Facts y conserva todas las filas."""

import pytest

from nutriscan.bronce import construir_bronce, leer_csv_off
from nutriscan.spark import crear_spark

# Mini archivo con el mismo formato que el real (tabuladores, sin comillas).
# La segunda fila trae comillas sueltas, que en el volcado real son comunes.
TSV = (
    "code\tproduct_name\tcountries_tags\tsugars_100g\n"
    "7702011271662\tGalletas\ten:colombia\t30.5\n"
    '7702000000001\tJugo "natural\ten:colombia,en:peru\t\n'
    "3017620422003\tNutella\ten:france\t56.3\n"
)


@pytest.fixture(scope="module")
def spark():
    s = crear_spark("test-bronce")
    yield s
    s.stop()


def test_lee_tsv_con_comillas_sueltas(spark, tmp_path):
    archivo = tmp_path / "mini.csv"
    archivo.write_text(TSV, encoding="utf-8")
    df = leer_csv_off(spark, str(archivo))
    assert df.count() == 3
    fila = df.filter(df.code == "7702000000001").first()
    assert fila["product_name"] == 'Jugo "natural'
    assert fila["countries_tags"] == "en:colombia,en:peru"
    assert fila["sugars_100g"] is None  # celda vacía = nulo


def test_construir_bronce_reporta_volumen(spark, tmp_path):
    archivo = tmp_path / "mini.csv"
    archivo.write_text(TSV, encoding="utf-8")
    info = construir_bronce(spark, str(archivo), tmp_path / "bronce")
    assert info["filas"] == 3
    assert info["columnas_origen"] == 4
    assert info["bytes_parquet"] > 0
