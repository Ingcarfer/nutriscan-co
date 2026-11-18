"""Prueba de humo: Spark arranca y puede escribir y leer Parquet."""

import pytest

from nutriscan.spark import crear_spark


@pytest.fixture(scope="module")
def spark():
    s = crear_spark("test")
    yield s
    s.stop()


def test_spark_escribe_y_lee_parquet(spark, tmp_path):
    df = spark.createDataFrame(
        [("7702011271662", "en:colombia", 12.5)], ["code", "countries_tags", "sugars_100g"]
    )
    destino = str(tmp_path / "prueba.parquet")
    df.write.mode("overwrite").parquet(destino)
    leido = spark.read.parquet(destino)
    assert leido.count() == 1
    assert leido.first()["code"] == "7702011271662"
