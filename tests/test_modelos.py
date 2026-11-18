import pytest

from nutriscan.modelos import evaluar, metricas
from nutriscan.spark import crear_spark


def test_metricas_calculadas_a_mano():
    # 80 aciertos NOVA 4, 20 falsos positivos, 10 falsos negativos, 90 aciertos del resto
    m = metricas(tp=80, fp=20, fn=10, tn=90)
    assert m["precision"] == pytest.approx(0.8)
    assert m["recall"] == pytest.approx(80 / 90, abs=1e-4)
    assert m["f1"] == pytest.approx(2 * 0.8 * (80 / 90) / (0.8 + 80 / 90), abs=1e-4)
    assert m["exactitud"] == pytest.approx(170 / 200)


def test_linea_base_mayoritaria_tiene_f1_alto_pero_macro_bajo():
    # Si el 75 % es NOVA 4, predecir siempre 4 da F1 de 0,857 sin aprender nada:
    # por eso se reporta también el F1 macro (promedio de ambas clases).
    m = metricas(tp=75, fp=25, fn=0, tn=0)
    assert m["f1"] == pytest.approx(0.8571, abs=1e-4)
    assert m["f1_macro"] == pytest.approx(0.4286, abs=1e-4)


def test_evaluar_con_spark():
    spark = crear_spark("test-modelos")
    pred = spark.createDataFrame(
        [(1.0, 1.0), (1.0, 1.0), (1.0, 0.0), (0.0, 0.0), (0.0, 1.0)], ["label", "prediction"])
    m = evaluar(pred)
    assert (m["tp"], m["fn"], m["tn"], m["fp"]) == (2, 1, 1, 1)
    spark.stop()
