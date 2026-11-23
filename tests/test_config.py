"""Verifica que los archivos de configuración existen y tienen lo necesario."""

from nutriscan.config import RAIZ, cargar_proyecto, cargar_sellos, ruta


def test_proyecto_tiene_secciones_basicas():
    cfg = cargar_proyecto()
    for seccion in ["fuente", "rutas", "paises", "spark", "modelado"]:
        assert seccion in cfg


def test_particion_suma_uno():
    p = cargar_proyecto()["modelado"]["particion"]
    assert abs(sum(p.values()) - 1.0) < 1e-9


def test_sellos_tiene_los_cinco_sellos():
    sellos = cargar_sellos()["sellos"]
    assert set(sellos) == {"azucares", "grasas_saturadas", "grasas_trans", "sodio", "edulcorantes"}


def test_umbrales_resolucion_2492():
    s = cargar_sellos()["sellos"]
    assert s["azucares"]["porcentaje_energia_minimo"] == 10
    assert s["grasas_saturadas"]["porcentaje_energia_minimo"] == 10
    assert s["grasas_trans"]["porcentaje_energia_minimo"] == 1
    assert s["sodio"]["mg_por_kcal_minimo"] == 1
    assert s["sodio"]["mg_por_100g_minimo"] == 300


def test_rutas_de_capas_existen():
    for capa in ["raw", "bronze", "silver", "gold", "reportes"]:
        assert ruta(capa).is_dir(), f"Falta la carpeta {ruta(capa).relative_to(RAIZ)}"
