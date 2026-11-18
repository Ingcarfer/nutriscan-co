"""Lectura de los archivos de configuración (carpeta config/).

Uso:
    from nutriscan.config import cargar_proyecto, cargar_sellos, ruta
    cfg = cargar_proyecto()
    carpeta_bronce = ruta("bronze")
"""

from functools import lru_cache
from pathlib import Path

import yaml

# Carpeta raíz del repositorio (dos niveles arriba de este archivo: src/nutriscan/)
RAIZ = Path(__file__).resolve().parents[2]
CARPETA_CONFIG = RAIZ / "config"


def _leer_yaml(nombre: str) -> dict:
    with open(CARPETA_CONFIG / nombre, encoding="utf-8") as f:
        return yaml.safe_load(f)


@lru_cache
def cargar_proyecto() -> dict:
    """Parámetros generales (rutas, muestra, Spark, modelado)."""
    return _leer_yaml("proyecto.yaml")


@lru_cache
def cargar_sellos() -> dict:
    """Umbrales de los sellos de advertencia (Resolución 2492 de 2022)."""
    return _leer_yaml("sellos.yaml")


def ruta(capa: str) -> Path:
    """Ruta absoluta de una capa del data lake: raw, bronze, silver, gold o reportes."""
    return RAIZ / cargar_proyecto()["rutas"][capa]
