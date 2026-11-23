# NutriScan CO

Análisis a gran escala del nivel de procesamiento (NOVA) y del etiquetado frontal de
advertencia de los alimentos empacados en Colombia, con la base mundial de
[Open Food Facts](https://world.openfoodfacts.org).

Proyecto final de **Ciencia de Datos y Big Data**, Facultad de Sistemas, ETITC (2026).

- Carlos Alfonso Fernández Salas: ingeniería de datos (ingesta, Spark, almacenamiento, backend)
- Santiago Sandoval Urrego: análisis y modelado (EDA, motor de sellos, modelos)

## Alcance

Según la retroalimentación de la docente, el alcance se divide en un **mínimo entregable**
y unos **extras opcionales**:

| Bloque | Contenido | Alcance |
|---|---|---|
| 0 | Repositorio, entorno, umbrales configurables | Mínimo |
| 1 | Ingesta, capa bronce, informe de calidad y **cobertura de Colombia** | Mínimo |
| 2 | Limpieza (capas plata y oro) y EDA de Colombia frente a Chile, México y Perú | Mínimo |
| 3 | Motor de sellos (Res. 2492/2022) con tests e indicadores por categoría y fabricante | Mínimo |
| 4 | TF-IDF + regresión logística, MLP y K-means. Evaluación **global, con ruido y Colombia, por separado** | Mínimo |
| 5 | API por código de barras (FastAPI) y tablero (Streamlit) | Mínimo |
| 6 | DistilBERT, app de escaneo con cámara/OCR, MongoDB | Opcional |

## Arquitectura (data lake por capas)

```
Open Food Facts (CSV ~9 GB)
        │  src/ (descarga)
        ▼
data/raw      → archivo original, sin tocar
data/bronze   → mismo contenido, en Parquet (rápido de leer)
data/silver   → datos limpios: unidades corregidas, sin duplicados
data/gold     → indicadores y tablas listas para el tablero y la API
```

## Estructura del repositorio

```
config/      Parámetros del proyecto y umbrales de los sellos (editables)
src/nutriscan/  Código Python reutilizable (Spark, sellos, modelos)
notebooks/   Análisis exploratorio y experimentos
app/         API y tablero
tests/       Pruebas automáticas (pytest)
reports/     Informes generados (calidad, cobertura, evaluación)
docs/        Documentación (diccionario de datos, guías)
data/        Datos (NO se suben a GitHub)
```

## Instalación (una sola vez)

Requisitos: Python 3.10 o superior y Java 17 o superior (se probó con Java 21).

```bash
cd nutriscan-co
python3 -m venv .venv               # crea el entorno virtual
source .venv/bin/activate           # lo activa (hacerlo cada vez que abras una terminal)
pip install -r requirements.txt     # instala las librerías
```

Para la red neuronal (Bloque 4), si se entrena en el PC y no en Colab:

```bash
pip install -r requirements-ml.txt --index-url https://download.pytorch.org/whl/cpu
```

## Verificar que todo funciona

```bash
source .venv/bin/activate
pytest
```

Todas las pruebas deben salir en verde (`passed`).

## Ejecutar el flujo completo

```bash
bash ejecutar_flujo.sh                      # descarga, bronce, plata, oro, modelos, segmentacion
bash ejecutar_flujo.sh oro modelos          # solo algunos pasos
```

Necesita unos 10 GB de disco y tarda alrededor de 35 minutos (más la descarga de 1,5 GB).
Cada paso guarda su registro en `reports/logs/`. Los resultados se ven en
`notebooks/01_eda.ipynb` y `notebooks/02_modelos.ipynb`. El informe de avance está en
`docs/avance/` (se genera desde `avance.html` con Chrome: Imprimir → Guardar como PDF).

## Configuración

- `config/proyecto.yaml`: rutas, tamaño de la muestra de desarrollo, países y memoria de Spark.
- `config/sellos.yaml`: umbrales de la Resolución 2492 de 2022. Si la norma cambia, se edita
  solo este archivo.

## Licencia de los datos

Los datos de Open Food Facts se publican bajo la
[Open Database License (ODbL)](https://opendatacommons.org/licenses/odbl/1-0/) y sus contenidos
bajo la Database Contents License. Toda base derivada se comparte con la misma licencia.
Los resultados del proyecto son **estimaciones informativas**, no un concepto oficial de
cumplimiento normativo.
