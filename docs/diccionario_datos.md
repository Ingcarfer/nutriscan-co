# Diccionario de datos (capas plata y oro)

Fuente: volcado CSV de Open Food Facts (211 columnas originales). La capa plata conserva
las columnas útiles para el proyecto, con nombres sin guiones y con tipos correctos.
Todos los nutrientes están expresados **por 100 g (o 100 ml)**.

## Identificación y descripción

| Columna | Tipo | Descripción |
|---|---|---|
| `code` | texto | Código de barras (8 a 14 dígitos). Llave única tras quitar duplicados. |
| `product_name` | texto | Nombre del producto. |
| `brands`, `brands_tags` | texto | Marcas tal como se registraron. |
| `marca` | texto | Primera marca, en minúscula (derivada). |
| `brand_owner` | texto | Empresa dueña de la marca (poco diligenciado). |
| `quantity` | texto | Contenido neto declarado (ej. "500 g"). |
| `categories_tags` / `categorias` | texto / lista | Categorías jerárquicas (`en:snacks`, ...). |
| `main_category` | texto | Categoría principal según Open Food Facts. |
| `categoria` | texto | Grupo PNNS nivel 2 (capa oro); nulo si es "unknown". |
| `pnns_groups_1`, `pnns_groups_2` | texto | Grupos de alimentos del sistema francés PNNS. |
| `countries_tags` / `paises` | texto / lista | Países donde se vende el producto. |
| `es_colombia` | booleano | Verdadero si `paises` incluye `en:colombia`. |
| `pais_grupo` | texto | colombia, chile, mexico, peru u otros (Colombia tiene prioridad). **Partición** del Parquet. |
| `image_url` | texto | Foto del producto. |
| `created_t`, `last_modified_t` | entero | Fechas de creación y última modificación (Unix). |

## Ingredientes y procesamiento

| Columna | Tipo | Descripción |
|---|---|---|
| `ingredients_text` | texto | Lista de ingredientes en texto libre (cualquier idioma). |
| `ingredientes_norm` | texto | Igual, en minúscula y sin tildes (entrada del modelo). |
| `additives_tags` / `aditivos` | texto / lista | Aditivos detectados (códigos E, ej. `en:e955`). |
| `additives_n` | entero | Número de aditivos. |
| `nova_group` | entero 1–4 | Grupo NOVA calculado por Open Food Facts (4 = ultraprocesado). **Variable objetivo**. |
| `nutriscore_grade` | texto | Nutri-Score (a–e), solo como referencia. |

## Nutrientes (por 100 g)

| Columna | Unidad | Descripción |
|---|---|---|
| `energy_kcal_100g`, `energy_kj_100g` | kcal, kJ | Energía reportada. |
| `energia_kcal` | kcal | Energía usada en los cálculos: reportada → convertida de kJ (÷ 4,184) → calculada con Atwater (4·carb + 4·prot + 9·grasa). |
| `energia_origen` | texto | De dónde salió `energia_kcal`. |
| `fat_100g`, `saturated_fat_100g`, `trans_fat_100g` | g | Grasa total, saturada y trans. |
| `carbohydrates_100g`, `sugars_100g`, `added_sugars_100g` | g | Carbohidratos, azúcares totales y añadidos (estos casi nunca están). |
| `fiber_100g`, `proteins_100g` | g | Fibra y proteína. |
| `salt_100g`, `sodium_100g` | g | Sal y sodio. Si falta el sodio se calcula como sal ÷ 2,5. |
| `sodio_mg_100g` | mg | Sodio en miligramos (unidad de la norma). |
| `es_bebida` | booleano | Categoría `en:beverages` (regla especial de sodio). |

## Sellos (capa oro)

| Columna | Tipo | Descripción |
|---|---|---|
| `sello_azucares`, `sello_grasas_saturadas`, `sello_grasas_trans`, `sello_sodio`, `sello_edulcorantes` | booleano o nulo | Sello estimado según `config/sellos.yaml`. Nulo = no evaluable (falta un dato). |
| `n_sellos` | entero | Número de sellos (0 si la norma no aplica). |
| `aplica` | booleano | Falso para NOVA 1 (alimentos sin procesar). |
| `evaluable` | booleano | Tiene energía, azúcares, grasa saturada y sodio. |
| `con_sellos` | booleano | `n_sellos ≥ 1`. |

## Reglas de depuración

1. Código de barras inválido → la fila se descarta.
2. Código repetido → se conserva la versión con `last_modified_t` más reciente.
3. Un nutriente imposible (< 0, > 100 g, > 900 kcal) → se anula **toda** la tabla nutricional
   del producto (probable error de digitación por porción o por paquete).
4. Una parte mayor que el todo (saturada > grasa, azúcares > carbohidratos) → se anula esa parte.
5. NOVA fuera de 1–4 → nulo.
6. No se imputan valores faltantes de nutrientes: un producto sin dato queda "no evaluable".
