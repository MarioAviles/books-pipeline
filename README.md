# 📚 Books Pipeline

Este proyecto construye una *pipeline* de integración de datos para consolidar información de libros obtenida desde **Goodreads** y **Google Books API**, generando un modelo canónico `dim_book.parquet` y métricas de calidad documentadas en `docs/`.

---

## 🧱 Estructura del proyecto

BOOKS-PIPELINE/
│
├── 📂 docs/
│ ├── quality_metrics.json → métricas de calidad generadas automáticamente
│ └── schema.md → esquema documentado de la tabla final
│
├── 📂 landing/
│ ├── goodreads_books.json → fuente bruta de Goodreads
│ └── googlebooks_books.csv → datos enriquecidos desde Google Books
│
├── 📂 src/
│ ├── enrich_googlebooks.py → enriquece los libros de Goodreads usando Google Books API
│ ├── integrate_pipeline.py → integra y normaliza todas las fuentes en un modelo canónico
│ ├── scrape_goodreads.py → extracción (scraping) desde Goodreads
│ │
│ └── 📂 utils/
│   ├── init.py → marca el paquete como importable
│   ├── utils_isbn.py → generación y validación de IDs canónicos (ISBN o hash)
│   ├── utils_normalize.py → funciones de limpieza y normalización de datos
│   └── utils_quality.py → cálculo de métricas y validaciones de calidad
│
├── 📂 standard/
│ ├── dim_book.parquet → tabla maestra de libros (modelo canónico)
│ └── book_source_detail.parquet → detalle de fuentes originales
│
└── requirements.txt → dependencias del proyecto

---

## ⚙️ Instalación y entorno

### 1️⃣ Crear entorno virtual

```bash
python -m venv venv
```

### 2️⃣ Activar entorno virtual

- En Windows:

```bash
venv\Scripts\activate
```

- En macOS/Linux:

```bash
source venv/bin/activate
```

### 3️⃣ Instalar dependencias

```bash
pip install -r requirements.txt
```

## 🚀 Ejecución paso a

### 1️⃣ Scrapear o extraer datos de Goodreads

```bash
python src/scrape_goodreads.py
```

### 2️⃣ Enriquecer datos usando Google Books API

```bash
python src/enrich_googlebooks.py
```

### 3️⃣ Integrar y normalizar datos en el modelo canónico

```bash
python src/integrate_pipeline.py
```

## 📊 Resultados

- La tabla maestra `dim_book.parquet` se encuentra en el directorio `standard/`.
- Las métricas de calidad se encuentran en `docs/quality_metrics.json`.
- El esquema documentado está en `docs/schema.md`.
- El detalle de las fuentes originales está en `standard/book_source_detail.parquet`.
- Los datos brutos se encuentran en el directorio `landing/`.
- El código fuente está en el directorio `src/`.
- Las dependencias están listadas en `requirements.txt`.

#### 🧼 Limpieza automática de publisher

La función clean_publisher() normaliza los nombres de editoriales eliminando sufijos como Inc., Ltd., & Sons, Press, Media, etc.
Otras funciones en utils_normalize.py aseguran coherencia en autores, fechas, idiomas y precios.

#### 🧰 Scripts auxiliares

utils_isbn.py: Genera y valida identificadores canónicos (ISBN13 o hash SHA1).

utils_normalize.py: Limpieza de editoriales, autores, categorías, fechas, idiomas y precios.

utils_quality.py: Calcula métricas de completitud, duplicados y calidad general.

scrape_goodreads.py: Obtiene la fuente bruta de libros desde Goodreads.

enrich_googlebooks.py: Enriquecimiento de datos usando Google Books API.

integrate_pipeline.py: Une, limpia y estandariza las fuentes para generar el modelo final.



# Esquema de dim_book

Campo | Tipo | Nullable | Formato | Ejemplo | Reglas
---|---|---|---|---|---
book_id | object | No | string (ISBN-13 o hash) | 9781119741763 | Único, no nulo
title | object | No | string | Becoming a Data Head | Trim y capitalización correcta
subtitle | object | Sí | string o nulo | How to Think, Speak, and Understand Data Science | Opcional
publisher | object | Sí | string | Wiley | Normalizado y limpio
isbn13 | object | Sí | string (13 dígitos) | 9781119741763 | Validado por checksum
isbn10 | object | Sí | string (10 dígitos) | 1119741769 | Derivado o validado si existe
pub_date_norm | object | Sí | YYYY-MM-DD (ISO-8601) | 2021-04-13 | Debe ser fecha válida
language_norm | object | Sí | BCP-47 | en | Minúsculas; formato válido
price_amount_norm | float64 | Sí | decimal(10,2) | 27.99 | ≥ 0 o nulo
price_currency | object | Sí | ISO-4217 | EUR | Tres letras mayúsculas
categories | object | Sí | lista[string] | ['Business & Economics'] | Sin duplicados
authors | object | No | lista[string] | ['Alex J Gutman', 'Jordan Goldmeier'] | Sin duplicados ni nulos
fuente_ganadora | object | No | string (URL) | https://play.google.com/store/books/details?id=GCUqEAAAQBAJ | Debe ser URL válida
ts_ultima_actualizacion | object | No | timestamp ISO-8601 | 2025-11-19T10:56:30.416815 | Autogenerado


# Enlace repositorio GitHub
Repositorio GitHub: https://github.com/MarioAviles/books-pipeline