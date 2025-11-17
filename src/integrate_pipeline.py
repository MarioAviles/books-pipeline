import json
import pandas as pd
import os
import hashlib
import numpy as np
from datetime import datetime
from utils_quality import (
    validate_isbn13,
    validate_isbn10,
    normalize_date,
    normalize_language,
    normalize_price,
    detect_nulls,
    detect_duplicates,
    compute_quality_metrics,
)
from utils_isbn import canonical_book_id

LANDING_GOODREADS = "landing/goodreads_books.json"
LANDING_GOOGLE = "landing/googlebooks_books.csv"

OUT_DIM = "standard/dim_book.parquet"
OUT_DETAIL = "standard/book_source_detail.parquet"
OUT_QUALITY = "docs/quality_metrics.json"
OUT_SCHEMA = "docs/schema.md"


# ============================================================
# Conversión segura a tipos nativos JSON
# ============================================================
def to_python_native(obj):
    """
    Convierte numpy.int64, numpy.float64, pandas NA, etc.
    a tipos nativos serializables por JSON.
    """
    if isinstance(obj, dict):
        return {k: to_python_native(v) for k, v in obj.items()}

    elif isinstance(obj, list):
        return [to_python_native(v) for v in obj]

    elif isinstance(obj, (np.integer,)):
        return int(obj)

    elif isinstance(obj, (np.floating,)):
        return float(obj)

    elif pd.isna(obj):
        return None

    return obj


# ============================================================
# 1. CARGA DE DATOS
# ============================================================
def load_sources():
    print("[INFO] Cargando landing/ ...")

    goodreads = pd.read_json(LANDING_GOODREADS, dtype=str)
    google = pd.read_csv(LANDING_GOOGLE, sep=";", dtype=str)

    print(f"[INFO] Goodreads: {len(goodreads)} filas")
    print(f"[INFO] GoogleBooks: {len(google)} filas")

    return goodreads, google


# ============================================================
# 2. CREAR book_source_detail (Trazabilidad + Validaciones)
# ============================================================
def build_source_detail(goodreads, google):

    goodreads["source"] = "goodreads"
    google["source"] = "googlebooks"

    df = pd.concat([goodreads, google], ignore_index=True, sort=False)

    # flags de validación
    df["flag_isbn13_valid"] = df["isbn13"].apply(lambda x: validate_isbn13(x))
    df["flag_isbn10_valid"] = df["isbn10"].apply(lambda x: validate_isbn10(x))
    df["flag_date_valid"] = df["pub_date"].apply(lambda x: normalize_date(x) is not None)

    return df


# ============================================================
# 3. MODELO CANÓNICO (dim_book)
# ============================================================
def build_dim_book(df):

    # Normalizaciones
    df["pub_date_norm"] = df["pub_date"].apply(normalize_date)
    df["language_norm"] = df["language"].apply(normalize_language)
    df["price_amount_norm"] = df.apply(lambda x: normalize_price(x["price_amount"]), axis=1)

    # ID canónico
    df["book_id"] = df.apply(canonical_book_id, axis=1)

    # Agrupación por ID
    groups = df.groupby("book_id")

    dim_rows = []

    for bid, g in groups:
        row = {}

        # Reglas de supervivencia
        priority = ["googlebooks", "goodreads"]

        winner = None
        for src in priority:
            if src in g["source"].values:
                winner = g[g["source"] == src].iloc[0]
                break

        # Campos principales desde el ganador
        for col in [
            "title", "subtitle", "publisher", "isbn13", "isbn10",
            "pub_date_norm", "language_norm", "price_amount_norm"
        ]:
            row[col] = winner.get(col)

        # Autores → unión de todas las fuentes
        all_authors = set()
        for a in g["authors"].dropna().values:
            for x in str(a).split(";"):
                x = x.strip()
                if x:
                    all_authors.add(x)
        row["authors"] = sorted(list(all_authors))

        # Categorías → unión
        all_cat = set()
        for c in g["categories"].dropna().values:
            for x in str(c).split(";"):
                x = x.strip()
                if x:
                    all_cat.add(x)
        row["categories"] = sorted(list(all_cat))

        row["book_id"] = bid
        row["fuente_ganadora"] = winner["source"]
        row["ts_ultima_actualizacion"] = datetime.now().isoformat()

        dim_rows.append(row)

    return pd.DataFrame(dim_rows)


# ============================================================
# 4. GUARDAR ARTEFACTOS
# ============================================================
def save_outputs(dim, detail, quality):

    os.makedirs("standard", exist_ok=True)
    os.makedirs("docs", exist_ok=True)

    dim.to_parquet(OUT_DIM, index=False)
    detail.to_parquet(OUT_DETAIL, index=False)

    # Convertir métricas a tipos nativos JSON
    quality_clean = to_python_native(quality)

    with open(OUT_QUALITY, "w", encoding="utf-8") as f:
        json.dump(quality_clean, f, indent=4, ensure_ascii=False)

    # Schema en Markdown
    with open(OUT_SCHEMA, "w", encoding="utf-8") as f:
        f.write("# Schema del modelo canónico (dim_book)\n\n")
        f.write("| Campo | Tipo | Null | Descripción |\n")
        f.write("|-------|------|-------|--------------|\n")
        for col in dim.columns:
            f.write(f"| {col} | string | sí | Campo integrado |\n")

    print("[INFO] Archivos generados en /standard y /docs")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":

    goodreads, google = load_sources()

    detail = build_source_detail(goodreads, google)

    dim = build_dim_book(detail)

    quality = compute_quality_metrics(detail, dim)

    save_outputs(dim, detail, quality)

    print("[OK] Integración completada.")
