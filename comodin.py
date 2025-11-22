# ============================================================
# 📚 INTEGRATE PIPELINE - CARGA DE FUENTES + INTEGRACIÓN FINAL
# ============================================================

import os
import json
import re
import unicodedata
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from utils.utils_normalize import clean_publisher


# ============================================================
# 🔧 FUNCIONES AUXILIARES
# ============================================================

def normalize_text_full(txt):
    """Normaliza eliminando acentos, mayúsculas, espacios y signos de puntuación."""
    if pd.isna(txt):
        return ""
    txt = str(txt).lower().strip()
    txt = "".join(
        c for c in unicodedata.normalize("NFD", txt)
        if unicodedata.category(c) != "Mn"
    )
    txt = re.sub(r"[^\w\s]", "", txt)  # elimina signos de puntuación
    txt = txt.replace(" ", "")
    return txt


# ============================================================
# 📥 CARGA DE FUENTES
# ============================================================

def load_sources():
    """Carga y alinea las fuentes Goodreads y Google Books con columnas comunes."""

    LANDING_GOODREADS = "landing/goodreads_books.json"
    LANDING_GOOGLE = "landing/googlebooks_books.csv"

    # --- Goodreads ---
    df_gr = pd.read_json(LANDING_GOODREADS)
    df_gr["source"] = "goodreads"
    df_gr = df_gr.rename(columns={
        "id": "gr_id",
        "isbn": "isbn10",
        "isbn13": "isbn13",
        "rating_value": "gr_rating_value",
        "rating_count": "gr_rating_count"
    })

    # --- Google Books ---
    df_gb = pd.read_csv(LANDING_GOOGLE, sep=";")
    df_gb["source"] = "googlebooks"

    # --- Definir todas las columnas esperadas ---
    all_cols = [
        "gr_id", "gb_id", "title", "subtitle", "authors", "publisher",
        "pub_date", "language", "categories", "isbn13", "isbn10",
        "price_amount", "price_currency", "info_link", "url",
        "gr_rating_value", "gr_rating_count", "source"
    ]

    # --- Asegurar que ambas fuentes tienen todas las columnas ---
    for col in all_cols:
        if col not in df_gr.columns:
            df_gr[col] = None
        if col not in df_gb.columns:
            df_gb[col] = None

    df_gr = df_gr[all_cols]
    df_gb = df_gb[all_cols]

    # --- Limpieza básica ---
    df_gr["publisher"] = df_gr["publisher"].apply(clean_publisher)
    df_gb["publisher"] = df_gb["publisher"].apply(clean_publisher)

    print(f"📘 Goodreads: {len(df_gr)} registros ({df_gr.shape[1]} columnas)")
    print(f"📗 Google Books: {len(df_gb)} registros ({df_gb.shape[1]} columnas)")
    print(f"🧩 Columnas idénticas: {list(df_gr.columns) == list(df_gb.columns)}")

    return df_gr, df_gb


# ============================================================
# 🔗 INTEGRACIÓN DE FUENTES
# ============================================================

def integrate_sources(df_gr, df_gb):
    """
    Integra Goodreads y Google Books:
    - Goodreads como fuente base.
    - Coincidencia por ISBN13, o si falta, por título + subtítulo + autores normalizados.
    - Rellena nulos de Goodreads con valores de Google Books.
    - Usa la URL de la fuente con menos nulos.
    """

    rows = []
    cols_final = [
        "id", "authors", "title", "subtitle", "publisher", "pub_date",
        "language", "categories", "price_amount", "price_currency",
        "rating_value", "rating_count", "isbn", "isbn13", "url"
    ]

    for _, gr in df_gr.iterrows():
        gr_dict = {col: gr.get(col, None) for col in cols_final}
        gr_dict["id"] = gr.get("isbn13")

        # Coincidencia por ISBN13
        match = df_gb[df_gb["isbn13"] == gr["isbn13"]] if pd.notna(gr["isbn13"]) else pd.DataFrame()

        # Si no hay ISBN, comparar por título + subtítulo + autor
        if match.empty:
            gr_key = normalize_text_full(
                f"{gr.get('title','')}{gr.get('subtitle','')}{gr.get('authors','')}"
            )
            df_gb["match_key"] = df_gb.apply(
                lambda x: normalize_text_full(
                    f"{x.get('title','')}{x.get('subtitle','')}{x.get('authors','')}"
                ),
                axis=1
            )
            match = df_gb[df_gb["match_key"] == gr_key]

        if not match.empty:
            gb = match.iloc[0]
            gr_nulls = gr.isna().sum()
            gb_nulls = gb.isna().sum()

            # Completar vacíos
            for col in cols_final:
                val_gr = gr_dict.get(col)
                val_gb = gb.get(col, None)
                is_empty = (
                    val_gr is None
                    or (isinstance(val_gr, float) and pd.isna(val_gr))
                    or (isinstance(val_gr, (list, tuple, set)) and len(val_gr) == 0)
                    or (isinstance(val_gr, str) and val_gr.strip() == "")
                )
                if is_empty and pd.notna(val_gb) and val_gb not in ["", "nan", None]:
                    gr_dict[col] = val_gb

            # Si Goodreads no tenía ISBN pero Google sí
            if not gr_dict["id"] and pd.notna(gb.get("isbn13")):
                gr_dict["id"] = gb["isbn13"]

            # URL de la fuente más completa
            if gb_nulls < gr_nulls:
                gr_dict["url"] = gb.get("info_link", gr.get("url"))

        rows.append(gr_dict)

    df_final = pd.DataFrame(rows).convert_dtypes()
    print(f"✅ Integración completada: {len(df_final)} libros unificados")
    return df_final


# ============================================================
# 💾 GUARDADO DE RESULTADOS
# ============================================================

def save_outputs(df_final, df_gr, df_gb):
    STANDARD_DIR = "standard"
    os.makedirs(STANDARD_DIR, exist_ok=True)

    dim_path_csv = os.path.join(STANDARD_DIR, "dim_book.csv")
    dim_path_parquet = os.path.join(STANDARD_DIR, "dim_book.parquet")
    detail_path_csv = os.path.join(STANDARD_DIR, "book_source_detail.csv")
    detail_path_parquet = os.path.join(STANDARD_DIR, "book_source_detail.parquet")

    df_final.to_csv(dim_path_csv, index=False)
    pq.write_table(pa.Table.from_pandas(df_final), dim_path_parquet)
    print(f"✅ Guardado dim_book → {dim_path_csv} y {dim_path_parquet}")

    df_detail = pd.concat([df_gr, df_gb], ignore_index=True).convert_dtypes()
    df_detail["authors"] = df_detail["authors"].astype(str)
    pq.write_table(pa.Table.from_pandas(df_detail), detail_path_parquet)
    df_detail.to_csv(detail_path_csv, index=False)
    print(f"✅ Guardado book_source_detail → {detail_path_csv} y {detail_path_parquet}")


# ============================================================
# 🚀 MAIN
# ============================================================

if __name__ == "__main__":
    df_gr, df_gb = load_sources()
    df_final = integrate_sources(df_gr, df_gb)
    save_outputs(df_final, df_gr, df_gb)
