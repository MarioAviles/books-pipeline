import os
import json
from datetime import datetime
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# 📦 IMPORTS LOCALES
# ============================================================
from utils.utils_isbn import canonical_book_id
from utils.utils_quality import compute_quality_metrics
from utils.utils_normalize import (
    clean_publisher,
    normalize_authors,
    normalize_categories,
    normalize_date,
    normalize_language,
    normalize_price
)

# ============================================================
# 📁 RUTAS
# ============================================================
LANDING_GOODREADS = "landing/goodreads_books.json"
LANDING_GOOGLE = "landing/googlebooks_books.csv"
STANDARD_DIR = "standard"
DOCS_DIR = "docs"

# ============================================================
# 📥 CARGA DE FUENTES
# ============================================================
def load_sources():
    df_gr = pd.read_json(LANDING_GOODREADS)
    df_gr["source"] = "goodreads"
    df_gb = pd.read_csv(LANDING_GOOGLE, sep=";")
    df_gb["source"] = "googlebooks"

    df_gr["publisher"] = df_gr["publisher"].apply(clean_publisher)
    df_gb["publisher"] = df_gb["publisher"].apply(clean_publisher)
    return df_gr, df_gb

# ============================================================
# 🧩 DIVISIÓN AUTOMÁTICA TÍTULO / SUBTÍTULO
# ============================================================
def split_title_and_subtitle(title, subtitle):
    if isinstance(title, float) or pd.isna(title):
        title = ""
    if isinstance(subtitle, float) or pd.isna(subtitle):
        subtitle = None
    if subtitle and str(subtitle).strip():
        return str(title).strip(), str(subtitle).strip()
    if not title or str(title).strip() == "":
        return None, None
    text = str(title).strip()
    if ":" in text:
        parts = [p.strip() for p in text.split(":", 1)]
    elif "–" in text:
        parts = [p.strip() for p in text.split("–", 1)]
    else:
        parts = [text]
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], None

# ============================================================
# 🧩 MODELO CANÓNICO
# ============================================================
def build_dim_book(df):
    rows = []

    df["group_id"] = df.apply(canonical_book_id, axis=1)
    grouped = df.groupby("group_id")

    for gid, g in grouped:
        gb = g[g["source"] == "googlebooks"].copy()
        g["completitud"] = g.notna().sum(axis=1)
        winner = g.sort_values("completitud", ascending=False).iloc[0]


        row = {}
        title, subtitle = split_title_and_subtitle(winner.get("title"), winner.get("subtitle"))
        row["book_id"] = gid
        row["title"] = title
        row["subtitle"] = subtitle
        row["publisher"] = clean_publisher(winner.get("publisher"))

        isbn13w = winner.get("isbn13")
        row["isbn13"] = isbn13w if isinstance(isbn13w, (str, int)) and str(isbn13w).isdigit() and len(str(isbn13w)) == 13 else None
        row["isbn10"] = winner.get("isbn10")
        row["pub_date_norm"] = normalize_date(winner.get("pub_date"))
        row["language_norm"] = normalize_language(winner.get("language"))
        row["price_amount_norm"] = normalize_price(winner.get("price_amount"))
        row["price_currency"] = winner.get("price_currency")

        row["categories"] = sorted(list({x for c in g["categories"].dropna() for x in normalize_categories(c)})) or None
        row["authors"] = sorted(list({x for a in g["authors"].dropna() for x in normalize_authors(a)})) or None

        if winner["source"] == "googlebooks":
            row["fuente_ganadora"] = winner.get("info_link") or winner.get("canonical_link") or winner.get("api_query_url")
        elif winner["source"] == "goodreads":
            row["fuente_ganadora"] = winner.get("url")
        else:
            row["fuente_ganadora"] = None

        row["ts_ultima_actualizacion"] = datetime.now().isoformat()
        rows.append(row)

    df_final = pd.DataFrame(rows)
    df_final = df_final.drop_duplicates(subset=["isbn13", "title"], keep="first").reset_index(drop=True)

    # 🔍 Deduplicación avanzada
    df_final["title_norm"] = df_final["title"].str.lower().str.replace(r"[^a-z0-9 ]", "", regex=True).str.strip()
    df_final["main_author"] = df_final["authors"].apply(lambda x: x[0].lower() if isinstance(x, list) and x else None)
    df_final["completitud"] = df_final.notna().sum(axis=1)

    deduped = []
    for _, group in df_final.groupby(["title_norm", "main_author"], dropna=False):
        with_isbn = group[group["isbn13"].notna()]
        chosen = with_isbn.iloc[0] if len(with_isbn) > 0 else group.sort_values("completitud", ascending=False).iloc[0]
        deduped.append(chosen)

    df_final = pd.DataFrame(deduped).drop(columns=["title_norm", "main_author", "completitud"], errors="ignore").reset_index(drop=True)
    return df_final

# ============================================================
# 📄 DETALLE DE FUENTES
# ============================================================
def build_book_source_detail(df):
    df_copy = df.copy()
    df_copy["authors"] = df_copy["authors"].apply(lambda x: json.dumps(normalize_authors(x), ensure_ascii=False))
    df_copy["categories"] = df_copy["categories"].apply(lambda x: json.dumps(normalize_categories(x), ensure_ascii=False))
    return df_copy

# ============================================================
# 💾 GUARDAR SALIDAS
# ============================================================
def save_outputs(df_dim, df_detail, metrics):
    os.makedirs(STANDARD_DIR, exist_ok=True)
    os.makedirs(DOCS_DIR, exist_ok=True)

    pq.write_table(pa.Table.from_pandas(df_dim), f"{STANDARD_DIR}/dim_book.parquet")
    pq.write_table(pa.Table.from_pandas(df_detail), f"{STANDARD_DIR}/book_source_detail.parquet")

    with open(f"{DOCS_DIR}/quality_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=4, ensure_ascii=False)

    print("\n✔ Archivos generados correctamente en /standard y /docs\n")

# ============================================================
# 🚀 MAIN
# ============================================================
if __name__ == "__main__":
    df_gr, df_gb = load_sources()
    df_all = pd.concat([df_gr, df_gb], ignore_index=True, sort=False)

    df_all["isbn13"] = df_all["isbn13"].astype(str).str.replace(".0", "", regex=False).replace("nan", np.nan)
    df_all["pub_date"] = df_all["pub_date"].fillna(df_all.get("publication_date"))
    df_all["pub_date"] = df_all["pub_date"].fillna(
        df_all.get("pub_info").astype(str).str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    )

    df_dim = build_dim_book(df_all)
    df_detail = build_book_source_detail(df_all)
    metrics = compute_quality_metrics(df_detail, df_dim)
    save_outputs(df_dim, df_detail, metrics)
