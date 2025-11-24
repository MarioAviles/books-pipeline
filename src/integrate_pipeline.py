import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any

import pandas as pd
import numpy as np

# -------------------------
# IMPORTS EXTERNALIZADOS
# -------------------------

from utils.utils_isbn import (
    normalize_str,
    normalize_title,
    normalize_author,
    get_first_author,
    iso_date,
    canonical_id_from_data
)

from utils.utils_quality import (
    save_dataframe_robust,
    write_quality_metrics,
    write_schema_markdown
)


# -------------------------
# CONFIG
# -------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LANDING_DIR = PROJECT_ROOT / "landing"
STANDARD_DIR = PROJECT_ROOT / "standard"
DOCS_DIR = PROJECT_ROOT / "docs"

LANDING_DIR.mkdir(parents=True, exist_ok=True)
STANDARD_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

GOODREADS_FILE = LANDING_DIR / "goodreads_books.json"
GOOGLE_PARQUET = LANDING_DIR / "googlebooks_books.parquet"
GOOGLE_CSV = LANDING_DIR / "googlebooks_books.csv"

DIM_BOOK = STANDARD_DIR / "dim_book.parquet"
DETAIL = STANDARD_DIR / "book_source_detail.parquet"
METRICS = DOCS_DIR / "quality_metrics.json"


# -------------------------
# UTIL
# -------------------------

def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def safe_read_goodreads(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[WARN] No existe {path}")
        return pd.DataFrame()

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            return pd.DataFrame([raw])
    except:
        pass

    # fallback NDJSON
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rows.append(json.loads(line.strip()))
            except:
                pass
    return pd.DataFrame(rows)


def safe_read_google(parquet: Path, csv: Path) -> pd.DataFrame:
    df = pd.DataFrame()

    if parquet.exists():
        try:
            df = pd.read_parquet(parquet)
        except:
            pass

    if df.empty and csv.exists():
        try:
            df = pd.read_csv(csv, sep=";")
        except:
            pass

    if not df.empty:
        df = df.replace({np.nan: None})

    return df


# -------------------------
# LÓGICA DE MERGE
# -------------------------

def choose(val_g, val_gg, prefer="goodreads"):
    if val_g is None:
        return val_gg
    if val_gg is None:
        return val_g
    return val_g if prefer == "goodreads" else val_gg


def merge_records(gr: Dict, gg: Dict) -> Dict:

    # título
    t_g = normalize_str(gr.get("title"))
    t_gg = normalize_str(gg.get("title") if gg else None)
    title = choose(t_g, t_gg)

    # autores
    a_g = normalize_author(gr.get("authors"))
    a_gg = normalize_author(gg.get("authors")) if gg else []

    merged_a = list(dict.fromkeys(a_g + a_gg))
    first_author = merged_a[0] if merged_a else None
    authors_str = " | ".join(merged_a) if merged_a else None

    # categorías
    def normalize_categories(v):
        if not v:
            return []
        if isinstance(v, list):
            return [str(x).strip() for x in v if x]
        s = str(v)
        if "|" in s:
            return [x.strip() for x in s.split("|") if x.strip()]
        return [s.strip()]

    c_g = normalize_categories(gr.get("genres") or gr.get("categories"))
    c_gg = normalize_categories(gg.get("categories") if gg else None)
    categories_str = " | ".join(list(dict.fromkeys(c_g + c_gg))) if (c_g or c_gg) else None

    # fechas
    pub_g = iso_date(gr.get("publication_date") or gr.get("pub_date"))
    pub_gg = iso_date(gg.get("pub_date") if gg else None)
    pub_date = choose(pub_g, pub_gg)

    pub_year = None
    if pub_date and re.match(r"^\d{4}", pub_date):
        pub_year = int(pub_date[:4])

    # precio
    def safe_decimal(v):
        try:
            return float(str(v).replace(",", "."))
        except:
            return None

    price_amt = safe_decimal(gg.get("price_amount") if gg else None)

    def normalize_currency(v):
        if not v:
            return None
        s = str(v).strip().upper()
        m = {"€": "EUR", "$": "USD", "£": "GBP"}
        return m.get(s, s[:3])

    price_cur = normalize_currency(gg.get("price_currency") if gg else None)

    # ISBN
    isbn13 = normalize_str(gr.get("isbn13")) or normalize_str(gg.get("isbn13"))
    isbn10 = normalize_str(gr.get("isbn") or gr.get("isbn10")) or normalize_str(gg.get("isbn10"))

    # publisher
    publisher = choose(normalize_str(gr.get("publisher")),
                       normalize_str(gg.get("publisher")) if gg else None)

    # descripción
    description = choose(
        normalize_str(gr.get("description") or gr.get("desc")),
        normalize_str(gg.get("description")) if gg else None
    )

    # canonical_id
    if isbn13:
        cid = isbn13
    elif isbn10:
        cid = isbn10
    else:
        cid = canonical_id_from_data(title, first_author, publisher, str(pub_year or ""))

    # páginas
    num_pages = choose(gr.get("num_pages"), gg.get("pageCount") if gg else None)

    # preferencia
    score_g = sum(1 for v in [t_g, a_g, pub_g, gr.get("num_pages")] if v)
    score_gg = sum(1 for v in [t_gg, a_gg, pub_gg, price_amt, gg.get("isbn13") if gg else None] if v)
    prefer = "goodreads" if score_g >= score_gg else "google"

    url_pref = gr.get("url")
    if gg and prefer == "google":
        url_pref = gg.get("gb_url") or gr.get("url")

    return {
        "canonical_id": cid,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "title": title,
        "authors": authors_str,
        "first_author": first_author,
        "publisher": publisher,
        "pub_date": pub_date,
        "pub_year": pub_year,
        "language": gr.get("language") or (gg.get("language") if gg else None),
        "categories": categories_str,
        "num_pages": num_pages,
        "format": choose(gr.get("format"), gg.get("format") if gg else None),
        "description": description,
        "rating_value": gr.get("rating_value"),
        "rating_count": gr.get("rating_count"),
        "price_amount": price_amt,
        "price_currency": price_cur,
        "source_preference": prefer,
        "most_complete_url": url_pref,
        "ingestion_date_goodreads": gr.get("ingestion_date"),
        "ingestion_date_google": gg.get("ingestion_date_google") if gg else None
    }


# -------------------------
# PIPELINE PRINCIPAL
# -------------------------

def run_pipeline():
    ts = now_ts()
    print(f"[{ts}] INICIANDO MERGE...")

    df_good = safe_read_goodreads(GOODREADS_FILE)
    df_gg = safe_read_google(GOOGLE_PARQUET, GOOGLE_CSV)

    print(f"[INFO] Goodreads: {len(df_good)} | Google: {len(df_gg)}")

    google_by_isbn = {}
    google_by_key = {}

    if not df_gg.empty:
        for r in df_gg.to_dict(orient="records"):
            if r.get("isbn13"):
                google_by_isbn[str(r["isbn13"])] = r

            tnorm = normalize_title(r.get("title"))
            afirst = get_first_author(r.get("authors"))
            if tnorm and afirst:
                google_by_key.setdefault(f"{tnorm}||{afirst}", r)

    merged_rows = []
    detail_rows = []

    for g in df_good.to_dict(orient="records"):
        matched = None
        method = "none"

        isbn = normalize_str(g.get("isbn13") or g.get("isbn"))
        if isbn and isbn in google_by_isbn:
            matched = google_by_isbn[isbn]
            method = "isbn13"
        else:
            tnorm = normalize_title(g.get("title"))
            afirst = get_first_author(g.get("authors"))
            if tnorm and afirst:
                k = f"{tnorm}||{afirst}"
                if k in google_by_key:
                    matched = google_by_key[k]
                    method = "heuristic"

        merged = merge_records(g, matched or {})
        merged_rows.append(merged)

        detail_rows.append({
            "canonical_id": merged["canonical_id"],
            "from_google": bool(matched),
            "merge_method": method,
            "timestamp": ts,
            "raw_goodreads": g,                   # ← datos crudos Goodreads
            "raw_google": matched if matched else None   # ← datos crudos Google Books
        })


    df_final = pd.DataFrame(merged_rows)
    df_detail = pd.DataFrame(detail_rows)

    if not df_final.empty:
        df_final["_score"] = df_final.notnull().sum(axis=1)
        df_final = df_final.sort_values("_score", ascending=False)
        df_final = df_final.drop_duplicates(subset=["canonical_id"], keep="first")
        df_final.drop(columns=["_score"], inplace=True)

    save_dataframe_robust(df_final, DIM_BOOK)
    save_dataframe_robust(df_detail, DETAIL)

    metrics = {
        "generated_at": ts,
        "rows_input_goodreads": len(df_good),
        "rows_output": len(df_final),
        "matched_with_google": int(df_detail["from_google"].sum()),
        "percent_with_isbn13": round(100 * df_final["isbn13"].notnull().mean(), 2),
        "percent_with_pub_date": round(100 * df_final["pub_date"].notnull().mean(), 2),
        "source_preference_counts": df_final["source_preference"].value_counts().to_dict(),
    }

    write_quality_metrics(METRICS, metrics)
    schema_path = DOCS_DIR / "schema.md"
    write_schema_markdown(schema_path, df_final)
    print(f"[FIN] Filas finales: {len(df_final)}")


if __name__ == "__main__":
    run_pipeline()
