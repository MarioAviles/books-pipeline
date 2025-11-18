import os
import json
import hashlib
from datetime import datetime
import ast
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# 📁 RUTAS
# ============================================================

LANDING_GOODREADS = "landing/goodreads_books.json"
LANDING_GOOGLE = "landing/googlebooks_books.csv"

STANDARD_DIR = "standard"
DOCS_DIR = "docs"

# ============================================================
# 🔧 NORMALIZADORES Y MAPEOS
# ============================================================

PUBLISHER_MAP = {
    "oreilly & associates incorporated": "O'Reilly Media",
    "o'reilly media, inc.": "O'Reilly Media",
    "oreilly media": "O'Reilly Media",
    "john wiley & sons": "Wiley",
    "wiley": "Wiley",
}

def clean_publisher(p):
    if not p or str(p).lower() in ("nan", "none"):
        return None
    p = str(p).strip().replace('"', "").replace("'", "")
    key = p.lower()
    return PUBLISHER_MAP.get(key, p.strip())

def normalize_authors(value):
    if value is None:
        return []
    if isinstance(value, list):
        val = value
    elif isinstance(value, str):
        if value.startswith("[") and value.endswith("]"):
            try:
                val = ast.literal_eval(value)
            except:
                val = [value]
        elif ";" in value:
            val = value.split(";")
        else:
            val = [value]
    else:
        val = [str(value)]
    names = [str(x).strip().replace(".", "") for x in val if x and str(x).strip()]
    unique = sorted(set([n.title() for n in names]))
    return unique

def normalize_categories(value):
    if value is None:
        return []
    if isinstance(value, list):
        vals = value
    elif isinstance(value, str):
        if value.startswith("[") and value.endswith("]"):
            try:
                vals = ast.literal_eval(value)
            except:
                vals = [value]
        elif ";" in value:
            vals = value.split(";")
        else:
            vals = [value]
    else:
        vals = [str(value)]
    return sorted(set([x.strip() for x in vals if x.strip()]))

def normalize_date(date_str):
    if not date_str or str(date_str).lower() in ("nan", "none", ""):
        return None
    try:
        parsed = pd.to_datetime(str(date_str), errors="coerce")
        if pd.isna(parsed):
            return None
        if parsed.day == 1 and parsed.month == 1:
            return str(parsed.year)
        return str(parsed.date())
    except:
        return None

def normalize_language(lang):
    if not lang:
        return None
    lang = str(lang).lower().strip()
    mapping = {
        "english": "en",
        "eng": "en",
        "en-us": "en",
        "spanish": "es",
        "español": "es",
        "pt-br": "pt-BR"
    }
    return mapping.get(lang, lang)

def normalize_price(value):
    if value is None or str(value).lower() in ("nan", "none", ""):
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except:
        return None

def canonical_id_from_fields(title, authors, publisher):
    key = (str(title).lower().strip()
           + str(authors).lower().strip()
           + str(publisher).lower().strip())
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]

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
# 🔎 NORMALIZACIÓN Y AGRUPACIÓN
# ============================================================

def normalize_title(t):
    if not t:
        return ""
    return str(t).lower().replace(":", "").replace("-", "").replace('"', "").strip()

def same_book(title1, authors1, title2, authors2):
    if normalize_title(title1) != normalize_title(title2):
        return False
    set1 = set([a.lower() for a in normalize_authors(authors1)])
    set2 = set([a.lower() for a in normalize_authors(authors2)])
    return len(set1.intersection(set2)) > 0

# ============================================================
# 🧩 DIVISIÓN AUTOMÁTICA TÍTULO / SUBTÍTULO
# ============================================================

def split_title_and_subtitle(title, subtitle):
    """
    Si no hay subtitle pero el title contiene ':' o '–',
    divide automáticamente en título y subtítulo.
    """
    if isinstance(title, float) or pd.isna(title):
        title = ""
    if isinstance(subtitle, float) or pd.isna(subtitle):
        subtitle = None

    if subtitle and str(subtitle).strip():
        return str(title).strip(), str(subtitle).strip()

    if not title or str(title).strip() == "":
        return None, None

    # Detectar separadores comunes
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

    """
    Si no hay subtitle pero el title contiene ':' o '–',
    divide automáticamente en título y subtítulo.
    """
    if subtitle:
        return title.strip(), subtitle.strip()
    if not title:
        return None, None
    # Detectar separador común
    if ":" in title:
        parts = [p.strip() for p in title.split(":", 1)]
    elif "–" in title:
        parts = [p.strip() for p in title.split("–", 1)]
    else:
        parts = [title.strip()]
    if len(parts) == 2:
        return parts[0], parts[1]
    return parts[0], None

# ============================================================
# 🧩 MODELO CANÓNICO
# ============================================================

def build_dim_book(df):
    rows = []

    def assign_group(row):
        isbn13 = str(row.get("isbn13")) if pd.notna(row.get("isbn13")) else None
        isbn10 = str(row.get("isbn10")) if pd.notna(row.get("isbn10")) else None
        if isbn13 and isbn13.isdigit() and len(isbn13) == 13:
            return isbn13
        if isbn10 and isbn10.isdigit() and len(isbn10) == 10:
            return isbn10
        return canonical_id_from_fields(row.get("title"), row.get("authors"), row.get("publisher"))

    df["group_id"] = df.apply(assign_group, axis=1)
    grouped = df.groupby("group_id")

    for gid, g in grouped:
        gb = g[g["source"] == "googlebooks"].copy()
        gr = g[g["source"] == "goodreads"].copy()

        if len(gb) > 0:
            gb.loc[:, "pub_tmp"] = gb["pub_date"].apply(normalize_date)
            gb = gb.sort_values("pub_tmp", ascending=False)
            winner = gb.iloc[0]
        else:
            winner = g.iloc[0]

        row = {}
        title, subtitle = split_title_and_subtitle(winner.get("title"), winner.get("subtitle"))
        row["book_id"] = gid
        row["title"] = title
        row["subtitle"] = subtitle
        row["publisher"] = clean_publisher(winner.get("publisher"))

        isbn13w = winner.get("isbn13")
        row["isbn13"] = isbn13w if isinstance(isbn13w, (str, int)) and str(isbn13w).isdigit() and len(str(isbn13w)) == 13 else None
        row["isbn10"] = winner.get("isbn10")

        pubdate = winner.get("pub_date")
        if not pubdate:
            pubdate = next((x for x in g["pub_date"].dropna()), None)
        row["pub_date_norm"] = normalize_date(pubdate)

        lang = winner.get("language")
        if not lang or pd.isna(lang):
            lang = next((x for x in g["language"].dropna()), None)
        row["language_norm"] = normalize_language(lang)

        price = winner.get("price_amount")
        if not price:
            price = next((x for x in g["price_amount"].dropna()), None)
        row["price_amount_norm"] = normalize_price(price)
        row["price_currency"] = winner.get("price_currency")

        categories = set()
        for c in g["categories"].dropna():
            for x in normalize_categories(c):
                categories.add(x)
        row["categories"] = sorted(list(categories)) if categories else None

        authors = set()
        for a in g["authors"].dropna():
            for x in normalize_authors(a):
                authors.add(x)
        row["authors"] = sorted(list(authors)) if authors else None

        row["fuente_ganadora"] = winner["source"]
        row["ts_ultima_actualizacion"] = datetime.now().isoformat()
        rows.append(row)

    df_result = pd.DataFrame(rows)

    # ✅ 1️⃣ Eliminar duplicados exactos por ISBN13
    df_result = df_result.drop_duplicates(subset=["isbn13"], keep="first")

    # ✅ 2️⃣ Eliminar duplicados por título + subtítulo, conservando el que tenga más datos no nulos
    def count_non_nulls(row):
        return row.notna().sum()

    df_result["non_nulls"] = df_result.apply(count_non_nulls, axis=1)

    df_result = (
        df_result.sort_values("non_nulls", ascending=False)
        .drop_duplicates(subset=["title", "subtitle"], keep="first")
        .drop(columns=["non_nulls"])
        .reset_index(drop=True)
    )

    return df_result

    rows = []

    def assign_group(row):
        isbn13 = str(row.get("isbn13")) if pd.notna(row.get("isbn13")) else None
        isbn10 = str(row.get("isbn10")) if pd.notna(row.get("isbn10")) else None
        if isbn13 and isbn13.isdigit() and len(isbn13) == 13:
            return isbn13
        if isbn10 and isbn10.isdigit() and len(isbn10) == 10:
            return isbn10
        return canonical_id_from_fields(row.get("title"), row.get("authors"), row.get("publisher"))

    df["group_id"] = df.apply(assign_group, axis=1)
    grouped = df.groupby("group_id")

    for gid, g in grouped:
        gb = g[g["source"] == "googlebooks"].copy()
        gr = g[g["source"] == "goodreads"].copy()

        if len(gb) > 0:
            gb.loc[:, "pub_tmp"] = gb["pub_date"].apply(normalize_date)
            gb = gb.sort_values("pub_tmp", ascending=False)
            winner = gb.iloc[0]
        else:
            winner = g.iloc[0]

        row = {}
        title, subtitle = split_title_and_subtitle(winner.get("title"), winner.get("subtitle"))
        row["book_id"] = gid
        row["title"] = title
        row["subtitle"] = subtitle
        row["publisher"] = clean_publisher(winner.get("publisher"))

        isbn13w = winner.get("isbn13")
        row["isbn13"] = isbn13w if isinstance(isbn13w, (str, int)) and str(isbn13w).isdigit() and len(str(isbn13w)) == 13 else None
        row["isbn10"] = winner.get("isbn10")

        pubdate = winner.get("pub_date")
        if not pubdate:
            pubdate = next((x for x in g["pub_date"].dropna()), None)
        row["pub_date_norm"] = normalize_date(pubdate)

        lang = winner.get("language")
        if not lang or pd.isna(lang):
            lang = next((x for x in g["language"].dropna()), None)
        row["language_norm"] = normalize_language(lang)

        price = winner.get("price_amount")
        if not price:
            price = next((x for x in g["price_amount"].dropna()), None)
        row["price_amount_norm"] = normalize_price(price)
        row["price_currency"] = winner.get("price_currency")

        categories = set()
        for c in g["categories"].dropna():
            for x in normalize_categories(c):
                categories.add(x)
        row["categories"] = sorted(list(categories)) if categories else None

        authors = set()
        for a in g["authors"].dropna():
            for x in normalize_authors(a):
                authors.add(x)
        row["authors"] = sorted(list(authors)) if authors else None

        row["fuente_ganadora"] = winner["source"]
        row["ts_ultima_actualizacion"] = datetime.now().isoformat()
        rows.append(row)

    df_result = pd.DataFrame(rows)

    # ✅ Eliminamos duplicados exactos por ISBN13
    df_result = df_result.drop_duplicates(subset=["isbn13"], keep="first")

    return df_result

    rows = []

    def assign_group(row):
        isbn13 = str(row.get("isbn13")) if pd.notna(row.get("isbn13")) else None
        isbn10 = str(row.get("isbn10")) if pd.notna(row.get("isbn10")) else None
        if isbn13 and isbn13.isdigit() and len(isbn13) == 13:
            return isbn13
        if isbn10 and isbn10.isdigit() and len(isbn10) == 10:
            return isbn10
        return canonical_id_from_fields(row.get("title"), row.get("authors"), row.get("publisher"))

    df["group_id"] = df.apply(assign_group, axis=1)
    grouped = df.groupby("group_id")

    for gid, g in grouped:
        gb = g[g["source"] == "googlebooks"].copy()
        gr = g[g["source"] == "goodreads"].copy()

        if len(gb) > 0:
            gb.loc[:, "pub_tmp"] = gb["pub_date"].apply(normalize_date)
            gb = gb.sort_values("pub_tmp", ascending=False)
            winner = gb.iloc[0]
        else:
            winner = g.iloc[0]

        row = {}
        title, subtitle = split_title_and_subtitle(winner.get("title"), winner.get("subtitle"))
        row["book_id"] = gid
        row["title"] = title
        row["subtitle"] = subtitle
        row["publisher"] = clean_publisher(winner.get("publisher"))

        isbn13w = winner.get("isbn13")
        row["isbn13"] = isbn13w if isinstance(isbn13w, (str, int)) and str(isbn13w).isdigit() and len(str(isbn13w)) == 13 else None
        row["isbn10"] = winner.get("isbn10")

        pubdate = winner.get("pub_date")
        if not pubdate:
            pubdate = next((x for x in g["pub_date"].dropna()), None)
        row["pub_date_norm"] = normalize_date(pubdate)

        lang = winner.get("language")
        if not lang or pd.isna(lang):
            lang = next((x for x in g["language"].dropna()), None)
        row["language_norm"] = normalize_language(lang)

        price = winner.get("price_amount")
        if not price:
            price = next((x for x in g["price_amount"].dropna()), None)
        row["price_amount_norm"] = normalize_price(price)
        row["price_currency"] = winner.get("price_currency")

        categories = set()
        for c in g["categories"].dropna():
            for x in normalize_categories(c):
                categories.add(x)
        row["categories"] = sorted(list(categories)) if categories else None

        authors = set()
        for a in g["authors"].dropna():
            for x in normalize_authors(a):
                authors.add(x)
        row["authors"] = sorted(list(authors)) if authors else None

        row["fuente_ganadora"] = winner["source"]
        row["ts_ultima_actualizacion"] = datetime.now().isoformat()
        rows.append(row)

    return pd.DataFrame(rows)

# ============================================================
# 📄 DETALLE DE FUENTES
# ============================================================

def build_book_source_detail(df):
    df_copy = df.copy()
    df_copy["authors"] = df_copy["authors"].apply(lambda x: json.dumps(normalize_authors(x), ensure_ascii=False))
    df_copy["categories"] = df_copy["categories"].apply(lambda x: json.dumps(normalize_categories(x), ensure_ascii=False))
    return df_copy

# ============================================================
# 📊 MÉTRICAS DE CALIDAD
# ============================================================

def build_quality_metrics(df_dim, df_detail):
    return {
        "total_libros_dim": len(df_dim),
        "total_registros_fuente": len(df_detail),
        "%isbn13_nulos": float(df_dim["isbn13"].isna().mean()),
        "%titulos_nulos": float(df_dim["title"].isna().mean()),
        "%fechas_validas": float(df_dim["pub_date_norm"].notna().mean()),
        "%idiomas_validos": float(df_dim["language_norm"].notna().mean()),
        "duplicados_por_isbn13": int(df_dim["isbn13"].duplicated().sum()),
        "columnas_dim": list(df_dim.columns),
        "columnas_fuente": list(df_detail.columns),
        "timestamp": datetime.now().isoformat()
    }

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

    with open(f"{DOCS_DIR}/schema.md", "w", encoding="utf-8") as f:
        f.write("# Esquema de dim_book\n\nCampo | Tipo | Descripción\n---|---|---\n")
        schema = {
            "book_id": "ID canónico (isbn13, isbn10 o hash)",
            "title": "Título principal (sin subtítulo)",
            "subtitle": "Subtítulo separado automáticamente si aplica",
            "publisher": "Editorial limpia y estandarizada",
            "isbn13": "Identificador ISBN13",
            "isbn10": "Identificador ISBN10",
            "pub_date_norm": "Fecha normalizada ISO-8601 o año",
            "language_norm": "Idioma BCP-47",
            "price_amount_norm": "Precio decimal",
            "price_currency": "Moneda ISO-4217",
            "categories": "Lista de categorías únicas",
            "authors": "Autores únicos y limpios",
            "fuente_ganadora": "Fuente prioritaria (googlebooks/goodreads)",
            "ts_ultima_actualizacion": "Marca temporal"
        }
        for col, desc in schema.items():
            f.write(f"{col} | {df_dim[col].dtype} | {desc}\n")

    print("\n✔ Archivos generados correctamente en /standard y /docs\n")

# ============================================================
# 🚀 MAIN
# ============================================================

if __name__ == "__main__":
    df_gr, df_gb = load_sources()
    df_all = pd.concat([df_gr, df_gb], ignore_index=True, sort=False)

    df_all["isbn13"] = (
        df_all["isbn13"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .replace("nan", np.nan)
    )

    df_all["pub_date"] = df_all["pub_date"].fillna(df_all.get("publication_date"))
    df_all["pub_date"] = df_all["pub_date"].fillna(
        df_all.get("pub_info").astype(str).str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    )

    df_dim = build_dim_book(df_all)
    df_detail = build_book_source_detail(df_all)
    metrics = build_quality_metrics(df_dim, df_detail)
    save_outputs(df_dim, df_detail, metrics)
