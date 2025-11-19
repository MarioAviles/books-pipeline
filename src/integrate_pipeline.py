import os
import json
import hashlib
from datetime import datetime
import ast
import re
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


def clean_publisher(p):
    if not p or str(p).lower() in ("nan", "none"):
        return None

    p = str(p).strip()
    p = re.sub(r"[\"']", "", p)          # quitar comillas
    p = re.sub(r"\s{2,}", " ", p)        # espacios duplicados
    p = re.sub(r"inc\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"ltd\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"co\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"press$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"media$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"and\s*sons$", "", p, flags=re.IGNORECASE)

    p = p.strip().title()                # capitalización uniforme
    return p if p else None
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

        # ============================================================
    # 🔍 DEDUPLICACIÓN AVANZADA POR OBRA REAL
    # ============================================================
    df_final["title_norm"] = df_final["title"].str.lower().str.replace(r"[^a-z0-9 ]", "", regex=True).str.strip()
    df_final["main_author"] = df_final["authors"].apply(lambda x: x[0].lower() if isinstance(x, list) and x else None)
    df_final["completitud"] = df_final.notna().sum(axis=1)

    deduped = []
    for _, group in df_final.groupby(["title_norm", "main_author"], dropna=False):
        # priorizar ISBN13 válidos
        with_isbn = group[group["isbn13"].notna()]
        if len(with_isbn) > 0:
            chosen = with_isbn.iloc[0]
        else:
            # sin ISBN → el más completo
            chosen = group.sort_values("completitud", ascending=False).iloc[0]
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
        f.write("# Esquema de dim_book\n\n")
        f.write("Campo | Tipo | Nullable | Formato | Ejemplo | Reglas\n")
        f.write("---|---|---|---|---|---\n")

        schema = {
            "book_id": {"formato": "string (ISBN-13 o hash)", "ejemplo": "9781119741763", "reglas": "Único, no nulo"},
            "title": {"formato": "string", "ejemplo": "Becoming a Data Head", "reglas": "Trim y capitalización correcta"},
            "subtitle": {"formato": "string o nulo", "ejemplo": "How to Think, Speak, and Understand Data Science", "reglas": "Opcional"},
            "publisher": {"formato": "string", "ejemplo": "Wiley", "reglas": "Normalizado y limpio"},
            "isbn13": {"formato": "string (13 dígitos)", "ejemplo": "9781119741763", "reglas": "Validado por checksum"},
            "isbn10": {"formato": "string (10 dígitos)", "ejemplo": "1119741769", "reglas": "Derivado o validado si existe"},
            "pub_date_norm": {"formato": "YYYY-MM-DD (ISO-8601)", "ejemplo": "2021-04-13", "reglas": "Debe ser fecha válida"},
            "language_norm": {"formato": "BCP-47", "ejemplo": "en", "reglas": "Minúsculas; formato válido"},
            "price_amount_norm": {"formato": "decimal(10,2)", "ejemplo": "27.99", "reglas": "≥ 0 o nulo"},
            "price_currency": {"formato": "ISO-4217", "ejemplo": "EUR", "reglas": "Tres letras mayúsculas"},
            "categories": {"formato": "lista[string]", "ejemplo": "['Business & Economics']", "reglas": "Sin duplicados"},
            "authors": {"formato": "lista[string]", "ejemplo": "['Alex J Gutman', 'Jordan Goldmeier']", "reglas": "Sin duplicados ni nulos"},
            "fuente_ganadora": {"formato": "string (URL)", "ejemplo": "https://play.google.com/store/books/details?id=GCUqEAAAQBAJ", "reglas": "Debe ser URL válida"},
            "ts_ultima_actualizacion": {"formato": "timestamp ISO-8601", "ejemplo": "2025-11-19T10:56:30.416815", "reglas": "Autogenerado"}
        }

        for col, meta in schema.items():
            dtype = str(df_dim[col].dtype) if col in df_dim.columns else "desconocido"
            nullable = "Sí" if df_dim[col].isnull().any() else "No"
            f.write(f"{col} | {dtype} | {nullable} | {meta['formato']} | {meta['ejemplo']} | {meta['reglas']}\n")

    print("\n✔ Archivos generados correctamente en /standard y /docs\n")


# ============================================================
# 🚀 MAIN
# ============================================================

if __name__ == "__main__":
    df_gr, df_gb = load_sources()
    df_all = pd.concat([df_gr, df_gb], ignore_index=True, sort=False)

    df_all["isbn13"] = (
        df_all["isbn13"].astype(str).str.replace(".0", "", regex=False).replace("nan", np.nan)
    )

    df_all["pub_date"] = df_all["pub_date"].fillna(df_all.get("publication_date"))
    df_all["pub_date"] = df_all["pub_date"].fillna(
        df_all.get("pub_info").astype(str).str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
    )

    df_dim = build_dim_book(df_all)
    df_detail = build_book_source_detail(df_all)
    metrics = build_quality_metrics(df_dim, df_detail)
    save_outputs(df_dim, df_detail, metrics)
