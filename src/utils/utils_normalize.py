import ast
import re
import pandas as pd
import numpy as np

# ------------------------------------------------------------
# EDITORIALES
# ------------------------------------------------------------
def clean_publisher(p):
    if not p or str(p).lower() in ("nan", "none"):
        return None

    p = str(p).strip()
    p = re.sub(r"[\"']", "", p)
    p = re.sub(r"\s{2,}", " ", p)
    p = re.sub(r"inc\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"ltd\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"co\.?$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"press$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"media$", "", p, flags=re.IGNORECASE)
    p = re.sub(r"and\s*sons$", "", p, flags=re.IGNORECASE)
    return p.strip().title() or None


# ------------------------------------------------------------
# AUTORES
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# CATEGORÍAS
# ------------------------------------------------------------
def normalize_categories(value):
    if value is None or (isinstance(value, float) and np.isnan(value)):
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

    vals = [x.strip() for x in vals if isinstance(x, str) and x.strip()]
    return sorted(set(vals))


# ------------------------------------------------------------
# FECHA
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# IDIOMA
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# PRECIO
# ------------------------------------------------------------
def normalize_price(value):
    if value is None or str(value).lower() in ("nan", "none", ""):
        return None
    try:
        return float(str(value).replace(",", ".").strip())
    except:
        return None
