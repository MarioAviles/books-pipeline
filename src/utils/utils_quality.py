import pandas as pd
from datetime import datetime

def validate_isbn13(x):
    if not isinstance(x, str):
        return False
    x = x.replace("-", "")
    return len(x) == 13 and x.isdigit()

def validate_isbn10(x):
    if not isinstance(x, str):
        return False
    x = x.replace("-", "")
    return len(x) == 10 and x.isdigit()

def normalize_date(x):
    if x is None or x == "" or str(x) == "nan":
        return None
    try:
        return str(pd.to_datetime(x).date())
    except:
        return None

def normalize_language(x):
    if not x or x == "nan":
        return None
    return str(x).lower().strip()

def normalize_price(x):
    try:
        return float(x)
    except:
        return None

def compute_quality_metrics(df_detail, df_dim):

    return {
        "total_fuente_goodreads": int((df_detail["source"] == "goodreads").sum()),
        "total_fuente_googlebooks": int((df_detail["source"] == "googlebooks").sum()),
        "unique_books_dim": len(df_dim),
        "duplicados_por_isbn13": int(df_dim["isbn13"].duplicated().sum()),
        "nulos_por_campo_dim": df_dim.isna().mean().to_dict(),
        "timestamp": datetime.now().isoformat()
    }
