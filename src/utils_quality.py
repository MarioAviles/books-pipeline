import pandas as pd
from datetime import datetime

def validate_isbn13(x):
    if x is None or str(x) == "nan":
        return False
    x = str(x).replace("-", "")
    return len(x) == 13 and x.isdigit()

def validate_isbn10(x):
    if x is None or str(x) == "nan":
        return False
    x = str(x).replace("-", "")
    return len(x) == 10 and x.isdigit()

def normalize_date(x):
    if not x or x == "nan":
        return None
    try:
        # soporta 2021, 2021-05, 2021-05-10
        return str(pd.to_datetime(x).date())
    except:
        return None

def normalize_language(x):
    if not x or x == "nan":
        return None
    return x.lower()

def normalize_price(x):
    if not x or x == "nan":
        return None
    try:
        return float(x)
    except:
        return None

def detect_nulls(df):
    return df.isna().mean().to_dict()

def detect_duplicates(df):
    return df["book_id"].duplicated().sum()

def compute_quality_metrics(detail, dim):

    return {
        "rows_goodreads": int((detail["source"] == "goodreads").sum()),
        "rows_googlebooks": int((detail["source"] == "googlebooks").sum()),
        "unique_books": len(dim),
        "duplicates_resueltos": detect_duplicates(detail),
        "porcentaje_nulos": detect_nulls(detail),
        "timestamp": datetime.now().isoformat()
    }
