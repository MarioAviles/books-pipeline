# utils_isbn.py
# ------------------------------------------
# Funciones relacionadas con ISBN, hashes,
# títulos normalizados y autores.
# ------------------------------------------

import re
import hashlib
import unicodedata
from typing import Any, Optional, List
from dateutil import parser as date_parser
from datetime import datetime


# -------------------------
# Normalización de strings
# -------------------------

def normalize_str(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None
    s = re.sub(r"\s+", " ", s)
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def normalize_title(title: Any) -> Optional[str]:
    t = normalize_str(title)
    if not t:
        return None
    t = _strip_accents(t.lower())
    t = re.sub(r"[^\w\s]", "", t)
    return re.sub(r"\s+", " ", t).strip()


# -------------------------
# Autores
# -------------------------

def normalize_author(auth: Any) -> List[str]:
    if not auth:
        return []
    if isinstance(auth, str):
        parts = re.split(r"[|,;]", auth)
    elif isinstance(auth, list):
        parts = auth
    else:
        return []
    return [normalize_str(p) for p in parts if normalize_str(p)]


def get_first_author(auth: Any) -> str:
    a = normalize_author(auth)
    return a[0] if a else ""


# -------------------------
# ISBN / Identificador canonico
# -------------------------

def stable_hash(fields: List[str]) -> str:
    return hashlib.sha1("||".join(f or "" for f in fields).encode()).hexdigest()


def canonical_id_from_data(title: str, first_author: str, publisher: str, pub_year: str) -> str:
    return stable_hash([
        normalize_title(title) or "",
        first_author or "",
        publisher or "",
        pub_year or ""
    ])


# -------------------------
# Fechas
# -------------------------

def iso_date(v: Any) -> Optional[str]:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None

    try:
        dt = date_parser.parse(s, default=datetime(1, 1, 1))
        y, m, d = dt.year, dt.month, dt.day
        if d != 1:
            return dt.date().isoformat()
        if m != 1:
            return f"{y:04d}-{m:02d}"
        return f"{y:04d}"
    except:
        pass

    # patrones directos
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}-\d{2}$", s):
        return s
    if re.match(r"^\d{4}$", s):
        return s

    return None
