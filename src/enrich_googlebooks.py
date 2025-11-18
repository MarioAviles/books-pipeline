import json
import time
import requests
import pandas as pd
import os
from typing import Dict, Any, Optional

GOOGLE_API_URL = "https://www.googleapis.com/books/v1/volumes"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

LANDING_PATH = "landing/googlebooks_books.csv"
GOODREADS_JSON = "landing/goodreads_books.json"


# --------------------------------------------------
# 1. Cargar JSON de Goodreads
# --------------------------------------------------
def load_goodreads_json() -> list:
    print("[INFO] Cargando goodreads_books.json...")
    with open(GOODREADS_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


# --------------------------------------------------
# 🔍 Función general de búsqueda + retry inteligente
# --------------------------------------------------
def google_books_search(query: str, retry: int = 2) -> Optional[Dict]:
    params = {"q": query, "maxResults": 1}
    print(f"[DEBUG] Google Books Query: {params}")

    try:
        r = requests.get(GOOGLE_API_URL, headers=HEADERS, params=params, timeout=20)

        if r.status_code != 200:
            print(f"[WARNING] API error: {r.status_code}")
            return None

        data = r.json()

        # Manejo de rate limit
        if "error" in data and retry > 0:
            reason = data["error"]["errors"][0].get("reason")
            if reason == "rateLimitExceeded":
                print("[WARNING] Rate limit — reintentando...")
                time.sleep(2)
                return google_books_search(query, retry - 1)

        if "items" not in data:
            return None

        return data["items"][0]

    except Exception as e:
        print("[ERROR] Excepción en Google Books:", e)
        return None


# --------------------------------------------------
# 🔎 2. Lógica de búsqueda con fallback
# --------------------------------------------------
def query_google_books(isbn13: Optional[str], isbn10: Optional[str], title: str, authors: list) -> Optional[Dict]:

    # limpiar título de problemas comunes
    clean_title = (
        title.replace('"', "")
             .replace("'", "")
             .replace("“", "")
             .replace("”", "")
             .strip()
    )

    primary_author = authors[0] if isinstance(authors, list) and authors else ""

    # 1️⃣ Buscar por ISBN13
    if isbn13:
        item = google_books_search(f"isbn:{isbn13}")
        if item:
            return item

    # 2️⃣ Buscar por ISBN10
    if isbn10:
        item = google_books_search(f"isbn:{isbn10}")
        if item:
            return item

    # 3️⃣ Título + autor
    if clean_title and primary_author:
        item = google_books_search(f'intitle:"{clean_title}" inauthor:"{primary_author}"')
        if item:
            return item

    # 4️⃣ Solo título
    if clean_title:
        item = google_books_search(f'intitle:"{clean_title}"')
        if item:
            return item

    print("[INFO] Sin resultados tras todas las estrategias")
    return None


# --------------------------------------------------
# 3. Extraer campos del JSON de Google Books
# --------------------------------------------------
def extract_googlebooks_fields(item: Dict[str, Any]) -> Dict[str, Any]:

    volume = item.get("volumeInfo", {})
    sale = item.get("saleInfo", {})

    # ISBNs
    isbn13 = None
    isbn10 = None

    for entry in volume.get("industryIdentifiers", []):
        t = entry.get("type")
        if t == "ISBN_13":
            isbn13 = entry.get("identifier")
        elif t == "ISBN_10":
            isbn10 = entry.get("identifier")

    # Autores
    authors_raw = volume.get("authors")
    authors = "; ".join(authors_raw) if isinstance(authors_raw, list) else None

    # Categorías
    cats_raw = volume.get("categories", [])
    if isinstance(cats_raw, list):
        cats_clean = [c for c in cats_raw if isinstance(c, str)]
        categories = "; ".join(cats_clean) if cats_clean else None
    else:
        categories = None

    # Precio
    price_amount = None
    price_currency = None

    if sale.get("saleability") == "FOR_SALE":
        price = sale.get("retailPrice", {})
        raw_amount = price.get("amount")
        if isinstance(raw_amount, str):
            raw_amount = raw_amount.replace(",", ".")
        try:
            price_amount = float(raw_amount)
        except:
            price_amount = None
        price_currency = price.get("currencyCode")

    return {
        "gb_id": item.get("id"),
        "title": volume.get("title"),
        "subtitle": volume.get("subtitle"),
        "authors": authors,
        "publisher": volume.get("publisher"),
        "pub_date": volume.get("publishedDate"),
        "language": volume.get("language"),
        "categories": categories,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "price_amount": price_amount,
        "price_currency": price_currency,
    }


# --------------------------------------------------
# 4. Guardar CSV en landing/
# --------------------------------------------------
def save_googlebooks_csv(rows: list):
    df = pd.DataFrame(rows)

    # orden de columnas consistente
    ordered_cols = [
        "gb_id", "title", "subtitle", "authors", "publisher",
        "pub_date", "language", "categories",
        "isbn13", "isbn10",
        "price_amount", "price_currency"
    ]

    df = df.reindex(columns=ordered_cols)

    os.makedirs("landing", exist_ok=True)
    df.to_csv(LANDING_PATH, index=False, encoding="utf-8", sep=";")

    print(f"[INFO] Archivo generado: {LANDING_PATH}")


# --------------------------------------------------
# Main
# --------------------------------------------------
if __name__ == "__main__":

    goodreads = load_goodreads_json()
    enriched_rows = []

    for book in goodreads:
        print("\n============================================")
        print(f"[INFO] Procesando: {book.get('title')}")

        isbn13 = book.get("isbn13")
        isbn10_raw = book.get("isbn")
        isbn10 = isbn10_raw if isbn10_raw and str(isbn10_raw).strip() else None

        title = book.get("title", "")
        authors = book.get("authors", [])

        item = query_google_books(isbn13, isbn10, title, authors)
        time.sleep(0.4)

        if item:
            enriched_rows.append(extract_googlebooks_fields(item))
        else:
            print("[INFO] Libro sin coincidencia en Google Books → se omite")

    save_googlebooks_csv(enriched_rows)
