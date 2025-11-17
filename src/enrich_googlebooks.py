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
# 🔍 Función general de búsqueda: varias estrategias
# --------------------------------------------------
def google_books_search(query: str) -> Optional[Dict]:
    params = {"q": query, "maxResults": 1}

    print(f"[DEBUG] Google Books Query: {params}")

    try:
        r = requests.get(GOOGLE_API_URL, headers=HEADERS, params=params, timeout=20)
        if r.status_code != 200:
            print("[WARNING] API error:", r.status_code)
            return None

        data = r.json()
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

    clean_title = title.replace('"', '').replace("'", "")
    primary_author = authors[0] if authors else ""

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

    # 3️⃣ Buscar por título + autor
    if primary_author:
        item = google_books_search(f'intitle:"{clean_title}" inauthor:"{primary_author}"')
        if item:
            return item

    # 4️⃣ Buscar solo por título
    item = google_books_search(f'intitle:"{clean_title}"')
    if item:
        return item

    # 5️⃣ Nada encontrado
    print("[INFO] Sin resultados tras aplicar todas las estrategias.")
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
        if entry["type"] == "ISBN_13":
            isbn13 = entry["identifier"]
        elif entry["type"] == "ISBN_10":
            isbn10 = entry["identifier"]

    # Precio
    price_amount = None
    price_currency = None
    if sale.get("saleability") == "FOR_SALE":
        price = sale.get("retailPrice", {})
        price_amount = price.get("amount")
        price_currency = price.get("currencyCode")

    return {
        "gb_id": item.get("id"),
        "title": volume.get("title"),
        "subtitle": volume.get("subtitle"),
        "authors": "; ".join(volume.get("authors", [])),
        "publisher": volume.get("publisher"),
        "pub_date": volume.get("publishedDate"),
        "language": volume.get("language"),
        "categories": "; ".join(volume.get("categories", []))
        if "categories" in volume else None,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "price_amount": price_amount,
        "price_currency": price_currency
    }


# --------------------------------------------------
# 4. Guardar CSV en landing/
# --------------------------------------------------
def save_googlebooks_csv(rows: list):
    df = pd.DataFrame(rows)
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
        print(f"[INFO] Procesando: {book['title']}")

        isbn13 = book.get("isbn13")
        isbn10 = book.get("isbn")  # Goodreads puede guardar ISBN10 aquí
        title = book.get("title", "")
        authors = book.get("authors", [])

        item = query_google_books(isbn13, isbn10, title, authors)
        time.sleep(0.4)

        if item:
            row = extract_googlebooks_fields(item)
            enriched_rows.append(row)
        else:
            print("[INFO] Libro sin coincidencia en Google Books → se omite")


    save_googlebooks_csv(enriched_rows)
