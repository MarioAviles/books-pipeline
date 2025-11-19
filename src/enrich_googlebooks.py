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
# 🔍 Búsqueda en Google Books
# --------------------------------------------------
def google_books_search_by_isbn(isbn: str) -> Optional[Dict]:
    api_url = f"{GOOGLE_API_URL}?q=isbn:{isbn}"
    print(f"[DEBUG] API call: {api_url}")

    try:
        r = requests.get(api_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"[WARNING] Error HTTP {r.status_code}")
            return None

        data = r.json()
        if "items" not in data:
            return None

        item = data["items"][0]
        item["_query_url"] = api_url
        return item
    except Exception as e:
        print("[ERROR] Excepción en la búsqueda:", e)
        return None


def google_books_search_fallback(query: str) -> Optional[Dict]:
    api_url = f"{GOOGLE_API_URL}?q={query}"
    print(f"[DEBUG] Fallback API call: {api_url}")

    try:
        r = requests.get(api_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None

        data = r.json()
        if "items" not in data:
            return None

        item = data["items"][0]
        item["_query_url"] = api_url
        return item
    except Exception as e:
        print("[ERROR] Excepción en fallback:", e)
        return None


# --------------------------------------------------
# 2. Lógica combinada de búsqueda
# --------------------------------------------------
def query_google_books(isbn13: Optional[str], isbn10: Optional[str], title: str, authors: list) -> Optional[Dict]:
    clean_title = title.replace('"', "").replace("'", "").strip()
    author = authors[0] if isinstance(authors, list) and authors else ""

    if isbn13:
        item = google_books_search_by_isbn(isbn13)
        if item:
            return item
    if isbn10:
        item = google_books_search_by_isbn(isbn10)
        if item:
            return item
    if clean_title and author:
        query = f'intitle:"{clean_title}" inauthor:"{author}"'
        item = google_books_search_fallback(query)
        if item:
            return item
    if clean_title:
        query = f'intitle:"{clean_title}"'
        item = google_books_search_fallback(query)
        if item:
            return item

    print("[INFO] Sin resultados para:", title)
    return None


# --------------------------------------------------
# 3. Extraer campos del JSON de Google Books
# --------------------------------------------------
def extract_googlebooks_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    volume = item.get("volumeInfo", {})
    sale = item.get("saleInfo", {})

    isbn13, isbn10 = None, None
    for entry in volume.get("industryIdentifiers", []):
        if entry.get("type") == "ISBN_13":
            isbn13 = entry.get("identifier")
        elif entry.get("type") == "ISBN_10":
            isbn10 = entry.get("identifier")

    authors = "; ".join(volume.get("authors", [])) if "authors" in volume else None
    categories = "; ".join(volume.get("categories", [])) if "categories" in volume else None

    price_amount, price_currency = None, None
    if sale.get("saleability") == "FOR_SALE":
        price = sale.get("retailPrice", {})
        price_amount = price.get("amount")
        price_currency = price.get("currencyCode")

    info_link = volume.get("infoLink")
    canonical_link = volume.get("canonicalVolumeLink")
    api_query_url = item.get("_query_url")

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
        "info_link": info_link,
        "canonical_link": canonical_link,
        "api_query_url": api_query_url,
    }


# --------------------------------------------------
# 4. Guardar CSV
# --------------------------------------------------
def save_googlebooks_csv(rows: list):
    df = pd.DataFrame(rows)
    ordered_cols = [
        "gb_id", "title", "subtitle", "authors", "publisher",
        "pub_date", "language", "categories",
        "isbn13", "isbn10", "price_amount", "price_currency",
        "info_link", "canonical_link", "api_query_url"
    ]
    df = df.reindex(columns=ordered_cols)
    df = df.drop_duplicates(subset=["isbn13", "title"], keep="first")
    os.makedirs("landing", exist_ok=True)
    df.to_csv(LANDING_PATH, index=False, encoding="utf-8", sep=";")
    print(f"[INFO] Archivo generado sin duplicados: {LANDING_PATH}")


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":
    goodreads = load_goodreads_json()

    # ✅ Filtrar duplicados antes de consultar la API
    unique_books = {(b.get("isbn13"), b.get("title")): b for b in goodreads}.values()

    enriched_rows = []
    for book in unique_books:
        print(f"\n📘 Procesando: {book.get('title')}")
        isbn13 = book.get("isbn13")
        isbn10 = book.get("isbn")
        title = book.get("title", "")
        authors = book.get("authors", [])

        item = query_google_books(isbn13, isbn10, title, authors)
        time.sleep(0.5)
        if item:
            enriched_rows.append(extract_googlebooks_fields(item))
        else:
            print("[INFO] Libro sin coincidencia en Google Books → omitido")

    save_googlebooks_csv(enriched_rows)
