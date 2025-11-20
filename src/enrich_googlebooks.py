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
# 1️⃣ Cargar JSON de Goodreads
# --------------------------------------------------
def load_goodreads_json() -> list:
    print("[INFO] Cargando goodreads_books.json...")
    with open(GOODREADS_JSON, "r", encoding="utf-8") as f:
        return json.load(f)


# --------------------------------------------------
# 2️⃣ Buscar en Google Books (única función)
# --------------------------------------------------
def google_books_search(query: str) -> Optional[Dict]:
    """Ejecuta una búsqueda en Google Books y devuelve el primer resultado"""
    api_url = f"{GOOGLE_API_URL}?q={query}"
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
        print(f"[ERROR] Excepción en la búsqueda: {e}")
        return None


# --------------------------------------------------
# 3️⃣ Lógica combinada de búsqueda
# --------------------------------------------------
def query_google_books(isbn13: Optional[str], isbn10: Optional[str], title: str, authors: list) -> Optional[Dict]:
    """Busca un libro priorizando ISBN, luego título+autor"""
    clean_title = title.replace('"', "").replace("'", "").strip()
    author = authors[0] if isinstance(authors, list) and authors else ""

    for query in filter(None, [
        f"isbn:{isbn13}" if isbn13 else None,
        f"isbn:{isbn10}" if isbn10 else None,
        f'intitle:"{clean_title}" inauthor:"{author}"' if clean_title and author else None,
        f'intitle:"{clean_title}"' if clean_title else None
    ]):
        item = google_books_search(query)
        if item:
            return item

    print(f"[INFO] Sin resultados para: {title}")
    return None


# --------------------------------------------------
# 4️⃣ Extraer campos del JSON de Google Books
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

    price_amount = sale.get("retailPrice", {}).get("amount") if sale.get("saleability") == "FOR_SALE" else None
    price_currency = sale.get("retailPrice", {}).get("currencyCode") if sale.get("saleability") == "FOR_SALE" else None

    return {
        "gb_id": item.get("id"),
        "title": volume.get("title"),
        "subtitle": volume.get("subtitle"),
        "authors": "; ".join(volume.get("authors", [])) if "authors" in volume else None,
        "publisher": volume.get("publisher"),
        "pub_date": volume.get("publishedDate"),
        "language": volume.get("language"),
        "categories": "; ".join(volume.get("categories", [])) if "categories" in volume else None,
        "isbn13": isbn13,
        "isbn10": isbn10,
        "price_amount": price_amount,
        "price_currency": price_currency,
        "info_link": volume.get("infoLink"),
        "canonical_link": volume.get("canonicalVolumeLink"),
        "api_query_url": item.get("_query_url"),
    }


# --------------------------------------------------
# 5️⃣ Guardar CSV limpio y ordenado
# --------------------------------------------------
def save_googlebooks_csv(rows: list):
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["isbn13", "title"], keep="first")
    ordered_cols = [
        "gb_id", "title", "subtitle", "authors", "publisher",
        "pub_date", "language", "categories",
        "isbn13", "isbn10", "price_amount", "price_currency",
        "info_link", "canonical_link", "api_query_url"
    ]
    df = df.reindex(columns=ordered_cols)
    os.makedirs("landing", exist_ok=True)
    df.to_csv(LANDING_PATH, index=False, encoding="utf-8", sep=";")
    print(f"[INFO] Archivo generado sin duplicados: {LANDING_PATH}")


# --------------------------------------------------
# 🚀 MAIN
# --------------------------------------------------
if __name__ == "__main__":
    goodreads = load_goodreads_json()

    # ✅ Evitar duplicados antes de consultar
    unique_books = {(b.get("isbn13"), b.get("title")): b for b in goodreads}.values()

    enriched_rows = []
    for book in unique_books:
        print(f"\n📘 Procesando: {book.get('title')}")
        item = query_google_books(book.get("isbn13"), book.get("isbn"), book.get("title", ""), book.get("authors", []))
        time.sleep(0.5)
        if item:
            enriched_rows.append(extract_googlebooks_fields(item))
        else:
            print("[INFO] Libro sin coincidencia en Google Books → omitido")

    save_googlebooks_csv(enriched_rows)
