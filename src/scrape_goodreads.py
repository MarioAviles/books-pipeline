# ===============================================
# 📦 Imports y utilidades
# ===============================================
from bs4 import BeautifulSoup
import requests, re, json, time
import pandas as pd

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def make_headless_chrome():
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")

    return webdriver.Chrome(options=opts)


BASE_URL = "https://www.goodreads.com/book/show/"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
})

# ===============================================
# 🧱 Dataclass BookData
# ===============================================
@dataclass
class BookData:
    id: str
    url: str

    title: Optional[str] = None
    authors: List[str] = field(default_factory=list)

    rating_value: Optional[float] = None
    desc: Optional[str] = None
    pub_info: Optional[str] = None
    cover: Optional[str] = None

    format: Optional[str] = None
    num_pages: Optional[int] = None

    publication_timestamp: Optional[int] = None
    publication_date: Optional[str] = None

    publisher: Optional[str] = None
    isbn: Optional[str] = None
    isbn13: Optional[str] = None
    language: Optional[str] = None

    review_count_by_lang: Dict[str, int] = field(default_factory=dict)
    genres: List[str] = field(default_factory=list)

    rating_count: Optional[int] = None
    review_count: Optional[int] = None

    comments: List[Dict] = field(default_factory=list)

    ingestion_date: Optional[str] = None

# ===============================================
# 🕸️ Parsing básico
# ===============================================
def parse_basic(html: str, book_id: str) -> BookData:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find(class_="Text Text__title1")
    title = title_el.get_text(strip=True) if title_el else None

    authors = [a.get_text(strip=True) for a in soup.find_all(class_="ContributorLink__name")]

    rating_el = soup.find(class_="RatingStatistics__rating")
    rating_value = float(rating_el.get_text(strip=True)) if rating_el else None

    desc_el = soup.find(class_="DetailsLayoutRightParagraph__widthConstrained")
    desc = desc_el.get_text(" ", strip=True) if desc_el else None

    pub_info_el = soup.find("p", {"data-testid": "publicationInfo"})
    pub_info = pub_info_el.get_text(" ", strip=True) if pub_info_el else None

    cover_el = soup.find(class_="ResponsiveImage")
    cover = cover_el.get("src") if cover_el else None

    rating_count = None
    review_count = None
    m1 = re.search(r'"ratingCount":(\d+)', html)
    if m1:
        rating_count = int(m1.group(1))
    m2 = re.search(r'"reviewCount":(\d+)', html)
    if m2:
        review_count = int(m2.group(1))

    review_count_by_lang = {}
    matches = re.findall(r'"count":(\d+),"isoLanguageCode":"([a-z]{2})"', html)
    for count, lang in matches:
        review_count_by_lang[lang] = review_count_by_lang.get(lang, 0) + int(count)

    genres = []
    try:
        block = re.findall(r'"bookGenres":.*?}}],"details":', html, flags=re.DOTALL)[0]
        block = block.rstrip(',"details":')
        genres_json = json.loads("{" + block + "}")
        genres = [g["genre"]["name"] for g in genres_json.get("bookGenres", [])]
    except:
        pass

    return BookData(
        id=book_id, url=f"{BASE_URL}{book_id}",
        title=title, authors=authors,
        rating_value=rating_value, desc=desc, pub_info=pub_info,
        cover=cover, review_count_by_lang=review_count_by_lang,
        genres=genres, rating_count=rating_count, review_count=review_count
    )

# ===============================================
# 🧩 Parse JSON interno de Goodreads
# ===============================================
def parse_details_from_embedded_json(html: str, bd: BookData) -> BookData:
    try:
        match = re.search(r'"details"\s*:\s*({.*?})\s*,\s*"work"', html, flags=re.DOTALL)
        if not match:
            return bd

        details = json.loads(match.group(1))

        bd.format = details.get("format")
        bd.num_pages = details.get("numPages")
        bd.publication_timestamp = details.get("publicationTime")

        if bd.publication_timestamp:
            bd.publication_date = datetime.fromtimestamp(
                bd.publication_timestamp / 1000
            ).strftime("%Y-%m-%d")

        bd.publisher = details.get("publisher")
        bd.isbn = details.get("isbn")
        bd.isbn13 = details.get("isbn13")

        lang = details.get("language") or {}
        bd.language = lang.get("name") if isinstance(lang, dict) else None

    except:
        pass

    return bd

# ===============================================
# 🌐 Descarga del HTML
# ===============================================
def fetch_book_html_requests(book_id: str) -> Optional[str]:
    url = f"{BASE_URL}{book_id}"
    print(f"[DEBUG] Descargando libro con Requests: {url}")

    try:
        r = SESSION.get(url, timeout=30)
        print(f"[DEBUG] Status code Requests: {r.status_code}")

        if r.status_code == 200:
            print(f"[DEBUG] HTML recibido correctamente ({len(r.text)} bytes)")
            return r.text
        else:
            print("[WARNING] Requests devolvió un código distinto a 200")
            return None
    except Exception as e:
        print(f"[ERROR] Excepción en Requests: {e}")
        return None



def fetch_book_html_selenium(book_id: str) -> Optional[str]:
    url = f"{BASE_URL}{book_id}"
    print(f"[DEBUG] Intentando con Selenium: {url}")

    driver = make_headless_chrome()
    try:
        driver.get(url)
        time.sleep(2)
        html = driver.page_source
        print(f"[DEBUG] Selenium devolvió HTML de {len(html)} bytes")
        return html
    except Exception as e:
        print(f"[ERROR] Selenium lanzó excepción: {e}")
        return None
    finally:
        driver.quit()


# ===============================================
# 📘 Obtener datos del libro
# ===============================================
def get_book(book_id: str) -> BookData:
    print(f"\n===============================")
    print(f"[DEBUG] Procesando libro ID: {book_id}")
    print("===============================")

    html = fetch_book_html_requests(book_id)

    if html is None:
        print("[WARNING] Requests falló. Probando Selenium...")
        html = fetch_book_html_selenium(book_id)

    if html is None:
        print("[ERROR] No se pudo obtener HTML para este libro.")
        return BookData(id=book_id, url=f"{BASE_URL}{book_id}")

    print("[DEBUG] Parseando datos del libro...")

    bd = parse_basic(html, book_id)
    bd = parse_details_from_embedded_json(html, bd)

    print(f"[DEBUG] Título obtenido: {bd.title}")
    print(f"[DEBUG] Autores: {bd.authors}")

    bd.ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


    return bd


# ===============================================
# 🔍 Extraer IDs desde categoría
# ===============================================
def get_book_ids_from_genre(genre_url: str, limit: int = 15) -> List[str]:
    print(f"[DEBUG] Accediendo a URL de género: {genre_url}")

    r = SESSION.get(genre_url, timeout=30)
    print(f"[DEBUG] Status code: {r.status_code}")

    if r.status_code != 200:
        print("[ERROR] No se pudo acceder al género.")
        return []

    print("[DEBUG] Procesando HTML de la página del género...")

    soup = BeautifulSoup(r.text, "html.parser")

    links = soup.select("a.bookTitle")
    print(f"[DEBUG] Enlaces encontrados con .bookTitle: {len(links)}")

    ids = []
    for idx, link in enumerate(links[:limit]):
        href = link.get("href", "")
        print(f"[DEBUG] Link #{idx}: href={href}")

        m = re.search(r'/book/show/(\d+)', href)
        if m:
            book_id = m.group(1)
            print(f"[DEBUG] → ID detectado: {book_id}")
            ids.append(book_id)
        else:
            print("[WARNING] No se pudo extraer ID de este enlace.")

    print(f"[DEBUG] IDs finales: {ids}")

    return ids


# ===============================================
# 🚀 EJECUCIÓN PRINCIPAL
# ===============================================

import os

# ===============================================
# 🚀 EJECUCIÓN PRINCIPAL
# ===============================================
if __name__ == "__main__":
    genre_url = f"https://www.goodreads.com/search?q=data+science"

    print("Buscando libros en la categoría...\n")
    book_ids = get_book_ids_from_genre(genre_url, limit=10)

    print("IDs encontrados:", book_ids, "\n")

    print("Scrapeando libros...\n")
    books = [get_book(bid) for bid in book_ids]

    books_json = [asdict(b) for b in books]

    # Crear carpeta landing si no existe
    os.makedirs("landing", exist_ok=True)

    output_path = os.path.join("landing", "goodreads_books.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(books_json, f, indent=4, ensure_ascii=False)

    print(f"Scraping completado. Archivo generado: {output_path}")

