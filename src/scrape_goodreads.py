# ===============================================
# 📦 Imports y configuración base
# ===============================================
from bs4 import BeautifulSoup
import requests, re, json, time, os
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import List, Dict, Optional

# ===============================================
# 🌍 Constantes y sesión HTTP
# ===============================================
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
    author_principal: Optional[str] = None

    rating_value: Optional[float] = None
    rating_count: Optional[int] = None
    review_count: Optional[int] = None

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

    genres: List[str] = field(default_factory=list)
    review_count_by_lang: Dict[str, int] = field(default_factory=dict)
    ingestion_date: Optional[str] = None


# ===============================================
# 🎭 Función para extraer géneros
# ===============================================
def extract_genres(html: str) -> List[str]:
    try:
        block = re.search(r'"bookGenres":(\[.*?\])', html, flags=re.DOTALL)
        if not block:
            return []
        genres_json = json.loads(block.group(1))
        return [g["genre"]["name"] for g in genres_json]
    except:
        return []


# ===============================================
# 🔍 Parseo HTML principal
# ===============================================
def parse_basic(html: str, book_id: str) -> BookData:
    soup = BeautifulSoup(html, "html.parser")

    title = soup.find(class_="Text Text__title1")
    title = title.get_text(strip=True) if title else None

    authors = [a.get_text(strip=True) for a in soup.find_all(class_="ContributorLink__name")]
    authors = [re.sub(r"\s+", " ", a).strip() for a in authors]
    authors = list(dict.fromkeys(authors))

    rating_el = soup.find(class_="RatingStatistics__rating")
    rating_value = float(rating_el.get_text(strip=True)) if rating_el else None

    desc_el = soup.find(class_="DetailsLayoutRightParagraph__widthConstrained")
    desc = desc_el.get_text(" ", strip=True) if desc_el else None

    pub_info_el = soup.find("p", {"data-testid": "publicationInfo"})
    pub_info = pub_info_el.get_text(" ", strip=True) if pub_info_el else None

    cover_el = soup.find(class_="ResponsiveImage")
    cover = cover_el.get("src") if cover_el else None

    rating_count = re.search(r'"ratingCount":(\d+)', html)
    review_count = re.search(r'"reviewCount":(\d+)', html)
    rating_count = int(rating_count.group(1)) if rating_count else None
    review_count = int(review_count.group(1)) if review_count else None

    review_count_by_lang = {}
    matches = re.findall(r'"count":(\d+),"isoLanguageCode":"([a-z]{2})"', html)
    for count, lang in matches:
        review_count_by_lang[lang] = review_count_by_lang.get(lang, 0) + int(count)

    genres = extract_genres(html)

    return BookData(
        id=book_id,
        url=f"{BASE_URL}{book_id}",
        title=title,
        authors=authors,
        author_principal=authors[0] if authors else None,
        rating_value=rating_value,
        rating_count=rating_count,
        review_count=review_count,
        desc=desc,
        pub_info=pub_info,
        cover=cover,
        review_count_by_lang=review_count_by_lang,
        genres=genres
    )


# ===============================================
# 📚 Parseo JSON interno (details)
# ===============================================
def parse_details_from_embedded_json(html: str, bd: BookData) -> BookData:
    try:
        match = re.search(r'"details"\s*:\s*({.*?})\s*,\s*"', html, flags=re.DOTALL)
        if not match:
            return bd

        details = json.loads(match.group(1))

        bd.format = details.get("format")
        bd.num_pages = details.get("numPages")
        bd.publisher = details.get("publisher")
        bd.isbn = details.get("isbn")
        bd.isbn13 = details.get("isbn13")

        ts = details.get("publicationTime")
        bd.publication_timestamp = ts
        if ts:
            bd.publication_date = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")

        lang = details.get("language")
        if isinstance(lang, dict):
            bd.language = lang.get("name", "").lower().strip()

        return bd

    except:
        return bd


# ===============================================
# 🌐 Obtener HTML (solo Requests)
# ===============================================
def fetch_book_html(book_id: str) -> Optional[str]:
    url = f"{BASE_URL}{book_id}"
    try:
        r = SESSION.get(url, timeout=30)
        if r.status_code == 200:
            return r.text
        print(f"[WARNING] Código HTTP {r.status_code} para {url}")
    except Exception as e:
        print(f"[ERROR] No se pudo obtener {url}: {e}")
    return None


# ===============================================
# 📘 Obtener datos del libro
# ===============================================
def get_book(book_id: str) -> BookData:
    html = fetch_book_html(book_id)
    if html is None:
        return BookData(id=book_id, url=f"{BASE_URL}{book_id}")

    bd = parse_basic(html, book_id)
    bd = parse_details_from_embedded_json(html, bd)
    bd.ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return bd


# ===============================================
# 🔎 Obtener IDs de libros desde búsqueda
# ===============================================
def get_book_ids_from_search(search_url: str, limit: int = 15) -> List[str]:
    ids = []
    page = 1
    while len(ids) < limit:
        paged_url = f"{search_url}&page={page}"
        r = SESSION.get(paged_url, timeout=30)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")
        links = soup.select("a.bookTitle")
        if not links:
            break

        for link in links:
            href = link.get("href", "")
            m = re.search(r'/book/show/(\d+)', href)
            if m:
                ids.append(m.group(1))
                if len(ids) >= limit:
                    break

        page += 1
        time.sleep(0.7)
    return ids[:limit]


# ===============================================
# 🚀 EJECUCIÓN PRINCIPAL
# ===============================================
if __name__ == "__main__":
    search_url = "https://www.goodreads.com/search?q=data+science"

    print("📚 Buscando libros...")
    book_ids = get_book_ids_from_search(search_url, limit=15)
    print("🔍 IDs obtenidos:", book_ids)
    print("\n🕸️ Scrapeando libros...\n")

    books = []
    for bid in book_ids:
        bd = get_book(bid)
        books.append(bd)
        time.sleep(0.8)

    os.makedirs("landing", exist_ok=True)
    output_path = "landing/goodreads_books.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump([asdict(b) for b in books], f, indent=4, ensure_ascii=False)

    print(f"\n✅ Scraping completado. Archivo generado: {output_path}")
