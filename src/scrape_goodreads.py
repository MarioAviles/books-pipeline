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

    rating_value: Optional[float] = None
    rating_count: Optional[int] = None

    isbn: Optional[str] = None
    isbn13: Optional[str] = None

    format: Optional[str] = None
    num_pages: Optional[int] = None
    publisher: Optional[str] = None
    publication_timestamp: Optional[int] = None
    publication_date: Optional[str] = None
    language: Optional[str] = None

    genres: List[str] = field(default_factory=list)
    description: Optional[str] = None

    ingestion_date: Optional[str] = None


# ===============================================
# 🧱 Extraer descripción
# ===============================================
def extract_description(html: str) -> Optional[str]:
    # 1) React __NEXT_DATA__
    try:
        m = re.search(r'<script id="__NEXT_DATA__".*?>(.*?)</script>',
                      html, flags=re.DOTALL)
        if m:
            data = json.loads(m.group(1))

            def find_desc(obj):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        if k.lower() == "description" and isinstance(v, str) and v.strip():
                            return v
                        r = find_desc(v)
                        if r:
                            return r
                elif isinstance(obj, list):
                    for item in obj:
                        r = find_desc(item)
                        if r:
                            return r
                return None

            desc = find_desc(data)
            if desc:
                return desc.strip()

    except:
        pass

    # 2) ld+json
    try:
        blocks = re.findall(r'<script type="application/ld\+json">(.*?)</script>',
                            html, flags=re.DOTALL)
        for block in blocks:
            try:
                d = json.loads(block)
                if isinstance(d, dict) and "description" in d:
                    desc = d["description"]
                    if isinstance(desc, str) and desc.strip():
                        return desc.strip()
            except:
                continue
    except:
        pass

    return None


# ===============================================
# 🎭 Extraer géneros
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
# 🔍 Parseo principal
# ===============================================
def parse_basic(html: str, book_id: str) -> BookData:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.find(class_="Text Text__title1")
    title = title_el.get_text(strip=True) if title_el else None

    authors = [a.get_text(strip=True) for a in soup.find_all(class_="ContributorLink__name")]
    authors = list(dict.fromkeys(authors)) if authors else []

    rating_el = soup.find(class_="RatingStatistics__rating")
    rating_value = float(rating_el.get_text(strip=True)) if rating_el else None

    rating_count_match = re.search(r'"ratingCount"\s*:\s*"?(?P<num>[\d,\.]+)"?', html)
    rating_count = int(re.sub(r"[^\d]", "", rating_count_match.group("num"))) if rating_count_match else None

    isbn10_match = re.search(r'"isbn":"([0-9Xx]{10})"', html)
    isbn13_match = re.search(r'"isbn13":"(\d{13})"', html)

    isbn10 = isbn10_match.group(1) if isbn10_match else None
    isbn13 = isbn13_match.group(1) if isbn13_match else None

    return BookData(
        id=book_id,
        url=f"{BASE_URL}{book_id}",
        title=title,
        authors=authors,
        rating_value=rating_value,
        rating_count=rating_count,
        isbn=isbn10,
        isbn13=isbn13,
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

        bd.isbn = details.get("isbn") or bd.isbn
        bd.isbn13 = details.get("isbn13") or bd.isbn13

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
# 🌐 Obtener HTML del libro
# ===============================================
def fetch_book_html(book_id: str) -> Optional[str]:
    try:
        r = SESSION.get(f"{BASE_URL}{book_id}", timeout=30)
        if r.status_code == 200:
            return r.text
    except:
        pass
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
    bd.genres = extract_genres(html)
    bd.description = extract_description(html)
    bd.ingestion_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return bd


# ===============================================
# 🔎 Obtener IDs de libro de la búsqueda
# ===============================================
def get_book_ids_from_search(search_url: str, limit: int = 20) -> List[str]:
    ids = []
    seen = set()
    page = 1

    print("📚 Buscando libros...")

    while len(ids) < limit:
        paged = f"{search_url}&page={page}"
        print(f"[INFO] Página {page}: {paged}")

        r = SESSION.get(paged, timeout=30)
        if r.status_code != 200:
            break

        soup = BeautifulSoup(r.text, "html.parser")

        # Capturar MUCHOS más enlaces
        links = soup.select('a[href*="/book/show/"]')
        if not links:
            break

        for l in links:
            href = l.get("href", "")
            m = re.search(r'/book/show/(\d+)', href)
            if m:
                bid = m.group(1)
                if bid not in seen:
                    seen.add(bid)
                    ids.append(bid)
                    print(" ➕ Nuevo ID:", bid)
                    if len(ids) >= limit:
                        break

        page += 1
        time.sleep(0.7)

    print("📌 IDs finales:", ids)
    return ids[:limit]


# ===============================================
# 🚀 MAIN
# ===============================================
if __name__ == "__main__":
    search_url = "https://www.goodreads.com/search?q=data+science"

    ids = get_book_ids_from_search(search_url, limit=20)

    books = []
    for bid in ids:
        print("⛏️  Scrapeando:", bid)
        bd = get_book(bid)
        books.append(bd)
        time.sleep(0.8)

    os.makedirs("landing", exist_ok=True)
    out = "landing/goodreads_books.json"

    with open(out, "w", encoding="utf-8") as f:
        json.dump([asdict(b) for b in books], f, indent=4, ensure_ascii=False)

    print(f"✅ Archivo generado: {out}")
