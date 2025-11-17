import hashlib

def canonical_book_id(row):
    """
    Regla oficial: ISBN13 → si no hay, hash(título+autor+editorial)
    """
    isbn13 = row.get("isbn13")
    if isbn13 and str(isbn13) != "nan":
        return isbn13

    key = (
        str(row.get("title", "")).lower().strip()
        + str(row.get("authors", "")).lower().strip()
        + str(row.get("publisher", "")).lower().strip()
    )

    return hashlib.md5(key.encode()).hexdigest()
