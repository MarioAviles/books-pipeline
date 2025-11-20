import hashlib
import pandas as pd

def canonical_book_id(row):
    """
    Regla oficial:
    1. Si hay ISBN13 válido → usarlo
    2. Si no → hash(título + autor_principal + editorial)
    """

    isbn13 = row.get("isbn13")

    # Caso 1: ISBN13 válido
    if isinstance(isbn13, str) and isbn13.strip() != "" and isbn13.strip().isdigit():
        return isbn13.strip()

    # Caso 2: Generar ID canónico
    title = str(row.get("title", "")).lower().strip()

    # autor_principal puede venir como lista → tomar primero
    authors = row.get("authors", "")
    if isinstance(authors, list) and len(authors) > 0:
        author_principal = str(authors[0]).lower().strip()
    else:
        author_principal = str(authors).lower().strip()

    publisher = str(row.get("publisher", "")).lower().strip()

    key = title + author_principal + publisher
    
    # SHA1 más seguro que MD5
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
