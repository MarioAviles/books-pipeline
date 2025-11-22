# utils_quality.py
# ------------------------------------------
# Funciones relacionadas con calidad,
# métricas y validación de datos.
# ------------------------------------------

import json
from pathlib import Path
import pandas as pd


def save_dataframe_robust(df: pd.DataFrame, path: Path):
    if df is None or df.empty:
        print(f"[WARN] DF vacío, no guardado: {path}")
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = path.with_suffix(".csv")

    try:
        df.to_parquet(path, index=False)
        print(f"[OK] Parquet: {path}")
    except Exception as e:
        print(f"[WARN] No se pudo escribir Parquet ({e}).")

    try:
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8")
        print(f"[OK] CSV: {csv_path}")
    except Exception as e:
        print(f"[ERROR] No se pudo escribir CSV ({e}).")


def write_quality_metrics(path: Path, metrics: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)

def write_schema_markdown(path: Path, df: pd.DataFrame):
    """
    Genera schema.md con:
    Campo | Tipo | Nullable | Formato | Ejemplo | Reglas
    donde se generan reglas automáticas según nombre, tipo y patrón.
    """
    lines = []
    lines.append("| Campo | Tipo | Nullable | Formato | Ejemplo | Reglas |")
    lines.append("|-------|------|----------|----------|---------|---------|")

    for col in df.columns:
        series = df[col]
        tipo = str(series.dtype)
        nullable = "Sí" if series.isnull().any() else "No"

        # Ejemplo real
        ejemplo = None
        for v in series:
            if v is not None:
                ejemplo = v
                break
        if ejemplo is None:
            ejemplo = ""

        # Formato y reglas automáticas
        formato = "string"
        reglas = []

        # ---------- REGLAS INTELIGENTES ----------
        cname = col.lower()

        # Tipos numéricos
        if "int" in tipo or "float" in tipo:
            formato = "numérico"
            reglas.append("Debe ser un número válido")

        # ISBN13
        if cname == "isbn13":
            reglas.append("Debe tener 13 dígitos")
            reglas.append("No debe incluir guiones")
            reglas.append("Debe coincidir con checksum ISBN-13")

        # ISBN10
        if cname == "isbn10":
            reglas.append("Debe tener 10 caracteres")
            reglas.append("Puede incluir 'X' como dígito de control")
            reglas.append("Debe coincidir con checksum ISBN-10")

        # canonical_id
        if cname == "canonical_id":
            reglas.append("Si existe ISBN13 usarlo")
            reglas.append("Si no, usar ISBN10")
            reglas.append("Si no, generar hash SHA-1 estable de título+autor+editor+año")

        # title
        if cname == "title":
            reglas.append("Debe ser texto normalizado (trim espacios)")
            reglas.append("No debe estar vacío")

        # title_normalized
        if cname == "title_normalized":
            reglas.append("Todo minúsculas, sin acentos")
            reglas.append("Solo caracteres alfanuméricos y espacios")

        # authors
        if cname == "authors":
            reglas.append("Lista separada por ' | '")
            reglas.append("Se deben eliminar duplicados")
            reglas.append("Debe incluir autor principal")

        # first_author
        if cname == "first_author":
            reglas.append("Primer autor tras normalización de lista")

        # categories
        if cname == "categories":
            reglas.append("Lista separada por ' | '")
            reglas.append("Valores únicos")
        
        # dates
        if cname in ("pub_date", "ingestion_date_google", "ingestion_date_goodreads"):
            reglas.append("Formato ISO-8601")
            reglas.append("Admite YYYY, YYYY-MM o YYYY-MM-DD")

        if cname == "pub_year":
            reglas.append("Debe ser año entre 1000 y 2100")
            formato = "año numérico"

        # URL
        if "url" in cname:
            reglas.append("Debe ser una URL válida")
            formato = "URL"

        # language
        if cname == "language":
            reglas.append("Código de idioma (ej. 'en', 'es')")

        # rating
        if cname == "rating_value":
            reglas.append("Número entre 0 y 5")
        if cname == "rating_count":
            reglas.append("Número entero ≥ 0")

        # description
        if cname == "description":
            reglas.append("Texto libre, puede contener varias líneas")

        # currency / price
        if cname == "price_currency":
            reglas.append("Debe ser moneda ISO-4217 (ej. EUR, USD)")
        if cname == "price_amount":
            reglas.append("Número decimal con punto o coma")

        # pages
        if cname == "num_pages":
            reglas.append("Debe ser entero > 0")

        # source_preference
        if cname == "source_preference":
            reglas.append("Debe ser 'goodreads' o 'google'")

        # from_google
        if cname == "from_google":
            reglas.append("Booleano (True/False)")

        # merge_method
        if cname == "merge_method":
            reglas.append("Valores válidos: isbn13, heuristic, none")

        # raw sources
        if cname in ("raw_goodreads", "raw_google"):
            formato = "JSON"
            reglas.append("Registro original sin modificar de la fuente")

        # Unir reglas
        reglas_texto = "<br>".join(reglas) if reglas else ""

        # Construir línea
        lines.append(
            f"| {col} | {tipo} | {nullable} | {formato} | {ejemplo} | {reglas_texto} |"
        )

    text = "\n".join(lines)
    path.write_text(text, encoding="utf-8")
    print(f"[OK] Esquema generado: {path}")
