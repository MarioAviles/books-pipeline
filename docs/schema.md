# Esquema de dim_book

Campo | Tipo | Nullable | Formato | Ejemplo | Reglas
---|---|---|---|---|---
book_id | object | No | string (ISBN-13 o hash) | 9781119741763 | Único, no nulo
title | object | No | string | Becoming a Data Head | Trim y capitalización correcta
subtitle | object | Sí | string o nulo | How to Think, Speak, and Understand Data Science | Opcional
publisher | object | Sí | string | Wiley | Normalizado y limpio
isbn13 | object | Sí | string (13 dígitos) | 9781119741763 | Validado por checksum
isbn10 | object | Sí | string (10 dígitos) | 1119741769 | Derivado o validado si existe
pub_date_norm | object | Sí | YYYY-MM-DD (ISO-8601) | 2021-04-13 | Debe ser fecha válida
language_norm | object | Sí | BCP-47 | en | Minúsculas; formato válido
price_amount_norm | float64 | Sí | decimal(10,2) | 27.99 | ≥ 0 o nulo
price_currency | object | Sí | ISO-4217 | EUR | Tres letras mayúsculas
categories | object | Sí | lista[string] | ['Business & Economics'] | Sin duplicados
authors | object | No | lista[string] | ['Alex J Gutman', 'Jordan Goldmeier'] | Sin duplicados ni nulos
fuente_ganadora | object | No | string (URL) | https://play.google.com/store/books/details?id=GCUqEAAAQBAJ | Debe ser URL válida
ts_ultima_actualizacion | object | No | timestamp ISO-8601 | 2025-11-19T10:56:30.416815 | Autogenerado
