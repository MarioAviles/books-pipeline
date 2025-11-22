| Campo | Tipo | Nullable | Formato | Ejemplo | Reglas |
|-------|------|----------|----------|---------|---------|
| canonical_id | object | No | string | 9781449336097 | Si existe ISBN13 usarlo<br>Si no, usar ISBN10<br>Si no, generar hash SHA-1 estable de título+autor+editor+año |
| isbn13 | object | Sí | string | 9781449336097 | Debe tener 13 dígitos<br>No debe incluir guiones<br>Debe coincidir con checksum ISBN-13 |
| isbn10 | object | Sí | string | 1449336094 | Debe tener 10 caracteres<br>Puede incluir 'X' como dígito de control<br>Debe coincidir con checksum ISBN-10 |
| title | object | No | string | What Is Data Science? | Debe ser texto normalizado (trim espacios)<br>No debe estar vacío |
| authors | object | No | string | Mike Loukides | Lista separada por ' | '<br>Se deben eliminar duplicados<br>Debe incluir autor principal |
| first_author | object | No | string | Mike Loukides | Primer autor tras normalización de lista |
| publisher | object | Sí | string | O'Reilly Media |  |
| pub_date | object | No | string | 2012-04-10 | Formato ISO-8601<br>Admite YYYY, YYYY-MM o YYYY-MM-DD |
| pub_year | int64 | No | año numérico | 2012 | Debe ser un número válido<br>Debe ser año entre 1000 y 2100 |
| language | object | Sí | string | english | Código de idioma (ej. 'en', 'es') |
| categories | object | No | string | Science | Technology | Nonfiction | Computer Science | Programming | Business | Professional Development | Software | Ebooks | Computers | Lista separada por ' | '<br>Valores únicos |
| num_pages | int64 | No | numérico | 23 | Debe ser un número válido<br>Debe ser entero > 0 |
| format | object | No | string | Kindle Edition |  |
| description | object | No | string | We've all heard it: according to Hal Varian, statistics is the next sexy job. Five years ago, in What is Web 2.0, Tim O'Reilly said that "data is the next Intel Inside." But what does that statement mean? Why do we suddenly care about statistics and about data? This report examines the many sides of data science -- the technologies, the companies and the unique skill sets.The web is full of "data-driven apps." Almost any e-commerce application is a data-driven application. There's a database behind a web front end, and middleware that talks to a number of other databases and data services (credit card processing companies, banks, and so on). But merely using data isn't really what we mean by "data science." A data application acquires its value from the data itself, and creates more data as a result. It's not just an application with data; it's a data product. Data science enables the creation of data products. | Texto libre, puede contener varias líneas |
| rating_value | float64 | No | numérico | 3.68 | Debe ser un número válido<br>Número entre 0 y 5 |
| rating_count | int64 | No | numérico | 590 | Debe ser un número válido<br>Número entero ≥ 0 |
| price_amount | float64 | Sí | numérico | 0.0 | Debe ser un número válido<br>Número decimal con punto o coma |
| price_currency | object | Sí | string | EUR | Debe ser moneda ISO-4217 (ej. EUR, USD) |
| source_preference | object | No | string | goodreads | Debe ser 'goodreads' o 'google' |
| most_complete_url | object | No | URL | https://www.goodreads.com/book/show/13638556 | Debe ser una URL válida |
| ingestion_date_goodreads | object | No | string | 2025-11-22 17:33:15 | Formato ISO-8601<br>Admite YYYY, YYYY-MM o YYYY-MM-DD |
| ingestion_date_google | object | Sí | string | 2025-11-22 17:34:43 | Formato ISO-8601<br>Admite YYYY, YYYY-MM o YYYY-MM-DD |