"""Streaming CSV row parser that yields bounded batches of complete products."""
import csv
from typing import Dict, Iterator, List, Optional, TextIO, Tuple

REQUIRED_HEADERS = ("productName", "category", "color", "size", "sku", "price", "stock")
OPTIONAL_HEADERS = ("description", "image")
DEFAULT_MAX_ROWS = 10_000
PRODUCTS_PER_CHUNK = 300
MAX_FIELD_SIZE = 1_000_000


class CsvImportError(ValueError):
    """The uploaded CSV cannot be processed as a product import."""


def _clean(value: Optional[str]) -> str:
    return (value or "").strip()


def iter_product_chunks(
    stream: TextIO,
    max_rows: int = DEFAULT_MAX_ROWS,
    chunk_size: int = PRODUCTS_PER_CHUNK,
) -> Iterator[Tuple[List[Dict[str, object]], int]]:
    """Read rows incrementally and yield (complete products, source rows) per chunk.

    Rows belonging to a product must be adjacent and share the same productName.
    This lets the worker finish one product before deciding whether the 300-product
    chunk is complete, without retaining the entire CSV in memory.
    """
    if max_rows < 1 or chunk_size < 1:
        raise ValueError("max_rows and chunk_size must be positive")
    csv.field_size_limit(MAX_FIELD_SIZE)
    reader = csv.reader(stream, strict=True)
    try:
        header = next(reader, None)
    except csv.Error as exc:
        raise CsvImportError("Unable to read the CSV header.") from exc
    if not header:
        raise CsvImportError("The CSV file is empty.")
    normalized = [cell.strip().lstrip("\ufeff") for cell in header]
    if len(set(normalized)) != len(normalized):
        raise CsvImportError("The CSV contains duplicate column names.")
    missing = [name for name in REQUIRED_HEADERS if name not in normalized]
    if missing:
        raise CsvImportError(
            "Missing column(s): " + ", ".join(missing)
            + ". Required columns: " + ", ".join(REQUIRED_HEADERS) + "."
        )
    indexes = {name: normalized.index(name) for name in normalized}
    chunk: List[Dict[str, object]] = []
    current = None
    seen_names = set()
    chunk_rows = 0
    row_index = 0

    def close_product():
        nonlocal current
        if current is not None:
            chunk.append(current)
            current = None

    try:
        for cells in reader:
            if not any(_clean(cell) for cell in cells):
                continue
            if row_index >= max_rows:
                raise CsvImportError("The CSV exceeds the maximum of %d data rows." % max_rows)
            if len(cells) > len(normalized):
                raise CsvImportError("CSV data row %d has more values than the header." % (row_index + 2))
            values = {name: _clean(cells[index] if index < len(cells) else "") for name, index in indexes.items()}
            product_name = values.get("productName", "")
            if not product_name:
                raise CsvImportError("Product name is required on CSV row %d." % (row_index + 2))
            if current is None or current["name"] != product_name:
                close_product()
                if len(chunk) == chunk_size:
                    yield chunk, chunk_rows
                    chunk, chunk_rows = [], 0
                if product_name in seen_names:
                    raise CsvImportError(
                        "Rows for product '%s' must be adjacent in the CSV." % product_name
                    )
                seen_names.add(product_name)
                current = {
                    "name": product_name,
                    "description": values.get("description", ""),
                    "categoryName": values.get("category", ""),
                    "firstRowIndex": row_index,
                    "variants": [],
                }
            current["variants"].append({
                "colorName": values.get("color", ""),
                "sizeName": values.get("size", ""),
                "sku": values.get("sku", ""),
                "price": values.get("price", ""),
                "stock": values.get("stock", ""),
                "imageFilename": values.get("image", ""),
                "sourceRowIndex": row_index,
            })
            row_index += 1
            chunk_rows += 1
        close_product()
    except csv.Error as exc:
        raise CsvImportError("The CSV contains malformed quoting or row data.") from exc
    if row_index == 0:
        raise CsvImportError("The CSV has a header but no product rows.")
    if chunk:
        yield chunk, chunk_rows
