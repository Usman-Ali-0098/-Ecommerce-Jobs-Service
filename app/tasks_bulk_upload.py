"""Orchestrate CSV imports from durable JobRun records using SQLAlchemy ORM."""
import io
import logging
import ssl
import urllib.request
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.parse import urlparse

import certifi
import cloudinary
import cloudinary.uploader
from sqlalchemy import select

from app.bulk_csv import CsvImportError, iter_product_chunks
from app.celery_app import celery_app
from app.config import settings
from app.db import SessionLocal
from app.ids import new_id
from app.models import CategoryModel, ColorModel, JobRunItemModel, JobRunModel, SizeModel
from app.next_notify import notify_nextjs_if_finished
from app.product_import_orm import finalize_job_progress, import_product

logger = logging.getLogger(__name__)
IMAGE_FOLDER = "ecommerce/products"
# TESTING ONLY: lowered from 300 so a ~35-product test CSV produces multiple
# chunks instead of one. Set this back to 300 before any real/production run.
PRODUCTS_PER_CHUNK = 15
# Shared across an entire chunk (not spun up per-product), so a 300-product
# chunk's image uploads actually run with this much real concurrency instead
# of being serialized across products with only ~4-way parallelism each.
IMAGE_UPLOAD_WORKERS = 8


class _LimitedResponseReader(io.RawIOBase):
    """Expose an HTTP response as a bounded raw stream for TextIOWrapper."""
    def __init__(self, response, max_bytes: int):
        self.response = response
        self.max_bytes = max_bytes
        self.total = 0

    def readable(self):
        return True

    def readinto(self, buffer):
        remaining = self.max_bytes - self.total
        data = self.response.read(min(len(buffer), remaining + 1))
        if not data:
            return 0
        if len(data) > remaining:
            raise CsvImportError("CSV exceeds the configured maximum file size.")
        self.total += len(data)
        buffer[:len(data)] = data
        return len(data)

    def close(self):
        if not self.closed:
            self.response.close()
        super().close()


@contextmanager
def _download_csv(url: str):
    parsed = urlparse(url)
    cloud_name = settings.cloudinary_cloud_name
    if parsed.scheme != "https" or parsed.hostname != "res.cloudinary.com":
        raise CsvImportError("CSV asset URL is not a trusted Cloudinary HTTPS URL.")
    if not cloud_name or not parsed.path.startswith("/" + cloud_name + "/"):
        raise CsvImportError("CSV asset does not belong to the configured Cloudinary account.")
    # Downloads the CSV from Cloudinary.
    response = urllib.request.urlopen(
        url, timeout=30, context=ssl.create_default_context(cafile=certifi.where())
    )
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > settings.bulk_upload_max_csv_bytes:
        response.close()
        raise CsvImportError("CSV exceeds the configured maximum file size.")
    raw = _LimitedResponseReader(response, settings.bulk_upload_max_csv_bytes)
    text_stream = io.TextIOWrapper(io.BufferedReader(raw), encoding="utf-8-sig", newline="")
    try:
        yield text_stream
    finally:
        text_stream.close()


def _load_reference_maps() -> Dict[str, Dict[str, str]]:
    with SessionLocal() as session:
        categories = session.scalars(select(CategoryModel).where(CategoryModel.is_active.is_(True))).all()
        colors = session.scalars(select(ColorModel).where(ColorModel.is_active.is_(True))).all()
        sizes = session.scalars(select(SizeModel).where(SizeModel.is_active.is_(True))).all()
    return {
        "category": {row.name.strip().casefold(): row.id for row in categories},
        "color": {row.name.strip().casefold(): row.id for row in colors},
        "size": {row.name.strip().casefold(): row.id for row in sizes},
    }


def _image_index() -> Dict[str, List[Path]]:
    root = Path(settings.bulk_upload_images_dir).expanduser().resolve()
    if not root.is_dir():
        raise RuntimeError("Bulk upload image directory is not available to the worker.")
    matches: Dict[str, List[Path]] = {}
    for candidate in root.rglob("*"):
        if candidate.is_symlink() or not candidate.is_file():
            continue
        resolved = candidate.resolve()
        if resolved.is_relative_to(root):
            matches.setdefault(candidate.name.casefold(), []).append(resolved)
    return matches


def _resolve_image_path(filename: str, index: Dict[str, List[Path]]) -> Path:
    name = filename.strip()
    if not name or name in (".", "..") or "/" in name or "\\" in name:
        raise ValueError("Image references must be filenames without folder paths.")
    matches = index.get(name.casefold(), [])
    if not matches:
        raise ValueError("Image file not found in worker image folder: %s" % name)
    if len(matches) > 1:
        raise ValueError("Image filename is ambiguous in worker image folder: %s" % name)
    return matches[0]


def _upload_image(path: Path) -> Dict[str, str]:
    if not settings.cloudinary_cloud_name or not settings.cloudinary_api_key or not settings.cloudinary_api_secret:
        raise RuntimeError("Cloudinary credentials are not configured in the jobs service.")
    cloudinary.config(cloud_name=settings.cloudinary_cloud_name, api_key=settings.cloudinary_api_key,
                      api_secret=settings.cloudinary_api_secret, secure=True)
    result = cloudinary.uploader.upload(str(path), folder=IMAGE_FOLDER, resource_type="image",
                                        use_filename=True, unique_filename=True, overwrite=False)
    return {"url": result["secure_url"], "publicId": result["public_id"]}


def _validate_variant_fields(sku: str, price_raw, stock_raw) -> None:
    """Cheap, no-I/O field checks. Raised before any image upload is attempted,
    so a row with a bad price/stock/SKU never pays for a Cloudinary upload
    that import_product() would have rejected anyway a few lines later."""
    if not sku:
        raise ValueError("Every variant requires a SKU.")
    try:
        price = Decimal(str(price_raw))
        stock = int(stock_raw)
    except (TypeError, ValueError, InvalidOperation):
        raise ValueError("Invalid price or stock for SKU %s." % sku)
    if price <= 0 or price != price.to_integral_value():
        raise ValueError("Price must be a positive whole number for SKU %s." % sku)
    if stock < 0:
        raise ValueError("Stock cannot be negative for SKU %s." % sku)


def _validate_and_collect(product, refs) -> Tuple[str, List[dict], List[str]]:
    """Phase 1: validate a product with zero I/O and collect the image
    filenames it needs. Raises ValueError on any problem. Nothing here
    touches Cloudinary or disk -- that only happens in _resolve_chunk_images,
    after every product in the chunk has passed this cheap check."""
    category_name = str(product.get("categoryName") or "").strip()
    category_id = refs["category"].get(category_name.casefold())
    if not category_id:
        raise ValueError("Category does not exist or is inactive: %s" % (category_name or "(empty)"))
    raw_variants = product.get("variants") or []
    if not raw_variants:
        raise ValueError("At least one variant is required.")
    variants = []
    skus_seen = set()
    for row in raw_variants:
        color_name = str(row.get("colorName") or "").strip()
        size_name = str(row.get("sizeName") or "").strip()
        color_id = refs["color"].get(color_name.casefold()) if color_name else None
        size_id = refs["size"].get(size_name.casefold()) if size_name else None
        if color_name and not color_id:
            raise ValueError("Color does not exist or is inactive: %s" % color_name)
        if size_name and not size_id:
            raise ValueError("Size does not exist or is inactive: %s" % size_name)
        sku = str(row.get("sku") or "").strip().upper()
        _validate_variant_fields(sku, row.get("price"), row.get("stock"))
        if sku in skus_seen:
            raise ValueError("Duplicate SKU within this product: %s" % sku)
        skus_seen.add(sku)
        variants.append({
            "sku": row.get("sku", ""), "price": row.get("price", ""), "stock": row.get("stock", ""),
            "colorId": color_id, "sizeId": size_id, "imageUrl": None, "imagePublicId": None,
            "_imageFilename": str(row.get("imageFilename") or "").strip(),
        })
    filenames = []
    seen_lower = set()
    for variant in variants:
        name = variant["_imageFilename"]
        if name and name.casefold() not in seen_lower:
            seen_lower.add(name.casefold())
            filenames.append(name)
    return category_id, variants, filenames


def _resolve_chunk_images(filenames_by_product: Iterable[List[str]], image_files, cache: Dict[str, dict], pool: ThreadPoolExecutor) -> None:
    # """Phase 2: resolve + upload every distinct filename needed by this whole chunk together,
    all_names: Dict[str, str] = {}
    for filenames in filenames_by_product:
        for name in filenames:
            all_names.setdefault(name.casefold(), name)

    to_submit: Dict[str, Path] = {}
    for key, name in all_names.items():
        if key in cache:
            continue
        try:
            to_submit[key] = _resolve_image_path(name, image_files)
        except Exception as exc:
            cache[key] = {"ok": False, "error": str(exc)[:1500] or "Image file could not be resolved."}

    if not to_submit:
        return
    futures = {key: pool.submit(_upload_image, path) for key, path in to_submit.items()}
    for key, future in futures.items():
        try:
            result = future.result()
            cache[key] = {"ok": True, "url": result["url"], "publicId": result["publicId"]}
        except Exception as exc:
            cache[key] = {"ok": False, "error": str(exc)[:1500] or "Image upload failed."}


def _finalize_product(product, category_id: str, variants: List[dict], filenames: List[str], cache: Dict[str, dict]) -> dict:
    """Phase 3: build the final product payload from already-resolved images.
    Raises ValueError if any of this product's own images failed in phase 2,
    isolating that failure to this product without re-touching Cloudinary."""
    images = []
    for position, filename in enumerate(filenames):
        key = filename.casefold()
        entry = cache.get(key)
        if not entry or not entry.get("ok"):
            raise ValueError(entry["error"] if entry else "Image file not found: %s" % filename)
        color_ids = {variant["colorId"] for variant in variants
                     if variant["_imageFilename"].casefold() == key and variant["colorId"]}
        images.append({"url": entry["url"], "publicId": entry["publicId"],
                       "colorId": next(iter(color_ids)) if len(color_ids) == 1 else None,
                       "position": position, "isPrimary": position == 0})
    clean_variants = [{k: v for k, v in variant.items() if k != "_imageFilename"} for variant in variants]
    return {"name": product.get("name", ""), "description": product.get("description", ""),
            "categoryId": category_id, "isActive": True, "images": images, "variants": clean_variants}


def _fail_job(job_id: str, message: str):
    now = datetime.utcnow()
    with SessionLocal.begin() as session:
        job = session.get(JobRunModel, job_id)
        if not job or job.status != "PROCESSING":
            return
        for item in session.scalars(select(JobRunItemModel).where(
            JobRunItemModel.job_run_id == job_id, JobRunItemModel.status == "QUEUED"
        )):
            item.status = "FAILED"
            item.message = "Import could not be queued: " + message[:1500]
            item.updated_at = now
        job.error = message[:2000]
        finalize_job_progress(session, job_id)
        # A fatal stream/queue error must not be mistaken for a completed import
        # just because every item from earlier committed chunks succeeded.
        job = session.get(JobRunModel, job_id)
        if job:
            job.status = "PARTIAL" if job.success_count else "FAILED"
            job.phase = job.status
            if job.success_count:
                job.summary = "%s products succeeded before the import stopped." % job.success_count
            job.error = message[:2000]
            job.finished_at = now
            job.updated_at = now
    notify_nextjs_if_finished(job_id)







#........start
@celery_app.task(bind=True, name="app.tasks_bulk_upload.process_csv_import")
def process_csv_import(self, job_run_id: str):
    """Stream products in chunks of PRODUCTS_PER_CHUNK and commit each chunk atomically."""
    with SessionLocal.begin() as session:
        job = session.scalar(select(JobRunModel).where(
            JobRunModel.id == job_run_id, JobRunModel.type == "PRODUCT_IMPORT", JobRunModel.status == "QUEUED"
        ).with_for_update(skip_locked=True))
        if not job:
            logger.info("CSV import %s was already claimed or is not queued", job_run_id)
            return {"jobId": job_run_id, "skipped": True}
        job.status = "PROCESSING"
        job.started_at = job.started_at or datetime.utcnow()
        job.heartbeat_at = datetime.utcnow()
        job.attempt += 1
        job.celery_task_id = self.request.id
        job.phase = "PARSING"
        job.updated_at = datetime.utcnow()
        csv_url = job.cloudinary_secure_url

    try:
        if not csv_url:
            raise CsvImportError("Import job has no CSV asset URL.")
        refs = _load_reference_maps()
        image_files = None
        image_cache: Dict[str, dict] = {}
        total_products = total_rows = 0
        chunk_index = 0
        with ThreadPoolExecutor(max_workers=IMAGE_UPLOAD_WORKERS) as image_pool, _download_csv(csv_url) as text_stream:
            for products, chunk_rows in iter_product_chunks(
                text_stream, max_rows=settings.bulk_upload_max_rows, chunk_size=PRODUCTS_PER_CHUNK
            ):
                if image_files is None:
                    has_images = any(
                        str(variant.get("imageFilename") or "").strip()
                        for product in products for variant in product.get("variants", [])
                    )
                    if has_images:
                        image_files = _image_index()

                # Phase 1: validate every row cheaply (no I/O).
                validated: Dict[int, Tuple[str, List[dict], List[str]]] = {}
                failures: Dict[int, str] = {}
                for index, product in enumerate(products):
                    try:
                        validated[index] = _validate_and_collect(product, refs)
                    except Exception as exc:
                        logger.exception("Could not validate CSV product %s", product.get("name"))
                        failures[index] = str(exc)[:1500] or "Product validation failed."

                # Phase 2: resolve + upload this chunk's distinct image
                _resolve_chunk_images(
                    (filenames for _, _, filenames in validated.values()),
                    image_files or {}, image_cache, image_pool,
                )

                # Phase 3: build each still-valid product's final payload from
                prepared = []
                for index, (category_id, variants, filenames) in validated.items():
                    try:
                        prepared.append((index, _finalize_product(products[index], category_id, variants, filenames, image_cache)))
                    except Exception as exc:
                        logger.exception("Could not finalize CSV product %s", products[index].get("name"))
                        failures[index] = str(exc)[:1500] or "Image resolution failed."

                now = datetime.utcnow()
                with SessionLocal.begin() as session:
                    job = session.get(JobRunModel, job_run_id)
                    if not job or job.status != "PROCESSING":
                        raise RuntimeError("Import job is no longer processing.")
                    by_index = {index: payload for index, payload in prepared}
                    chunk_success = 0
                    chunk_failed = 0
                    for index, product in enumerate(products):
                        variants = product.get("variants") or []
                        item = JobRunItemModel(
                            id=new_id(), job_run_id=job_run_id, item_key=str(total_products + index),
                            row_index=int(product.get("firstRowIndex", 0)), chunk_index=chunk_index,
                            product_name=str(product.get("name", ""))[:255],
                            sku_summary=", ".join(str(v.get("sku", "")).strip() for v in variants if v.get("sku"))[:500],
                            status="QUEUED", message=None, attempt=0, created_at=now, updated_at=now,
                        )
                        session.add(item)
                        if index in failures:
                            item.status = "FAILED"
                            item.message = failures[index]
                            item.raw_row = product
                            item.updated_at = datetime.utcnow()
                            chunk_failed += 1
                            continue
                        try:
                            # Savepoints isolate a bad row; the surrounding transaction
                            # remains the one atomic commit for this 300-product chunk.
                            with session.begin_nested():
                                product_id, action = import_product(session, by_index[index])
                            item.status = "COMPLETED"
                            item.message = '%s "%s"' % (action, product.get("name", ""))
                            item.product_id = product_id
                            chunk_success += 1
                        except Exception as exc:
                            logger.exception("Database import failed for CSV product %s", product.get("name"))
                            item.status = "FAILED"
                            item.message = str(exc)[:1500] or "Product database write failed."
                            item.raw_row = product
                            chunk_failed += 1
                        item.updated_at = datetime.utcnow()
                    # Incremental counters from this chunk's own results --
            
                    job.total_rows += chunk_rows
                    job.total_items += len(products)
                    job.processed_items += chunk_success + chunk_failed
                    job.success_count += chunk_success
                    job.failed_count += chunk_failed
                    job.phase = "IMPORTING"
                    job.heartbeat_at = datetime.utcnow()
                    job.updated_at = datetime.utcnow()
                total_products += len(products)
                total_rows += chunk_rows
                chunk_index += 1
                logger.info("Committed CSV import chunk: %d products (%d total)", len(products), total_products)

        with SessionLocal.begin() as session:
            finalize_job_progress(session, job_run_id)
        notify_nextjs_if_finished(job_run_id)
        return {"jobId": job_run_id, "products": total_products, "rows": total_rows, "chunkSize": PRODUCTS_PER_CHUNK}
    except Exception as exc:
        logger.exception("CSV import orchestration failed for job %s", job_run_id)
        _fail_job(job_run_id, str(exc))
        return {"jobId": job_run_id, "failed": True}
