"""ORM helpers for product writes and JobRun progress."""
import re
from decimal import Decimal, InvalidOperation
from datetime import datetime

from sqlalchemy import func, select

from app.ids import new_id
from app.models import (
    CategoryModel, ColorModel, JobRunItemModel, JobRunModel,
    ProductImageModel, ProductModel, ProductVariantModel, SizeModel,
)


class ImportValidationError(ValueError):
    pass


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-") or "product"


def _unique_slug(session, name: str) -> str:
    base = _slugify(name)
    slug = base
    suffix = 2
    while session.scalar(select(ProductModel.id).where(ProductModel.slug == slug)) is not None:
        slug = "%s-%s" % (base, suffix)
        suffix += 1
    return slug


def _validated_values(session, product):
    name = str(product.get("name") or "").strip()
    if not name:
        raise ImportValidationError("Product name is required.")
    category_id = product.get("categoryId")
    category = session.get(CategoryModel, category_id) if category_id else None
    if not category or not category.is_active:
        raise ImportValidationError("Selected category does not exist or is inactive.")
    variants = product.get("variants") or []
    if not variants:
        raise ImportValidationError("At least one variant is required.")
    skus = [str(v.get("sku") or "").strip().upper() for v in variants]
    if any(not sku for sku in skus):
        raise ImportValidationError("Every variant requires a SKU.")
    if len(set(skus)) != len(skus):
        raise ImportValidationError("Duplicate SKU within this product.")
    color_ids = {v.get("colorId") for v in variants if v.get("colorId")}
    size_ids = {v.get("sizeId") for v in variants if v.get("sizeId")}
    if color_ids:
        valid = set(session.scalars(select(ColorModel.id).where(ColorModel.id.in_(color_ids), ColorModel.is_active.is_(True))).all())
        if valid != color_ids:
            raise ImportValidationError("One or more colors are invalid or inactive.")
    if size_ids:
        valid = set(session.scalars(select(SizeModel.id).where(SizeModel.id.in_(size_ids), SizeModel.is_active.is_(True))).all())
        if valid != size_ids:
            raise ImportValidationError("One or more sizes are invalid or inactive.")
    images = product.get("images") or []
    if images and sum(1 for image in images if image.get("isPrimary")) != 1:
        raise ImportValidationError("Exactly one primary image is required when images are provided.")
    validated = []
    for variant, sku in zip(variants, skus):
        try:
            price = Decimal(str(variant["price"]))
            stock = int(variant["stock"])
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise ImportValidationError("Invalid price or stock for SKU %s." % sku)
        if price <= 0 or price != price.to_integral_value():
            raise ImportValidationError("Price must be a positive whole number for SKU %s." % sku)
        if stock < 0:
            raise ImportValidationError("Stock cannot be negative for SKU %s." % sku)
        validated.append((variant, sku, price, stock))
    return name, category_id, validated, images


def import_product(session, product):
    name, category_id, variants, images = _validated_values(session, product)
    skus = [sku for _, sku, _, _ in variants]
    existing = session.scalars(select(ProductVariantModel).where(ProductVariantModel.sku.in_(skus))).all()
    product_ids = {variant.product_id for variant in existing}
    if len(product_ids) > 1:
        raise ImportValidationError("These SKUs belong to more than one existing product.")
    now = datetime.utcnow()
    if product_ids:
        product_id = next(iter(product_ids))
        target = session.get(ProductModel, product_id)
        if not target:
            raise ImportValidationError("Existing product for these SKUs was not found.")
        existing_by_sku = {row.sku: row for row in existing}
        target.name = name
        target.description = str(product.get("description") or "").strip() or None
        target.category_id = category_id
        target.is_active = bool(product.get("isActive", True))
        target.updated_at = now
        existing_images = session.scalars(
            select(ProductImageModel)
            .where(ProductImageModel.product_id == product_id)
            .order_by(ProductImageModel.position, ProductImageModel.id)
        ).all()
        for index, image in enumerate(images):
            if index < len(existing_images):
                current_image = existing_images[index]
                current_image.url = image["url"]
                current_image.alt_text = name
                current_image.position = index
                current_image.is_primary = bool(image.get("isPrimary"))
                current_image.public_id = image.get("publicId")
                current_image.color_id = image.get("colorId")
            else:
                session.add(ProductImageModel(
                    id=new_id(), product_id=product_id, url=image["url"], alt_text=name,
                    position=index, is_primary=bool(image.get("isPrimary")),
                    public_id=image.get("publicId"), color_id=image.get("colorId"),
                ))
        for stale_image in existing_images[len(images):]:
            session.delete(stale_image)
        for variant, sku, price, stock in variants:
            current = existing_by_sku.get(sku)
            if current:
                current.color_id = variant.get("colorId")
                current.size_id = variant.get("sizeId")
                current.price = price
                current.stock = stock
                current.image_url = variant.get("imageUrl") or current.image_url
                current.image_public_id = variant.get("imagePublicId") or current.image_public_id
                current.updated_at = now
            else:
                session.add(ProductVariantModel(
                    id=new_id(), product_id=product_id, color_id=variant.get("colorId"),
                    size_id=variant.get("sizeId"), sku=sku, price=price, stock=stock,
                    is_active=True, created_at=now, updated_at=now,
                    image_url=variant.get("imageUrl"), image_public_id=variant.get("imagePublicId"),
                ))
        session.flush()
        return product_id, "Updated"

    product_id = new_id()
    session.add(ProductModel(
        id=product_id, name=name, slug=_unique_slug(session, name),
        description=str(product.get("description") or "").strip() or None,
        is_active=bool(product.get("isActive", True)), category_id=category_id,
        created_at=now, updated_at=now,
    ))
    # Persist the parent row before inserting child rows with productId FKs.
    session.flush()
    for index, image in enumerate(images):
        session.add(ProductImageModel(
            id=new_id(), product_id=product_id, url=image["url"], alt_text=name,
            position=image.get("position", index), is_primary=bool(image.get("isPrimary")),
            public_id=image.get("publicId"), color_id=image.get("colorId"),
        ))
    for variant, sku, price, stock in variants:
        session.add(ProductVariantModel(
            id=new_id(), product_id=product_id, color_id=variant.get("colorId"),
            size_id=variant.get("sizeId"), sku=sku, price=price, stock=stock,
            is_active=True, created_at=now, updated_at=now,
            image_url=variant.get("imageUrl"), image_public_id=variant.get("imagePublicId"),
        ))
    session.flush()
    return product_id, "Created"


def finalize_job_progress(session, job_run_id: str):
    # Sessions disable autoflush; persist item status changes before counting them.
    session.flush()
    rows = session.execute(
        select(JobRunItemModel.status, func.count())
        .where(JobRunItemModel.job_run_id == job_run_id)
        .group_by(JobRunItemModel.status)
    ).all()
    counts = {status: total for status, total in rows}
    total = sum(counts.values())
    completed = counts.get("COMPLETED", 0)
    failed = counts.get("FAILED", 0)
    pending = counts.get("QUEUED", 0) + counts.get("PROCESSING", 0)
    job = session.get(JobRunModel, job_run_id)
    if not job:
        return
    job.total_items = total
    job.processed_items = completed + failed
    job.success_count = completed
    job.failed_count = failed
    job.heartbeat_at = datetime.utcnow()
    job.updated_at = datetime.utcnow()
    if pending:
        return
    if total == 0:
        job.status = "FAILED"
        job.error = job.error or "Import contained no product items."
        job.phase = "FAILED"
    else:
        job.status = "COMPLETED" if failed == 0 else "FAILED" if completed == 0 else "PARTIAL"
        job.summary = "%s/%s products succeeded." % (completed, total)
        job.phase = job.status
    job.finished_at = datetime.utcnow()
