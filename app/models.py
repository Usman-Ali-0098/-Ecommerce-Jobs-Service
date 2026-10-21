"""SQLAlchemy ORM mappings for Prisma-owned tables used by CSV imports.

Prisma migrations are the schema authority. These mappings never create tables.
"""
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, Text, func
from sqlalchemy.dialects.postgresql import ENUM, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()
JobTypeDb = ENUM("EMAIL", "PRODUCT_IMPORT", "KNOWLEDGE_INDEX", name="JobType", create_type=False)
JobStatusDb = ENUM("QUEUED", "PROCESSING", "COMPLETED", "FAILED", "PARTIAL", name="JobStatus", create_type=False)


class JobRunModel(Base):
    __tablename__ = "JobRun"
    id = Column(Text, primary_key=True)
    type = Column(JobTypeDb, nullable=False)
    status = Column(JobStatusDb, nullable=False)
    requested_by = Column("requestedBy", Integer)
    cloudinary_public_id = Column("cloudinaryPublicId", Text)
    cloudinary_secure_url = Column("cloudinarySecureUrl", Text)
    cloudinary_version = Column("cloudinaryVersion", Text)
    original_filename = Column("originalFilename", Text)
    size_bytes = Column("sizeBytes", Integer)
    idempotency_key = Column("idempotencyKey", Text)
    celery_task_id = Column("celeryTaskId", Text)
    phase = Column(Text)
    attempt = Column(Integer, nullable=False)
    total_rows = Column("totalRows", Integer, nullable=False)
    total_items = Column("totalItems", Integer, nullable=False)
    processed_items = Column("processedItems", Integer, nullable=False)
    success_count = Column("successCount", Integer, nullable=False)
    failed_count = Column("failedCount", Integer, nullable=False)
    enqueue_attempts = Column("enqueueAttempts", Integer, nullable=False)
    enqueue_error = Column("enqueueError", Text)
    enqueued_at = Column("enqueuedAt", DateTime(timezone=False))
    heartbeat_at = Column("heartbeatAt", DateTime(timezone=False))
    cleanup_at = Column("cleanupAt", DateTime(timezone=False))
    summary = Column(Text)
    error = Column(Text)
    created_at = Column("createdAt", DateTime(timezone=False), nullable=False, server_default=func.now())
    updated_at = Column("updatedAt", DateTime(timezone=False), nullable=False)
    started_at = Column("startedAt", DateTime(timezone=False))
    finished_at = Column("finishedAt", DateTime(timezone=False))


class JobRunItemModel(Base):
    __tablename__ = "JobRunItem"
    id = Column(Text, primary_key=True)
    job_run_id = Column("jobRunId", Text, nullable=False)
    item_key = Column("itemKey", Text)
    row_index = Column("rowIndex", Integer)
    chunk_index = Column("chunkIndex", Integer)
    product_name = Column("productName", Text)
    sku_summary = Column("skuSummary", Text)
    raw_row = Column("rawRow", JSONB)
    status = Column(JobStatusDb, nullable=False)
    message = Column(Text)
    product_id = Column("productId", Text)
    attempt = Column(Integer, nullable=False)
    created_at = Column("createdAt", DateTime(timezone=False), nullable=False, server_default=func.now())
    updated_at = Column("updatedAt", DateTime(timezone=False), nullable=False)


class ProductModel(Base):
    __tablename__ = "Product"
    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False)
    slug = Column(Text, nullable=False, unique=True)
    description = Column(Text)
    is_active = Column("isActive", Boolean, nullable=False)
    created_at = Column("createdAt", DateTime(timezone=False), nullable=False)
    updated_at = Column("updatedAt", DateTime(timezone=False), nullable=False)
    category_id = Column("categoryId", Text, ForeignKey("Category.id"), nullable=False)


class CategoryModel(Base):
    __tablename__ = "Category"
    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    slug = Column(Text, nullable=False, unique=True)
    is_active = Column("isActive", Boolean, nullable=False)
    created_at = Column("createdAt", DateTime(timezone=False), nullable=False)
    updated_at = Column("updatedAt", DateTime(timezone=False), nullable=False)


class ColorModel(Base):
    __tablename__ = "Color"
    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    hexacode = Column(Text)
    is_active = Column("isActive", Boolean, nullable=False)


class SizeModel(Base):
    __tablename__ = "Size"
    id = Column(Text, primary_key=True)
    name = Column(Text, nullable=False, unique=True)
    sort_order = Column("sortOrder", Integer, nullable=False)
    is_active = Column("isActive", Boolean, nullable=False)


class ProductImageModel(Base):
    __tablename__ = "ProductImage"
    id = Column(Text, primary_key=True)
    product_id = Column("productId", Text, ForeignKey("Product.id", ondelete="CASCADE"), nullable=False)
    url = Column(Text, nullable=False)
    alt_text = Column("altText", Text)
    position = Column(Integer, nullable=False)
    is_primary = Column("isPrimary", Boolean, nullable=False)
    public_id = Column("publicId", Text)
    color_id = Column("colorId", Text, ForeignKey("Color.id"))


class ProductVariantModel(Base):
    __tablename__ = "ProductVariant"
    id = Column(Text, primary_key=True)
    product_id = Column("productId", Text, ForeignKey("Product.id", ondelete="CASCADE"), nullable=False)
    color_id = Column("colorId", Text, ForeignKey("Color.id"))
    size_id = Column("sizeId", Text, ForeignKey("Size.id"))
    sku = Column(Text, nullable=False, unique=True)
    price = Column(Numeric(10, 2), nullable=False)
    stock = Column(Integer, nullable=False)
    is_active = Column("isActive", Boolean, nullable=False)
    created_at = Column("createdAt", DateTime(timezone=False), nullable=False)
    updated_at = Column("updatedAt", DateTime(timezone=False), nullable=False)
    image_public_id = Column("imagePublicId", Text)
    image_url = Column("imageUrl", Text)
