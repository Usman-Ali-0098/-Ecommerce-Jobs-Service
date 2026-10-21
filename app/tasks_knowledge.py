import json
import logging
import math
import time
from typing import Optional

from google import genai
from google.genai import errors as genai_errors
from google.genai import types
from sqlalchemy import text

from app.celery_app import celery_app
from app.config import settings
from app.db import engine
from app.ids import new_id
from app.job_store import finalize_job_from_items, mark_item_result

logger = logging.getLogger(__name__)

EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_DIM = 768

_client = genai.Client(api_key=settings.gemini_api_key)

# Static policy/FAQ content -- there's no admin-editable table for these yet
# (that's a later phase in the chatbot plan). Edit this list directly for
# now. sourceId is stable across runs, so re-running the reindex job
# upserts these rather than duplicating them.
STATIC_KNOWLEDGE = [
    {
        "sourceId": "policy-shipping",
        "sourceType": "POLICY",
        "content": (
            "Shipping policy: Orders are processed within 1-2 business days. "
            "Standard delivery takes 3-7 business days depending on location. "
            "Shipping costs are calculated at checkout based on order weight and destination."
        ),
        "metadata": {"policyType": "shipping"},
    },
    {
        "sourceId": "policy-returns",
        "sourceType": "POLICY",
        "content": (
            "Return policy: Items can be returned within 14 days of delivery if unused and in "
            "original packaging. Refunds are issued to the original payment method within 5-10 "
            "business days after the returned item is received and inspected."
        ),
        "metadata": {"policyType": "returns"},
    },
    {
        "sourceId": "faq-payment-methods",
        "sourceType": "FAQ",
        "content": (
            "What payment methods are accepted? We accept credit/debit cards via Stripe and "
            "cash on delivery for eligible locations."
        ),
        "metadata": {"topic": "payment"},
    },
    {
        "sourceId": "internal-payment-dispute-escalation",
        "sourceType": "POLICY",
        "visibility": "INTERNAL",
        "content": (
            "Internal escalation policy: for payment disputes or chargeback claims over Rs. "
            "50,000, escalate to the finance team lead before processing any refund. Do not "
            "issue a refund above that threshold from a support conversation alone. Internal "
            "contact for escalations: finance-lead@budgetvibe.internal (not customer-facing)."
        ),
        "metadata": {"policyType": "internal-escalation"},
    },
]


def _normalize(values: list[float]) -> list[float]:
    """gemini-embedding-001 only guarantees unit length at its full 3072-dim
    output. We ask for a truncated 768-dim vector (see EMBEDDING_DIM), so it
    needs re-normalizing here or pgvector's cosine-distance operator (<=>)
    won't rank results correctly."""
    norm = math.sqrt(sum(v * v for v in values))
    if norm == 0:
        return values
    return [v / norm for v in values]


def embed_text(text_value: str, task_type: str = "RETRIEVAL_DOCUMENT") -> list[float]:
    """task_type matters for Gemini's embedding model: content going INTO the
    index is embedded as RETRIEVAL_DOCUMENT, while a user's search query
    (Step 3) must be embedded as RETRIEVAL_QUERY -- these produce vectors
    optimized for being matched against each other, not identical text
    embedded the same way twice."""
    #
    # (bad input, auth, etc.) is raised immediately, unretried.
    delays = [2, 5, 15, 30]
    for attempt, delay in enumerate([0, *delays]):
        if delay:
            time.sleep(delay)
        try:
            result = _client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=text_value,
                config=types.EmbedContentConfig(
                    output_dimensionality=EMBEDDING_DIM,
                    task_type=task_type,
                ),
            )
            return _normalize(result.embeddings[0].values)
        except genai_errors.ClientError as exc:
            if exc.code != 429 or attempt == len(delays):
                raise
            logger.warning("embed_text: rate limited (attempt %s), retrying in %ss", attempt + 1, delays[attempt])


def _vector_literal(values: list[float]) -> str:
    """pgvector accepts this bracketed-CSV text form cast to ::vector --
    there's no native Python binding type for it via plain SQLAlchemy Core."""
    return "[" + ",".join(f"{v:.8f}" for v in values) + "]"


def _upsert_chunk(
    conn,
    source_type: str,
    source_id: str,
    content: str,
    metadata: dict,
    visibility: str,
    embedding: list[float],
) -> None:
    conn.execute(
        text('DELETE FROM "KnowledgeChunk" WHERE "sourceType" = :source_type AND "sourceId" = :source_id'),
        {"source_type": source_type, "source_id": source_id},
    )
    conn.execute(
        text(
            """
            INSERT INTO "KnowledgeChunk"
                (id, "sourceType", "sourceId", content, metadata, visibility, embedding, "createdAt", "updatedAt")
            VALUES
                (:id, :source_type, :source_id, :content, CAST(:metadata AS JSONB), :visibility,
                 CAST(:embedding AS vector), now(), now())
            """
        ),
        {
            "id": new_id(),
            "source_type": source_type,
            "source_id": source_id,
            "content": content,
            "metadata": json.dumps(metadata),
            "visibility": visibility,
            "embedding": _vector_literal(embedding),
        },
    )


def prune_stale_chunks(conn, active_product_ids: list[str], active_category_ids: list[str]) -> int:
    """Deletes KnowledgeChunk rows whose source is no longer active/current --
    a product deactivated since the last reindex, a category removed, or a
    STATIC_KNOWLEDGE entry that was since deleted from this file. Without
    this, a stale chunk keeps surfacing in search results forever, since the
    normal upsert path only ever touches sources it's told to re-index, it
    never notices one that disappeared. This is the gap flagged (and
    deliberately deferred) back when the ingestion job was first built.

    Called once per reindex run, before dispatching per-item tasks, using
    exactly the same active-id lists the caller (main.py) already computed
    for dispatching -- so "stale" here means "not in this run's source set."
    """
    static_ids = [entry["sourceId"] for entry in STATIC_KNOWLEDGE]

    result = conn.execute(
        text(
            """
            DELETE FROM "KnowledgeChunk"
            WHERE ("sourceType" = 'PRODUCT' AND NOT ("sourceId" = ANY(:product_ids)))
               OR ("sourceType" = 'CATEGORY' AND NOT ("sourceId" = ANY(:category_ids)))
               OR ("sourceType" IN ('POLICY', 'FAQ') AND NOT ("sourceId" = ANY(:static_ids)))
            """
        ),
        {
            "product_ids": active_product_ids,
            "category_ids": active_category_ids,
            "static_ids": static_ids,
        },
    )
    return result.rowcount


def _load_product_content(conn, product_id: str) -> Optional[tuple[str, dict]]:
    row = conn.execute(
        text(
            """
            SELECT p.name, p.slug, p.description, p."categoryId",
                   c.name AS category_name, c.slug AS category_slug,
                   MIN(v.price) AS min_price, MAX(v.price) AS max_price,
                   COALESCE(SUM(v.stock), 0) AS total_stock
            FROM "Product" p
            JOIN "Category" c ON c.id = p."categoryId"
            LEFT JOIN "ProductVariant" v ON v."productId" = p.id AND v."isActive" = true
            WHERE p.id = :id AND p."isActive" = true
            GROUP BY p.id, p.name, p.slug, p.description, p."categoryId", c.name, c.slug
            """
        ),
        {"id": product_id},
    ).fetchone()
    if not row:
        return None

    colors = [
        r.name
        for r in conn.execute(
            text(
                """
                SELECT DISTINCT co.name
                FROM "ProductVariant" v
                JOIN "Color" co ON co.id = v."colorId"
                WHERE v."productId" = :id AND v."isActive" = true AND co."isActive" = true
                ORDER BY co.name
                """
            ),
            {"id": product_id},
        ).fetchall()
    ]
    sizes = [
        r.name
        for r in conn.execute(
            text(
                """
                SELECT DISTINCT s.name, s."sortOrder"
                FROM "ProductVariant" v
                JOIN "Size" s ON s.id = v."sizeId"
                WHERE v."productId" = :id AND v."isActive" = true AND s."isActive" = true
                ORDER BY s."sortOrder"
                """
            ),
            {"id": product_id},
        ).fetchall()
    ]
    primary_image = conn.execute(
        text(
            """
            SELECT url FROM "ProductImage"
            WHERE "productId" = :id
            ORDER BY "isPrimary" DESC, position ASC
            LIMIT 1
            """
        ),
        {"id": product_id},
    ).fetchone()

    parts = [f"{row.name} (category: {row.category_name})."]
    if row.description:
        parts.append(row.description.strip())
    if row.min_price is not None:
        parts.append(
            f"Price: {row.min_price}."
            if row.min_price == row.max_price
            else f"Price range: {row.min_price} - {row.max_price}."
        )
    if colors:
        parts.append(f"Available colors: {', '.join(colors)}.")
    if sizes:
        parts.append(f"Available sizes: {', '.join(sizes)}.")
    content = " ".join(parts)

    metadata = {
        "productId": product_id,
        "slug": row.slug,
        "categoryId": row.categoryId,
        "category": row.category_name,
        "categorySlug": row.category_slug,
        "priceMin": float(row.min_price) if row.min_price is not None else None,
        "priceMax": float(row.max_price) if row.max_price is not None else None,
        "inStock": bool(row.total_stock and row.total_stock > 0),
        "colors": colors,
        "sizes": sizes,
        "primaryImageUrl": primary_image.url if primary_image else None,
    }
    return content, metadata


def _load_category_content(conn, category_id: str) -> Optional[tuple[str, dict]]:
    row = conn.execute(
        text('SELECT name, slug FROM "Category" WHERE id = :id AND "isActive" = true'),
        {"id": category_id},
    ).fetchone()
    if not row:
        return None
    content = f"Category: {row.name}. Browse our {row.name} collection."
    metadata = {"categoryId": category_id, "category": row.name, "slug": row.slug}
    return content, metadata


@celery_app.task(name="app.tasks_knowledge.index_one_source")
def index_one_source(job_run_id: str, item_id: str, source_type: str, source_id: str):
    """One source (product / category / static policy-FAQ entry) per task --
    isolates a slow embedding call or a bad row from the rest of the batch,
    same reasoning as tasks_products.import_one_product. No retry: a
    validation/embedding failure is reported on the JobRunItem and the whole
    reindex can just be re-run afterward, since every write here is an
    upsert (delete-then-insert), not additive."""
    try:
        try:
            visibility = "PUBLIC"
            with engine.begin() as conn:
                if source_type == "PRODUCT":
                    loaded = _load_product_content(conn, source_id)
                elif source_type == "CATEGORY":
                    loaded = _load_category_content(conn, source_id)
                else:
                    static = next((s for s in STATIC_KNOWLEDGE if s["sourceId"] == source_id), None)
                    loaded = (static["content"], static["metadata"]) if static else None
                    if static:
                        visibility = static.get("visibility", "PUBLIC")

                if loaded is None:
                    mark_item_result(conn, item_id, "COMPLETED", message="Skipped (inactive or not found).")
                    return

                content, metadata = loaded

            embedding = embed_text(content)

            with engine.begin() as conn:
                _upsert_chunk(conn, source_type, source_id, content, metadata, visibility, embedding)
                mark_item_result(
                    conn,
                    item_id,
                    "COMPLETED",
                    message=f"Indexed {source_type} {source_id}",
                    product_id=source_id if source_type == "PRODUCT" else None,
                )
        except Exception as exc:  # noqa: BLE001 - embedding/DB error, report and move on
            logger.exception("index_one_source: failed for %s %s", source_type, source_id)
            with engine.begin() as conn:
                mark_item_result(conn, item_id, "FAILED", message=str(exc)[:500])
    finally:
        with engine.begin() as conn:
            finalize_job_from_items(conn, job_run_id)
