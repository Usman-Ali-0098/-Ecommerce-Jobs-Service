from datetime import datetime
from typing import Optional

from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, EmailStr
from sqlalchemy import select, text

from app.config import settings
from app.db import SessionLocal, engine
from app.job_store import create_job_run, create_job_run_item, mark_job_started
from app.tasks_email import send_email_task
from app.tasks_knowledge import STATIC_KNOWLEDGE, index_one_source, prune_stale_chunks
from app.tasks_orders import sweep_expired_orders
from app.tasks_bulk_upload import process_csv_import
from app.models import JobRunModel

app = FastAPI(title="Storefront Jobs Service")


def require_api_secret(authorization: Optional[str] = Header(default=None)):
    expected = f"Bearer {settings.jobs_api_secret}"
    if not authorization or authorization != expected:
        raise HTTPException(status_code=401, detail="Unauthorized.")


@app.get("/health")
def health():
    return {"status": "ok"}


class SendEmailRequest(BaseModel):
    to: EmailStr
    subject: str
    html: str
    requested_by: Optional[int] = None


@app.post("/jobs/email", dependencies=[Depends(require_api_secret)], status_code=202)
def create_email_job(payload: SendEmailRequest):
    """Next.js renders the email (it already owns the React Email templates)
    and hands over the final {to, subject, html} — this service only
    dispatches it. Returns immediately with a JobRun id Next.js can poll via
    Prisma (prisma.jobRun.findUnique)."""
    with engine.begin() as conn:
        job_id = create_job_run(conn, "EMAIL", requested_by=payload.requested_by)

    send_email_task.delay(job_id, payload.to, payload.subject, payload.html)

    return {"jobId": job_id}


class BulkUploadEnqueueRequest(BaseModel):
    jobRunId: str

# .........start 
@app.post("/jobs/bulk-upload", dependencies=[Depends(require_api_secret)], status_code=202)
def enqueue_bulk_upload(payload: BulkUploadEnqueueRequest):
    """Queue an existing CSV import JobRun; the Celery message contains only its ID."""
    queued_task_id = None
    response = None
    queue_error = None

    with SessionLocal() as session:
        with session.begin():
            job = session.scalar(
                select(JobRunModel).where(JobRunModel.id == payload.jobRunId)
            )
            if job is None:
                raise HTTPException(status_code=404, detail="Import job not found.")
            if job.type != "PRODUCT_IMPORT":
                raise HTTPException(status_code=409, detail="Job is not a product import.")
            if job.status in ("COMPLETED", "PARTIAL", "FAILED"):
                raise HTTPException(status_code=409, detail="Import job is already finished.")
            if job.status == "PROCESSING" or job.enqueued_at is not None:
                response = {"jobId": job.id, "taskId": job.celery_task_id, "status": job.status}
            elif not job.cloudinary_public_id or not job.cloudinary_secure_url:
                raise HTTPException(status_code=422, detail="Import job has no CSV asset attached.")
            else:
                try:
                    # asks Celery to enqueue the task
                    result = process_csv_import.apply_async(args=[job.id])
                    queued_task_id = result.id
                    job.celery_task_id = result.id
                    job.enqueued_at = datetime.utcnow()
                    job.enqueue_attempts += 1
                    job.enqueue_error = None
                    job.updated_at = datetime.utcnow()
                    response = {"jobId": job.id, "taskId": result.id, "status": "QUEUED"}
                except Exception as exc:
                    job.enqueue_attempts += 1
                    job.enqueue_error = str(exc)[:2000]
                    job.updated_at = datetime.utcnow()
                    queue_error = exc

    if queue_error is not None:
        raise HTTPException(status_code=503, detail="Unable to queue the import job.") from queue_error
    return response


class ReindexKnowledgeRequest(BaseModel):
    requested_by: Optional[int] = None


@app.post("/jobs/knowledge/reindex", dependencies=[Depends(require_api_secret)], status_code=202)
def create_knowledge_reindex_job(payload: ReindexKnowledgeRequest):
    """Rebuilds the RAG knowledge index (KnowledgeChunk): every active
    Product and Category, plus the hardcoded policy/FAQ entries in
    tasks_knowledge.py. Same per-item isolation pattern as product import —
    one JobRunItem + one Celery task per source, so a single slow/failed
    embedding call doesn't block the rest, and embedding calls run in
    parallel across workers. Every write is an upsert (delete-then-insert
    keyed by sourceType+sourceId in tasks_knowledge._upsert_chunk), so this
    is safe to re-run any time products/categories change until the
    on-change hook (Step 1's roadmap item) exists.

    Also prunes any KnowledgeChunk whose source is no longer active/current
    (see tasks_knowledge.prune_stale_chunks) -- a product deactivated since
    the last run doesn't just stop being re-embedded, its old chunk is
    actually removed so it can't keep surfacing in search results."""
    with engine.begin() as conn:
        product_ids = [
            row.id for row in conn.execute(text('SELECT id FROM "Product" WHERE "isActive" = true')).fetchall()
        ]
        category_ids = [
            row.id for row in conn.execute(text('SELECT id FROM "Category" WHERE "isActive" = true')).fetchall()
        ]

        pruned = prune_stale_chunks(conn, product_ids, category_ids)

        sources = (
            [("PRODUCT", pid) for pid in product_ids]
            + [("CATEGORY", cid) for cid in category_ids]
            + [(s["sourceType"], s["sourceId"]) for s in STATIC_KNOWLEDGE]
        )

        job_id = create_job_run(conn, "KNOWLEDGE_INDEX", requested_by=payload.requested_by)
        items = [
            (create_job_run_item(conn, job_id, row_index=index, status="QUEUED"), source_type, source_id)
            for index, (source_type, source_id) in enumerate(sources)
        ]
        mark_job_started(conn, job_id)

    for item_id, source_type, source_id in items:
        index_one_source.delay(job_id, item_id, source_type, source_id)

    return {"jobId": job_id, "sourceCount": len(items), "prunedStaleChunks": pruned}


@app.post("/sweep", dependencies=[Depends(require_api_secret)])
def trigger_sweep():
    """Manually enqueue an order-expiry sweep instead of waiting for the
    next Celery Beat tick — handy for testing without waiting out the
    120-hour window."""
    result = sweep_expired_orders.delay()
    return {"queued": True, "task_id": result.id}
