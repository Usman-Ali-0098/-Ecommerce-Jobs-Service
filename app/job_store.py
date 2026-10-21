"""Raw-SQL writes against the JobRun / JobRunItem tables Prisma owns (see
prisma/migrations/20260911180000_add_job_run in the Next.js repo). Next.js
reads these directly via Prisma to show job status/results — this module is
the only thing on the Python side that writes to them."""

from typing import Optional

from sqlalchemy import text

from app.ids import new_id


def create_job_run(conn, job_type: str, requested_by: Optional[int] = None) -> str:
    job_id = new_id()
    conn.execute(
        text(
            """
            INSERT INTO "JobRun" (id, type, status, "requestedBy", "createdAt", "updatedAt")
            VALUES (:id, :type, 'QUEUED', :requested_by, now(), now())
            """
        ),
        {"id": job_id, "type": job_type, "requested_by": requested_by},
    )
    return job_id


def mark_job_started(conn, job_id: str) -> None:
    conn.execute(
        text(
            """
            UPDATE "JobRun"
            SET status = 'PROCESSING', "startedAt" = now(), "updatedAt" = now()
            WHERE id = :id
            """
        ),
        {"id": job_id},
    )


def mark_job_completed(conn, job_id: str, summary: Optional[str] = None) -> None:
    conn.execute(
        text(
            """
            UPDATE "JobRun"
            SET status = 'COMPLETED', summary = :summary, "finishedAt" = now(), "updatedAt" = now()
            WHERE id = :id
            """
        ),
        {"id": job_id, "summary": summary},
    )


def mark_job_partial(conn, job_id: str, summary: Optional[str] = None) -> None:
    conn.execute(
        text(
            """
            UPDATE "JobRun"
            SET status = 'PARTIAL', summary = :summary, "finishedAt" = now(), "updatedAt" = now()
            WHERE id = :id
            """
        ),
        {"id": job_id, "summary": summary},
    )


def mark_job_failed(conn, job_id: str, error: str) -> None:
    conn.execute(
        text(
            """
            UPDATE "JobRun"
            SET status = 'FAILED', error = :error, "finishedAt" = now(), "updatedAt" = now()
            WHERE id = :id
            """
        ),
        {"id": job_id, "error": error[:2000]},
    )


def mark_item_result(
    conn,
    item_id: str,
    status: str,
    message: Optional[str] = None,
    product_id: Optional[str] = None,
) -> None:
    conn.execute(
        text(
            """
            UPDATE "JobRunItem"
            SET status = :status, message = :message, "productId" = :product_id, "updatedAt" = now()
            WHERE id = :id
            """
        ),
        {"id": item_id, "status": status, "message": message, "product_id": product_id},
    )


def finalize_job_from_items(conn, job_run_id: str) -> None:
    """Generic: once every JobRunItem under a job is out of QUEUED/PROCESSING,
    rolls the job's own status up from its items' outcomes. Idempotent — the
    WHERE guard means only the caller that observes the last item finish
    actually flips the row, safe to call from every item-completion task."""
    rows = conn.execute(
        text('SELECT status, COUNT(*) AS n FROM "JobRunItem" WHERE "jobRunId" = :id GROUP BY status'),
        {"id": job_run_id},
    ).fetchall()
    counts = {row.status: row.n for row in rows}
    pending = counts.get("QUEUED", 0) + counts.get("PROCESSING", 0)
    if pending > 0:
        return

    completed = counts.get("COMPLETED", 0)
    failed = counts.get("FAILED", 0)
    total = completed + failed

    if total == 0:
        return
    elif failed == 0:
        status = "COMPLETED"
    elif completed == 0:
        status = "FAILED"
    else:
        status = "PARTIAL"

    summary = f"{completed}/{total} succeeded."
    conn.execute(
        text(
            """
            UPDATE "JobRun"
            SET status = :status, summary = :summary, "finishedAt" = now(), "updatedAt" = now()
            WHERE id = :id AND status IN ('QUEUED', 'PROCESSING')
            """
        ),
        {"status": status, "summary": summary, "id": job_run_id},
    )


def create_job_run_item(
    conn,
    job_run_id: str,
    row_index: Optional[int] = None,
    status: str = "QUEUED",
    message: Optional[str] = None,
    product_id: Optional[str] = None,
) -> str:
    item_id = new_id()
    conn.execute(
        text(
            """
            INSERT INTO "JobRunItem"
                (id, "jobRunId", "rowIndex", status, message, "productId", "createdAt", "updatedAt")
            VALUES
                (:id, :job_run_id, :row_index, :status, :message, :product_id, now(), now())
            """
        ),
        {
            "id": item_id,
            "job_run_id": job_run_id,
            "row_index": row_index,
            "status": status,
            "message": message,
            "product_id": product_id,
        },
    )
    return item_id
