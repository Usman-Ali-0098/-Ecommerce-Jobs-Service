import logging

from app.celery_app import celery_app
from app.db import engine
from app.job_store import mark_job_completed, mark_job_failed, mark_job_started
from app.mailer import send_email

logger = logging.getLogger(__name__)


@celery_app.task(name="app.tasks_email.send_email_task", bind=True, max_retries=3, default_retry_delay=30)
def send_email_task(self, job_id: str, to: str, subject: str, html: str):
    with engine.begin() as conn:
        mark_job_started(conn, job_id)

    try:
        send_email(to=to, subject=subject, html=html)
    except Exception as exc:  # noqa: BLE001 - retry on any SMTP/network error
        logger.exception("send_email_task: failed for job %s (to=%s)", job_id, to)
        with engine.begin() as conn:
            mark_job_failed(conn, job_id, str(exc))
        raise self.retry(exc=exc)

    with engine.begin() as conn:
        mark_job_completed(conn, job_id, summary=f"Email sent to {to}")

    logger.info("send_email_task: sent email for job %s to %s", job_id, to)
    return {"jobId": job_id, "sent": True}
