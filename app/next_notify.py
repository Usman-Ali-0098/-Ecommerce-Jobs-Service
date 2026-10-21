"""Best-effort callbacks from Celery workers into the Next.js app."""
import json
import logging
from urllib.request import Request, urlopen

from app.config import settings
from app.db import SessionLocal
from app.models import JobRunModel

logger = logging.getLogger(__name__)
TERMINAL_STATUSES = {"COMPLETED", "PARTIAL", "FAILED"}


def notify_nextjs_if_finished(job_run_id: str) -> None:
    """Ask Next.js to publish an admin socket update for a terminal import."""
    with SessionLocal() as session:
        job = session.get(JobRunModel, job_run_id)
        if not job or job.status not in TERMINAL_STATUSES:
            return

    endpoint = settings.app_url.rstrip("/") + "/api/internal/socket-notify"
    request = Request(
        endpoint,
        data=json.dumps({"jobRunId": job_run_id}).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + settings.jobs_api_secret,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=5) as response:
            if response.status >= 300:
                logger.warning("Next.js job notification returned HTTP %s for %s", response.status, job_run_id)
    except Exception:
        # Job state is already committed. Status polling remains the fallback.
        logger.exception("Could not notify Next.js that import %s finished", job_run_id)
