from celery import Celery

from app.config import settings

celery_app = Celery(
    "storefront_jobs_service",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.tasks_email", "app.tasks_orders", "app.tasks_knowledge", "app.tasks_bulk_upload"],
)

celery_app.conf.timezone = "UTC"
celery_app.conf.beat_schedule = {
    "sweep-expired-orders": {
        "task": "app.tasks_orders.sweep_expired_orders",
        "schedule": settings.sweep_interval_seconds,
    },
}
