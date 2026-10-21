from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str
    celery_broker_url: str = "redis://localhost:6379/0"
    celery_result_backend: str = "redis://localhost:6379/1"
    sweep_interval_seconds: int = 120

    cloudinary_cloud_name: Optional[str] = None
    cloudinary_api_key: Optional[str] = None
    cloudinary_api_secret: Optional[str] = None
    bulk_upload_images_dir: str = "./bulk-upload-images"
    bulk_upload_max_csv_bytes: int = 50 * 1024 * 1024
    bulk_upload_max_rows: int = 10_000

    # Base URL of the Next.js storefront, used to build links (e.g. the
    # "Reorder" link in the cancellation email) that point back at it.
    app_url: str = "http://localhost:3000"

    smtp_host: str
    smtp_port: int
    smtp_user: str
    smtp_password: str
    email_from: str

    # Shared secret Next.js sends as "Authorization: Bearer <jobs_api_secret>"
    # on every request to this service's HTTP API.
    jobs_api_secret: str

    # Google AI Studio API key, used only for embedding calls
    # (gemini-embedding-001) when indexing KnowledgeChunk rows.
    gemini_api_key: str


settings = Settings()
