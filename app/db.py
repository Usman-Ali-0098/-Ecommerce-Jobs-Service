from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

# pool_pre_ping guards against the remote Postgres (Neon) dropping idle
# connections between Celery Beat ticks.
engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)

# ORM sessions for the jobs service. Prisma remains the migration/schema owner.
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
