# Storefront Jobs Service

FastAPI + Celery + Celery Beat service for the `authentication` Next.js
storefront. Runs against the **same Postgres database** as that app — it
only reads/writes rows in tables Prisma owns and never runs migrations
against them. Prisma stays the single source of truth for schema.

> **Companion repo:** this service only makes sense alongside the main
> Next.js app — see **[Ecommerce (authentication)](https://github.com/Usman-Ali-0098/Ecommerce)**.

Three things live here:

1. **Email dispatch** — Next.js renders emails via its own React Email
   templates, then POSTs `{to, subject, html}` to `POST /jobs/email`. This
   service creates a `JobRun` row, returns 202 with its id immediately, and
   a Celery worker actually sends the email via the same Gmail SMTP account
   the Next.js app already used directly. Next.js (or anything else) can
   poll delivery status by reading that `JobRun` row straight from Postgres
   via Prisma (`prisma.jobRun.findUnique`) — this service doesn't expose its
   own `GET /jobs/{id}`, since a shared Postgres table was the point.
2. **Auto-cancel stale unpaid orders** — Celery Beat runs
   `sweep_expired_orders` on a timer (default every `SWEEP_INTERVAL_SECONDS`
   seconds). It finds orders with `status IN (PENDING, PROCESSING)`,
   `paymentStatus IN (FAILED, UNPAID)`, and `updatedAt` older than
   `STALE_AFTER_MINUTES` minutes, then cancels each one, restores its
   reserved stock, and queues a cancellation email through the same
   `send_email_task` used above. This is the single scheduling authority for
   order expiry — the Next.js app's `/api/cron/release-stock` route has been
   retired in favor of this.
3. **Bulk product import** — the browser uploads the CSV directly to
   Cloudinary using a signed credential Next.js issues
   (`/api/admin/upload/csv-signature`); Next.js then creates one `JobRun`
   row pointing at that Cloudinary URL and calls `POST /jobs/bulk-upload`
   with just its id (`/api/admin/upload/csv-initiate`). This service streams
   the CSV straight from its Cloudinary URL — never loading the whole file
   into memory — grouping adjacent rows into products and processing them in
   chunks (`PRODUCTS_PER_CHUNK` products at a time, see
   `app/tasks_bulk_upload.py`). Each chunk runs three phases: validate every
   row first with zero I/O, then resolve/upload only the images still-valid
   rows need (deduplicated and cached for the whole job, uploaded through one
   shared thread pool), then write the whole chunk in a single transaction
   with each product in its own savepoint — one bad product never blocks or
   rolls back the rest of its chunk. One `JobRunItem` is written per product;
   a failed item also saves the row's original field values (`rawRow`) so
   the admin can fix and resubmit just that one product through the existing
   single-product form, without touching the rest of the job. `JobRun` rolls
   up to COMPLETED/PARTIAL/FAILED as chunks complete. The admin page polls
   `GET /api/admin/jobs/[id]` (Next.js reading `JobRun` + `JobRunItem` via
   Prisma) to show live progress grouped by chunk.

## Why this service mixes raw SQL and ORM models

Email dispatch and the order-expiry sweep (`app/job_store.py`,
`app/tasks_orders.py`, `app/tasks_email.py`, `app/tasks_knowledge.py`) use
raw, parametrized SQL via SQLAlchemy Core (`text(...)`) — each only touches
a handful of known columns on tables it doesn't own, so explicit SQL is less
to keep in sync than a duplicate ORM model would be.

Bulk CSV import (`app/tasks_bulk_upload.py`, `app/product_import_orm.py`,
`app/models.py`) is the opposite: it reads and writes several related tables
at once (`Category`, `Color`, `Size`, `Product`, `ProductVariant`,
`ProductImage`, `JobRun`, `JobRunItem`) with real relationships, upserts, and
per-product savepoint/rollback logic. For that shape of problem, full
SQLAlchemy ORM declarative models are what's actually declared in
`app/models.py` and used throughout — hand-written raw SQL for every one of
those branches would be far more error-prone to get right and keep correct.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate   # Python 3.9+ (repo is written for 3.9 compat —
                             # no `X | None` syntax, uses typing.Optional)
pip install -r requirements.txt

cp .env.example .env
# Edit .env:
#  - DATABASE_URL: paste from the Next.js app's .env, but change
#    "sslmode=verify-full&channel_binding=require" to "sslmode=require"
#    (psycopg2-binary's bundled OpenSSL can't verify Neon's cert chain on
#    macOS otherwise).
#  - SMTP_*/EMAIL_FROM: same Gmail SMTP account as the Next.js app's .env.
#  - JOBS_API_SECRET: generate one, put the same value in the Next.js app's
#    .env as JOBS_API_SECRET (and set FASTAPI_JOBS_URL there too).

docker compose up -d   # starts Redis (broker + result backend)
```

## Running

Three processes, each in its own terminal (with `.venv` activated):

source .venv/bin/activate   
```bash
# 1. Worker — executes send_email_task / expire_order tasks
celery -A app.celery_app worker --loglevel=info --pool=solo
# 2. Beat — schedules sweep_expired_orders every SWEEP_INTERVAL_SECONDS
celery -A app.celery_app beat --loglevel=info

# 3. FastAPI — the HTTP API Next.js calls (POST /jobs/email,
#    POST /jobs/bulk-upload, POST /sweep)
uvicorn app.main:app --reload --port 8001
```

Trigger a sweep immediately instead of waiting for the next Beat tick, or
send a test email — both need the `JOBS_API_SECRET` bearer token:

```bash
curl -X POST http://localhost:8001/sweep \
  -H "Authorization: Bearer <JOBS_API_SECRET>"

curl -X POST http://localhost:8001/jobs/email \
  -H "Authorization: Bearer <JOBS_API_SECRET>" \
  -H "Content-Type: application/json" \
  -d '{"to":"you@example.com","subject":"Test","html":"<p>Hello</p>"}'
```

## What `expire_order` does, per order

1. Claims the cancellation via a guarded `UPDATE ... WHERE ... RETURNING` —
   if another process (this service or the Next.js app's own
   `/api/cron/release-stock`) already claimed it, this becomes a no-op. Safe
   to run both simultaneously during a transition period.
2. Restores `ProductVariant.stock` for each `OrderItem`.
3. Closes any still-open `PaymentAttempt`s on that order.
4. Inserts a `Notification` (customer) and `AdminNotification` (admin).
5. Creates a `JobRun` (type EMAIL) and queues `send_email_task` to email the
   customer at the order's `shippingEmail`.

## Real-time in-app notifications: what this service does *not* do

The Next.js app pushes live notifications over a Socket.IO server embedded
in its own Node process (`server.ts`). This service only writes the
`Notification`/`AdminNotification` rows to Postgres — it has no way to reach
that in-memory socket server directly. The Next.js app's notification
dropdown already has a polling fallback, so cancellations still show up
there, just not instantly via the socket. If instant push matters later, add
a small authenticated webhook on the Next.js side (e.g.
`POST /api/internal/notifications/publish`) and call it from `expire_order`
after the transaction commits.

## Next.js cron route — retired

`/api/cron/release-stock` and the `releaseOrderStock` /
`releaseExpiredStockReservations` functions behind it have been removed from
the Next.js app. `sweep_expired_orders` here is now the only thing that
cancels stale orders and releases stock; tune `STALE_AFTER_MINUTES` in
`.env` to whatever window the business actually needs.
