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
3. **Bulk product import** — the Next.js admin CSV-import page
   (`/admin/products/import`) resolves category/color/size names to ids and
   uploads any images to Cloudinary itself, then POSTs the final
   `{products: [...]}` array to `POST /jobs/products/import`. This service
   creates one `JobRunItem` per product plus one `import_one_product` Celery
   task per product (so a bad row doesn't block the rest of the batch and
   products import in parallel across workers), and rolls the parent
   `JobRun` up to COMPLETED/PARTIAL/FAILED once every item is done. The
   admin page polls `GET /api/admin/jobs/[id]` (Next.js reading `JobRun` +
   `JobRunItem` via Prisma) to show live per-row progress.

## Why SQLAlchemy Core instead of ORM models

This service only runs a handful of parametrized `UPDATE`/`INSERT`/`SELECT`
statements against tables it doesn't own. Raw, explicit SQL against exact
column names is less to keep in sync than a full duplicate ORM model set
mirroring `schema.prisma` — every column referenced is visible directly in
`app/tasks_orders.py` / `app/job_store.py`.

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
#    POST /jobs/products/import, POST /sweep)
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
