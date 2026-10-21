import logging

from sqlalchemy import bindparam, text

from app.celery_app import celery_app
from app.config import settings
from app.db import engine
from app.ids import new_id
from app.job_store import create_job_run
from app.tasks_email import send_email_task

logger = logging.getLogger(__name__)

# Orders sitting in one of these order statuses, with one of these payment
# statuses, past their Order.paymentRetryExpiresAt deadline, are cancelled
# and their stock restored. UNPAID is included (not just FAILED) so orders
# that were simply abandoned — no decline ever happened, no retry attempted
# — still get cleaned up; otherwise they'd sit unpaid forever.
#
# paymentRetryExpiresAt (not a generic "last updated" staleness window) is
# the eligibility signal on purpose: it's a fixed deadline set once at order
# creation (Next.js PAYMENT_RETRY_LIFETIME_MS) and pulled forward to "now"
# the instant the customer's retry window truly ends — there's no cap on
# how many times they can retry before then, so a rolling "updated recently"
# check would let each retry push cancellation further out indefinitely.
# Keying off the fixed deadline instead means the worst case is always
# bounded to that deadline plus one sweep interval, no matter how much retry
# activity happened before it.
CANCELABLE_ORDER_STATUSES = ("PENDING", "PROCESSING")
CANCELABLE_PAYMENT_STATUSES = ("FAILED", "UNPAID")

# Status literals ('CANCELLED', 'CANCELED', ...) written directly into SQL
# text below are fixed constants, not bound parameters — Postgres resolves
# literal text in context of the compared enum column with no ambiguity.
# The two places a *list* of statuses is bound as a parameter, the enum
# column is cast to ::text first, sidestepping psycopg2/Postgres needing to
# infer the bind parameter's enum type in a WHERE/IN comparison.
#
# Only CARD orders ever get auto-cancelled here. A COD order sits in
# PENDING/UNPAID by design until it's delivered and paid in person — that's
# its normal resting state, not an abandoned checkout — so it must never be
# swept up just because it looks the same age as a genuinely abandoned card
# payment. LEGACY orders are excluded for the same reason: no PaymentAttempt
# lifecycle applies to them either.


def _money(amount) -> str:
    try:
        value = round(float(amount))
    except (TypeError, ValueError):
        value = 0
    return f"Rs. {value:,}"


def _order_item_row_html(item) -> str:
    # Mirrors the item-row pattern from the Next.js OrderInvoiceEmail
    # (src/emails/order-invoice-email.tsx), plus a thumbnail so the
    # customer can see at a glance which products were cancelled.
    meta_parts = [f"SKU: {item.sku}"]
    if item.colorName:
        meta_parts.append(item.colorName)
    if item.sizeName:
        meta_parts.append(item.sizeName)
    meta = " · ".join(meta_parts) + f" · Qty {item.quantity}"

    image_cell = (
        f'<img src="{item.imageUrl}" alt="{item.imageAltText or item.productName}" '
        'width="48" height="48" style="display:block; width:48px; height:48px; '
        'border-radius:6px; object-fit:cover; border:1px solid #e5e7eb;" />'
        if item.imageUrl
        else '<div style="width:48px; height:48px; border-radius:6px; '
        'background-color:#f3f4f6; border:1px solid #e5e7eb;"></div>'
    )

    return f"""
            <tr>
              <td style="padding:10px 10px 0 0; width:48px; vertical-align:top;">{image_cell}</td>
              <td style="padding:10px 0 0; vertical-align:top;">
                <p style="margin:0; color:#111827; font-size:12px; font-weight:600;">{item.productName}</p>
                <p style="margin:2px 0 0; color:#9ca3af; font-size:10px;">{meta}</p>
              </td>
              <td style="padding:10px 0 0; vertical-align:top; text-align:right; white-space:nowrap;">
                <p style="margin:0; color:#374151; font-size:12px;">{_money(item.lineTotal)}</p>
              </td>
            </tr>"""


def _order_cancelled_email_html(order_number: str, items, reorder_url: str) -> str:
    # Mirrors the branding of the Next.js app's OrderInvoiceEmail
    # (src/emails/order-invoice-email.tsx) so customers see a consistent
    # look whether the email comes from Next.js (invoice) or this service
    # (auto-cancellation) — same header bar, container, and font stack.
    items_html = "".join(_order_item_row_html(item) for item in items)
    items_section = (
        f"""
          <div style="border-top:1px solid #e5e7eb; border-bottom:1px solid #e5e7eb; margin:14px 0 12px; padding-bottom:2px;">
            <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
              <tbody>{items_html}
              </tbody>
            </table>
          </div>
        """
        if items
        else ""
    )
    # Deep-links to the order detail page rather than mutating anything
    # itself — email clients / security scanners can prefetch GET links, so
    # the actual "add to cart" action only ever happens from an explicit
    # button click once the customer is on that page, logged in.
    reorder_button = (
        f"""
          <table role="presentation" cellpadding="0" cellspacing="0" style="margin:4px auto 14px;">
            <tr>
              <td style="border-radius:6px; background-color:#087ff5;">
                <a href="{reorder_url}" target="_blank" rel="noopener"
                   style="display:inline-block; padding:9px 20px; color:#ffffff; font-size:12px; font-weight:600; text-decoration:none; border-radius:6px;">
                  Reorder these items
                </a>
              </td>
            </tr>
          </table>
        """
        if items
        else ""
    )
    return f"""
    <body style="margin:0; padding:20px 10px; background-color:#f7f9fb; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;">
      <div style="width:100%; max-width:480px; margin:0 auto; overflow:hidden; background-color:#ffffff; border:1px solid #e5e7eb; border-radius:7px;">
        <div style="padding:14px 20px; background-color:#087ff5; text-align:center;">
          <p style="margin:0; color:#ffffff; font-size:11px; font-weight:700; letter-spacing:1.2px;">BudgetVibe</p>
        </div>
        <div style="padding:24px 24px 20px;">
          <h1 style="margin:0 0 18px; color:#111827; font-size:20px; font-weight:600; line-height:1.25; text-align:center;">Order Cancelled</h1>
          <p style="margin:0 0 12px; color:#374151; font-size:13px; line-height:1.55;">
            Your order <strong>{order_number}</strong> has been cancelled because
            payment was not completed in time. Any reserved stock has been
            released back into inventory.
          </p>
          {items_section}
          {reorder_button}
          <p style="margin:0; color:#374151; font-size:13px; line-height:1.55;">
            If this was a mistake, feel free to place a new order any time.
          </p>
        </div>
        <div style="padding:12px 18px; background-color:#f9fafb; border-top:1px solid #e5e7eb;">
          <p style="margin:0; color:#9ca3af; font-size:9px; line-height:1.4; text-align:center;">
            Keep this email for your records.
          </p>
        </div>
      </div>
    </body>
    """


@celery_app.task(name="app.tasks_orders.sweep_expired_orders")
def sweep_expired_orders():
    """Finds orders eligible for auto-cancellation and fans out one
    expire_order task per order. This is the single scheduling authority for
    order expiry — the Next.js app's old /api/cron/release-stock route has
    been retired in favor of this."""
    with engine.connect() as conn:
        stmt = text(
            """
            SELECT id FROM "Order"
            WHERE status::text IN :order_statuses
              AND "paymentStatus"::text IN :payment_statuses
              AND "paymentMethod" = 'CARD'
              AND "paymentRetryExpiresAt" IS NOT NULL
              AND "paymentRetryExpiresAt" < now()
              AND "stockReleasedAt" IS NULL
            """
        ).bindparams(
            bindparam("order_statuses", expanding=True),
            bindparam("payment_statuses", expanding=True),
        )
        rows = conn.execute(
            stmt,
            {
                "order_statuses": list(CANCELABLE_ORDER_STATUSES),
                "payment_statuses": list(CANCELABLE_PAYMENT_STATUSES),
            },
        ).fetchall()

    order_ids = [row.id for row in rows]
    for order_id in order_ids:
        expire_order.delay(order_id)

    logger.info("sweep_expired_orders: queued %d order(s)", len(order_ids))
    return {"queued": len(order_ids)}


@celery_app.task(name="app.tasks_orders.expire_order", bind=True, max_retries=3, default_retry_delay=30)
def expire_order(self, order_id: str):
    """Idempotent: claims the cancellation via a guarded UPDATE (only one
    caller ever sees a claimed row), then restores stock, closes any open
    PaymentAttempts, writes in-app notifications, and queues an order-status
    email to the customer."""
    try:
        with engine.begin() as conn:
            claim_stmt = text(
                """
                UPDATE "Order"
                SET status = 'CANCELLED',
                    "paymentStatus" = 'CANCELED',
                    "cancelledAt" = now(),
                    "stockReleasedAt" = now(),
                    "updatedAt" = now()
                WHERE id = :order_id
                  AND status::text IN :order_statuses
                  AND "paymentStatus"::text IN :payment_statuses
                  AND "paymentMethod" = 'CARD'
                  AND "paymentRetryExpiresAt" IS NOT NULL
                  AND "paymentRetryExpiresAt" < now()
                  AND "stockReleasedAt" IS NULL
                RETURNING id, "userId", "orderNumber", "shippingEmail"
                """
            ).bindparams(
                bindparam("order_statuses", expanding=True),
                bindparam("payment_statuses", expanding=True),
            )

            claimed = conn.execute(
                claim_stmt,
                {
                    "order_id": order_id,
                    "order_statuses": list(CANCELABLE_ORDER_STATUSES),
                    "payment_statuses": list(CANCELABLE_PAYMENT_STATUSES),
                },
            ).fetchone()

            if claimed is None:
                logger.info("expire_order: %s already handled or no longer eligible", order_id)
                return {"orderId": order_id, "skipped": True}

            items = conn.execute(
                text(
                    """
                    SELECT "variantId", "productName", sku, "colorName", "sizeName",
                           quantity, "unitPrice", "lineTotal", "imageUrl", "imageAltText"
                    FROM "OrderItem"
                    WHERE "orderId" = :order_id
                    """
                ),
                {"order_id": order_id},
            ).fetchall()

            for item in items:
                if item.variantId:
                    conn.execute(
                        text(
                            """
                            UPDATE "ProductVariant"
                            SET stock = stock + :qty, "updatedAt" = now()
                            WHERE id = :variant_id
                            """
                        ),
                        {"qty": item.quantity, "variant_id": item.variantId},
                    )

            conn.execute(
                text(
                    """
                    UPDATE "PaymentAttempt"
                    SET status = 'CANCELED', "stockReleasedAt" = now(), "updatedAt" = now()
                    WHERE "orderId" = :order_id
                      AND "stockReleasedAt" IS NULL
                      AND status != 'PAID'
                    """
                ),
                {"order_id": order_id},
            )

            order_number = claimed.orderNumber
            shipping_email = claimed.shippingEmail

            conn.execute(
                text(
                    """
                    INSERT INTO "Notification"
                        (id, "userId", "orderId", type, title, message, "isRead", "createdAt")
                    VALUES
                        (:id, :user_id, :order_id, 'ORDER_CANCELLED', :title, :message, false, now())
                    """
                ),
                {
                    "id": new_id(),
                    "user_id": claimed.userId,
                    "order_id": order_id,
                    "title": "Order Cancelled",
                    "message": f"Order {order_number} was cancelled because payment was not completed in time.",
                },
            )

            conn.execute(
                text(
                    """
                    INSERT INTO "AdminNotification"
                        (id, "orderId", type, title, message, "isRead", "createdAt")
                    VALUES
                        (:id, :order_id, 'ORDER_CANCELLED', :title, :message, false, now())
                    """
                ),
                {
                    "id": new_id(),
                    "order_id": order_id,
                    "title": "Order Cancelled (Payment Expired)",
                    "message": f"Order {order_number} was cancelled after its payment window expired.",
                },
            )

            email_job_id = create_job_run(conn, "EMAIL") if shipping_email else None

        if email_job_id and shipping_email:
            reorder_url = f"{settings.app_url}/orders/{order_id}"
            send_email_task.delay(
                email_job_id,
                shipping_email,
                f"Order {order_number} cancelled",
                _order_cancelled_email_html(order_number, items, reorder_url),
            )

        logger.info("expire_order: cancelled %s and restored stock", order_id)
        return {"orderId": order_id, "skipped": False}
    except Exception as exc:  # noqa: BLE001 - retry on any transient DB/network error
        logger.exception("expire_order: failed for %s", order_id)
        raise self.retry(exc=exc)
