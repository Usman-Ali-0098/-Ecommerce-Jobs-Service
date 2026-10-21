import uuid


def new_id() -> str:
    """Id for rows this service inserts (Notification, AdminNotification).

    Prisma's @default(cuid()) is generated in the Node client, not by a
    Postgres DEFAULT — so this service must supply its own id. Format
    doesn't need to match Prisma's cuid()s; these are opaque text primary
    keys compared only for equality.
    """
    return uuid.uuid4().hex
