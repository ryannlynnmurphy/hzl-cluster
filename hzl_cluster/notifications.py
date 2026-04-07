"""
Notification manager -- in-memory store for Hazel to speak aloud or display.
Notifications come from sync events, health alerts, queue status, etc.
"""

import time
import uuid
from collections import deque

PRIORITY_CRITICAL = "critical"
PRIORITY_NORMAL   = "normal"
PRIORITY_LOW      = "low"

_VALID_PRIORITIES = {PRIORITY_CRITICAL, PRIORITY_NORMAL, PRIORITY_LOW}


class NotificationManager:
    """In-memory store for cluster notifications.

    Notifications are kept in insertion order internally; callers receive
    them newest-first via get_unread().  The store is capped at
    max_notifications entries; oldest are dropped when the cap is reached.
    """

    def __init__(self, max_notifications: int = 100):
        self._max = max_notifications
        self._store: deque[dict] = deque()

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add(
        self,
        title: str,
        body: str,
        source: str,
        priority: str = PRIORITY_NORMAL,
    ) -> dict:
        """Create and store a new notification, returning the notification dict.

        Raises ValueError if priority is not one of the accepted values.
        Drops the oldest notification when the store is at capacity.
        """
        if priority not in _VALID_PRIORITIES:
            raise ValueError(
                f"Invalid priority {priority!r}. "
                f"Must be one of: {sorted(_VALID_PRIORITIES)}"
            )

        notification = {
            "id":        str(uuid.uuid4()),
            "title":     title,
            "body":      body,
            "source":    source,
            "priority":  priority,
            "timestamp": time.time(),
            "read":      False,
        }

        if len(self._store) >= self._max:
            self._store.popleft()

        self._store.append(notification)
        return notification

    def get_unread(self) -> list[dict]:
        """Return all unread notifications, newest first."""
        return [n for n in reversed(self._store) if not n["read"]]

    def mark_read(self, notification_id: str) -> None:
        """Mark a single notification as read.  Silently ignores unknown IDs."""
        for n in self._store:
            if n["id"] == notification_id:
                n["read"] = True
                return

    def mark_all_read(self) -> None:
        """Mark every notification in the store as read."""
        for n in self._store:
            n["read"] = True

    def clear_old(self, hours: int = 24) -> None:
        """Remove notifications older than *hours* hours."""
        cutoff = time.time() - hours * 3600
        self._store = deque(
            (n for n in self._store if n["timestamp"] >= cutoff),
            maxlen=None,
        )

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def count(self) -> dict:
        """Return total, unread, and critical counts."""
        total    = len(self._store)
        unread   = sum(1 for n in self._store if not n["read"])
        critical = sum(1 for n in self._store if n["priority"] == PRIORITY_CRITICAL)
        return {"total": total, "unread": unread, "critical": critical}

    def summary(self) -> str:
        """Return a human-readable summary suitable for Hazel to speak aloud.

        Examples:
          "You have no unread notifications."
          "You have 1 unread notification."
          "You have 3 unread notifications. 1 critical: Gateway sync failed."
        """
        unread_notifications = self.get_unread()
        n = len(unread_notifications)

        if n == 0:
            return "You have no unread notifications."

        noun = "notification" if n == 1 else "notifications"
        base = f"You have {n} unread {noun}."

        critical = [x for x in unread_notifications if x["priority"] == PRIORITY_CRITICAL]
        if critical:
            c = len(critical)
            c_noun = "critical" if c == 1 else "critical"
            # Attach the title of the most recent critical notification.
            latest_title = critical[0]["title"]
            base += f" {c} {c_noun}: {latest_title}."

        return base
