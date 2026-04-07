"""
Cluster event bus -- pub/sub for topology and state changes.
Handlers register for event types and get called when events fire.
Supports both sync and async handlers.
"""

import asyncio
import inspect
from collections import defaultdict

# --- Event type constants ---

EVENT_NODE_JOINED     = "node.joined"       # kwargs: hostname, role, ip
EVENT_NODE_LOST       = "node.lost"         # kwargs: hostname, role
EVENT_NODE_RECOVERED  = "node.recovered"    # kwargs: hostname, role, ip
EVENT_RELAY_OPENED    = "relay.opened"      # kwargs: reason
EVENT_RELAY_CLOSED    = "relay.closed"      # kwargs: reason
EVENT_SYNC_STARTED    = "sync.started"      # kwargs: trigger (scheduled/manual/threshold)
EVENT_SYNC_COMPLETED  = "sync.completed"    # kwargs: results dict
EVENT_QUEUE_THRESHOLD = "queue.threshold"   # kwargs: depth, threshold
EVENT_PHONE_DOCKED    = "phone.docked"      # kwargs: hostname, ip
EVENT_PHONE_UNDOCKED  = "phone.undocked"    # kwargs: hostname
EVENT_EMERGENCY       = "emergency"         # kwargs: reason


class EventBus:
    """Pub/sub event bus for cluster topology and state changes."""

    def __init__(self):
        self._handlers: dict[str, list] = defaultdict(list)

    def on(self, event_type: str, handler: callable) -> None:
        """Register a handler for an event type."""
        if handler not in self._handlers[event_type]:
            self._handlers[event_type].append(handler)

    def off(self, event_type: str, handler: callable) -> None:
        """Unregister a handler for an event type."""
        try:
            self._handlers[event_type].remove(handler)
        except ValueError:
            pass

    async def emit(self, event_type: str, **kwargs) -> None:
        """Fire all handlers registered for event_type.

        Sync handlers are called directly; async handlers are awaited.
        Handlers for an unregistered event type are silently skipped.
        """
        for handler in list(self._handlers.get(event_type, [])):
            if inspect.iscoroutinefunction(handler):
                await handler(**kwargs)
            else:
                handler(**kwargs)

    def handlers(self, event_type: str) -> list:
        """Return a copy of the handler list for event_type."""
        return list(self._handlers.get(event_type, []))

    def clear(self) -> None:
        """Remove all registered handlers."""
        self._handlers.clear()
