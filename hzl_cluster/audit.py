"""
Security audit reports -- human-readable summaries of cluster activity.
Parses relay audit logs, scan results, and queue stats.
Answers: how long online, what was fetched, what was blocked.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Optional


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> Optional[datetime]:
    """Parse an ISO-8601 UTC timestamp string like 2026-04-07T06:00:01Z."""
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _format_duration(total_seconds: float) -> str:
    """Return a human-readable duration string, e.g. '5 minutes and 12 seconds'."""
    total_seconds = int(total_seconds)
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60

    parts = []
    if hours:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds or not parts:
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")

    if len(parts) == 1:
        return parts[0]
    return " and ".join([", ".join(parts[:-1]), parts[-1]])


def _count_word(n: int, singular: str, plural: Optional[str] = None) -> str:
    """Return 'No Xs', 'One X', or 'N Xs'."""
    if plural is None:
        plural = singular + "s"
    if n == 0:
        return f"No {plural}"
    if n == 1:
        return f"One {singular}"
    return f"{n} {plural}"


# ---------------------------------------------------------------------------
# AuditReporter
# ---------------------------------------------------------------------------

class AuditReporter:
    """
    Generates human-readable security audit reports from relay logs and
    queue activity.

    relay_log      — list of raw audit log strings from RelayController.get_audit_log()
    queue_db_path  — path to the SQLite queue database managed by QueueDB
    """

    def __init__(
        self,
        relay_log: Optional[List[str]] = None,
        queue_db_path: Optional[str] = None,
    ) -> None:
        self._relay_log: List[str] = relay_log or []
        self._queue_db_path: Optional[str] = queue_db_path

    # ------------------------------------------------------------------
    # Online time
    # ------------------------------------------------------------------

    def online_time_today(self) -> dict:
        """
        Parse the relay log and return total online time for the current UTC day.

        Returns:
            {
                "total_seconds": float,
                "sessions": [{"start": str, "end": str | None, "duration": float, "reason": str}]
            }

        A session begins at RELAY_OPEN and ends at the next RELAY_CLOSE or
        EMERGENCY_DISCONNECT.  If the relay is still open (no closing event),
        the session is treated as ongoing and excluded from the total.
        """
        today = datetime.now(timezone.utc).date()
        sessions = []
        total_seconds = 0.0

        open_event: Optional[tuple[datetime, str]] = None  # (timestamp, reason)

        for line in self._relay_log:
            parts = line.split()
            if len(parts) < 2:
                continue

            ts = _parse_ts(parts[0])
            if ts is None:
                continue

            event = parts[1]

            # Extract reason=... if present
            reason = ""
            for part in parts[2:]:
                if part.startswith("reason="):
                    reason = part[len("reason="):]
                    break

            if event == "RELAY_OPEN":
                open_event = (ts, reason)

            elif event in ("RELAY_CLOSE", "EMERGENCY_DISCONNECT") and open_event is not None:
                start_ts, open_reason = open_event
                end_ts = ts

                # Only count sessions that overlap with today
                if start_ts.date() == today or end_ts.date() == today:
                    # Clamp to today's boundary if the session spans midnight
                    today_start = datetime.combine(today, datetime.min.time()).replace(tzinfo=timezone.utc)
                    today_end = today_start + timedelta(days=1)

                    clamped_start = max(start_ts, today_start)
                    clamped_end = min(end_ts, today_end)

                    duration = max(0.0, (clamped_end - clamped_start).total_seconds())
                    total_seconds += duration

                    sessions.append({
                        "start": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "end": end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "duration": duration,
                        "reason": open_reason,
                    })

                open_event = None

        return {"total_seconds": total_seconds, "sessions": sessions}

    # ------------------------------------------------------------------
    # Quarantined files
    # ------------------------------------------------------------------

    def files_quarantined(self, since_hours: float = 24) -> List[dict]:
        """
        Return a list of quarantined-file events from the relay log.

        Looks for QUARANTINE log entries in the format:
            2026-04-07T06:00:05Z QUARANTINE file=report.exe reason=blocked_extension

        Returns list of {"file": str, "reason": str, "timestamp": str}.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        quarantined = []

        for line in self._relay_log:
            parts = line.split()
            if len(parts) < 2:
                continue

            ts = _parse_ts(parts[0])
            if ts is None or ts < cutoff:
                continue

            if parts[1] != "QUARANTINE":
                continue

            entry: dict = {"file": "", "reason": "", "timestamp": parts[0]}
            for part in parts[2:]:
                if part.startswith("file="):
                    entry["file"] = part[len("file="):]
                elif part.startswith("reason="):
                    entry["reason"] = part[len("reason="):]

            quarantined.append(entry)

        return quarantined

    # ------------------------------------------------------------------
    # Sync history
    # ------------------------------------------------------------------

    def sync_history(self, days: int = 7) -> List[dict]:
        """
        Return sync cycles logged in the relay log over the past N days.

        Looks for SYNC_START / SYNC_END pairs:
            2026-04-07T06:00:01Z SYNC_START reason=scheduled_sync
            2026-04-07T06:00:45Z SYNC_END items_fetched=12 items_quarantined=0

        Returns list of:
            {"timestamp": str, "duration": float, "items_fetched": int, "items_quarantined": int, "reason": str}
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        cycles = []

        open_sync: Optional[tuple[datetime, str]] = None  # (timestamp, reason)

        for line in self._relay_log:
            parts = line.split()
            if len(parts) < 2:
                continue

            ts = _parse_ts(parts[0])
            if ts is None:
                continue

            event = parts[1]

            if event == "SYNC_START":
                reason = ""
                for part in parts[2:]:
                    if part.startswith("reason="):
                        reason = part[len("reason="):]
                open_sync = (ts, reason)

            elif event == "SYNC_END" and open_sync is not None:
                start_ts, reason = open_sync
                if start_ts >= cutoff:
                    duration = (ts - start_ts).total_seconds()

                    items_fetched = 0
                    items_quarantined = 0
                    for part in parts[2:]:
                        if part.startswith("items_fetched="):
                            try:
                                items_fetched = int(part[len("items_fetched="):])
                            except ValueError:
                                pass
                        elif part.startswith("items_quarantined="):
                            try:
                                items_quarantined = int(part[len("items_quarantined="):])
                            except ValueError:
                                pass

                    cycles.append({
                        "timestamp": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "duration": duration,
                        "items_fetched": items_fetched,
                        "items_quarantined": items_quarantined,
                        "reason": reason,
                    })

                open_sync = None

        return cycles

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    def daily_summary(self) -> str:
        """
        Return a human-readable summary string suitable for Hazel to speak aloud.

        Example:
            "Gateway was online for 5 minutes and 12 seconds today. One sync cycle
             at 6:00 AM. 12 items fetched. No files quarantined. System secure."
        """
        online = self.online_time_today()
        quarantined = self.files_quarantined(since_hours=24)
        syncs = self.sync_history(days=1)

        # Online duration
        total_sec = online["total_seconds"]
        if total_sec > 0:
            online_str = f"Gateway was online for {_format_duration(total_sec)} today."
        else:
            online_str = "Gateway was not online today."

        # Sync cycles
        sync_count = len(syncs)
        if sync_count == 0:
            sync_str = "No sync cycles recorded."
        else:
            # Report the first sync's time in a readable format
            first_ts = _parse_ts(syncs[0]["timestamp"])
            time_str = first_ts.strftime("%I:%M %p").lstrip("0") if first_ts else "unknown time"
            cycle_word = _count_word(sync_count, "sync cycle")
            if sync_count == 1:
                sync_str = f"{cycle_word} at {time_str}."
            else:
                sync_str = f"{cycle_word}. First at {time_str}."

        # Items fetched
        total_fetched = sum(s["items_fetched"] for s in syncs)
        fetched_str = f"{_count_word(total_fetched, 'item')} fetched."

        # Quarantined files
        q_count = len(quarantined)
        if q_count == 0:
            quarantine_str = "No files quarantined."
        else:
            quarantine_str = f"{_count_word(q_count, 'file')} quarantined."

        # Security verdict
        verdict = "System secure." if q_count == 0 else "Review quarantine log."

        return " ".join([online_str, sync_str, fetched_str, quarantine_str, verdict])

    # ------------------------------------------------------------------
    # Security score
    # ------------------------------------------------------------------

    def security_score(self) -> dict:
        """
        Return a security score from 0-100 with contributing factors.

        Factors (each contributes a weighted penalty when conditions are bad):
            time_online         — less time online is better (10+ min starts penalising)
            quarantined_files   — any quarantined files penalise heavily
            failed_auths        — AUTH_FAIL events in the log penalise
            queue_overflow      — QUEUE_OVERFLOW events penalise

        Returns:
            {"score": int, "factors": [{"name": str, "value": ..., "weight": int}]}
        """
        online = self.online_time_today()
        quarantined = self.files_quarantined(since_hours=24)

        # Count failed auth events
        failed_auths = sum(
            1 for line in self._relay_log
            if len(line.split()) >= 2 and line.split()[1] == "AUTH_FAIL"
        )

        # Count queue overflow events
        queue_overflows = sum(
            1 for line in self._relay_log
            if len(line.split()) >= 2 and line.split()[1] == "QUEUE_OVERFLOW"
        )

        factors = [
            {"name": "time_online",       "value": online["total_seconds"], "weight": 20},
            {"name": "quarantined_files",  "value": len(quarantined),        "weight": 40},
            {"name": "failed_auths",       "value": failed_auths,            "weight": 25},
            {"name": "queue_overflow",     "value": queue_overflows,         "weight": 15},
        ]

        # Calculate penalty per factor
        penalty = 0.0

        # time_online: 0 penalty if <= 10 min; linear up to full weight at 60 min
        online_minutes = online["total_seconds"] / 60.0
        if online_minutes > 10:
            ratio = min(1.0, (online_minutes - 10) / 50.0)
            penalty += factors[0]["weight"] * ratio

        # quarantined_files: 10 points per file, capped at full weight
        if len(quarantined) > 0:
            ratio = min(1.0, len(quarantined) / 5.0)
            penalty += factors[1]["weight"] * ratio

        # failed_auths: 12 points per failure, capped at full weight
        if failed_auths > 0:
            ratio = min(1.0, failed_auths / 3.0)
            penalty += factors[2]["weight"] * ratio

        # queue_overflow: full penalty on any overflow
        if queue_overflows > 0:
            penalty += factors[3]["weight"]

        score = max(0, round(100 - penalty))

        return {"score": score, "factors": factors}
