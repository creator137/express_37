"""Small SQLite store for subscriptions, rounds, counters and signal delivery."""

from __future__ import annotations

from datetime import datetime, timezone
import sqlite3
from pathlib import Path

from config import Config
from signals import advance, signal_text


class Storage:
    def __init__(self, path: Path, config: Config):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.config = config
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id INTEGER PRIMARY KEY, active INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS rounds (
                round_id INTEGER PRIMARY KEY, number INTEGER NOT NULL,
                ended_at TEXT, received_at TEXT NOT NULL, source TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS misses (
                group_key TEXT PRIMARY KEY, value INTEGER
            );
            CREATE TABLE IF NOT EXISTS notifications (
                round_id INTEGER NOT NULL, group_key TEXT NOT NULL,
                chat_id INTEGER NOT NULL, text TEXT NOT NULL,
                status TEXT NOT NULL, attempted_at TEXT, sent_at TEXT,
                error TEXT,
                PRIMARY KEY (round_id, group_key, chat_id)
            );
        """)
        with self.db:
            for key in config.groups:
                self.db.execute("INSERT OR IGNORE INTO misses VALUES (?, 0)", (key,))

    def close(self):
        self.db.close()

    def last_round_id(self) -> int | None:
        row = self.db.execute("SELECT value FROM meta WHERE key='last_round_id'").fetchone()
        return int(row[0]) if row else None

    def set_baseline(self, round_id: int) -> bool:
        """Skip the initial WebSocket snapshot; remember its ID for deduplication."""
        if self.last_round_id() is not None:
            return False
        with self.db:
            self.db.execute("INSERT INTO meta VALUES ('last_round_id', ?)", (str(round_id),))
        return True

    def subscribe(self, chat_id: int):
        now = datetime.now(timezone.utc).isoformat()
        with self.db:
            self.db.execute("""INSERT INTO subscribers VALUES (?, 1, ?)
                ON CONFLICT(chat_id) DO UPDATE SET active=1, updated_at=excluded.updated_at""",
                (chat_id, now))

    def unsubscribe(self, chat_id: int):
        now = datetime.now(timezone.utc).isoformat()
        with self.db:
            self.db.execute("UPDATE subscribers SET active=0, updated_at=? WHERE chat_id=?", (now, chat_id))
            self.db.execute("UPDATE notifications SET status='cancelled' WHERE chat_id=? AND status='pending'", (chat_id,))

    def is_subscribed(self, chat_id: int) -> bool:
        row = self.db.execute("SELECT active FROM subscribers WHERE chat_id=?", (chat_id,)).fetchone()
        return bool(row and row[0])

    def misses(self) -> dict[str, int | None]:
        rows = self.db.execute("SELECT group_key, value FROM misses").fetchall()
        return {row[0]: row[1] for row in rows if row[0] in self.config.groups}

    def recent_numbers(self, limit: int | None = None) -> list[int]:
        limit = limit or self.config.last_numbers_count
        rows = self.db.execute("SELECT number FROM rounds ORDER BY round_id DESC LIMIT ?", (limit,)).fetchall()
        return [row[0] for row in reversed(rows)]

    def last_round_time(self) -> str | None:
        row = self.db.execute("SELECT received_at FROM rounds ORDER BY round_id DESC LIMIT 1").fetchone()
        return row[0] if row else None

    def apply_round(self, round_id: int, number: int, ended_at: datetime | None,
                    source: str = "live", notify: bool = True) -> tuple[dict[str, int | None], list[str]] | None:
        """Update a round and enqueue its signals in one transaction."""
        if type(round_id) is not int or round_id < 0:
            raise ValueError("invalid round ID")
        if type(number) is not int or not 0 <= number <= 36:
            raise ValueError("invalid result number")
        last = self.last_round_id()
        if last is not None and round_id <= last:
            return None
        misses = self.misses()
        if last is not None and round_id > last + 1:
            misses = {key: None for key in self.config.groups}
        updated, triggered = advance(misses, number, self.config.groups)
        now = datetime.now(timezone.utc).isoformat()
        with self.db:
            self.db.execute("INSERT INTO rounds VALUES (?, ?, ?, ?, ?)",
                            (round_id, number, ended_at.isoformat() if ended_at else None, now, source))
            self.db.execute("""INSERT INTO meta VALUES ('last_round_id', ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value""", (str(round_id),))
            for key, value in updated.items():
                self.db.execute("UPDATE misses SET value=? WHERE group_key=?", (value, key))
            recent = self.recent_numbers()
            subscribers = self.db.execute("SELECT chat_id FROM subscribers WHERE active=1").fetchall()
            for key in triggered if notify else []:
                group = self.config.groups[key]
                message = signal_text(group, updated[key], recent)
                for subscriber in subscribers:
                    self.db.execute("""INSERT OR IGNORE INTO notifications
                        (round_id, group_key, chat_id, text, status) VALUES (?, ?, ?, ?, 'pending')""",
                        (round_id, key, subscriber[0], message))
        return updated, triggered

    def recover_history(self, latest_id: int, history: list[int]) -> int:
        """Backfill missed IDs only when the saved round anchors the WS history."""
        if not history or any(type(n) is not int or not 0 <= n <= 36 for n in history):
            return 0
        last = self.last_round_id()
        if last is None or last > latest_id:
            return 0
        offset = latest_id - last
        if offset >= len(history):
            return 0
        anchor = self.db.execute("SELECT number FROM rounds WHERE round_id=?", (last,)).fetchone()
        if not anchor or anchor[0] != history[offset]:
            return 0
        recovered = 0
        for round_id in range(last + 1, latest_id + 1):
            number = history[latest_id - round_id]
            if self.apply_round(round_id, number, None, source="history", notify=False):
                recovered += 1
        # A prior gap may have left some counters unknown. The latest hit in
        # the history gives an exact streak length for those groups.
        current = self.misses()
        with self.db:
            for key, value in current.items():
                if value is None:
                    streak = next((i for i, number in enumerate(history)
                                   if number in self.config.groups[key].numbers), None)
                    if streak is not None:
                        self.db.execute("UPDATE misses SET value=? WHERE group_key=?", (streak, key))
        return recovered

    def claim_next(self) -> sqlite3.Row | None:
        """Claim before sending: a restart cannot resend an attempted notification."""
        with self.db:
            row = self.db.execute("""SELECT n.* FROM notifications n
                JOIN subscribers s ON s.chat_id=n.chat_id
                WHERE n.status='pending' AND s.active=1
                ORDER BY n.round_id, n.group_key, n.chat_id LIMIT 1""").fetchone()
            if row:
                self.db.execute("""UPDATE notifications SET status='attempted', attempted_at=?
                    WHERE round_id=? AND group_key=? AND chat_id=? AND status='pending'""",
                    (datetime.now(timezone.utc).isoformat(), row["round_id"], row["group_key"], row["chat_id"]))
            return row

    def mark_delivery(self, row: sqlite3.Row, success: bool, error: str | None = None):
        with self.db:
            self.db.execute("""UPDATE notifications SET status=?, sent_at=?, error=?
                WHERE round_id=? AND group_key=? AND chat_id=?""",
                ("sent" if success else "failed",
                 datetime.now(timezone.utc).isoformat() if success else None,
                 error, row["round_id"], row["group_key"], row["chat_id"]))
