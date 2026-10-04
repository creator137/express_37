"""Load and validate configuration without exposing the bot token in logs."""

from dataclasses import dataclass
import json
import os
from pathlib import Path


@dataclass(frozen=True)
class Group:
    key: str
    name: str
    numbers: frozenset[int]
    threshold: int


@dataclass(frozen=True)
class Config:
    websocket_url: str
    game_kind: int
    reconnect_backoff_seconds: tuple[float, ...]
    database: Path
    last_numbers_count: int
    groups: dict[str, Group]


def load_config(path: Path = Path(__file__).with_name("config.json")) -> Config:
    data = json.loads(path.read_text(encoding="utf-8"))
    groups: dict[str, Group] = {}
    for key, item in data["groups"].items():
        numbers = item["numbers"]
        threshold = item["threshold"]
        if not numbers or any(type(n) is not int or not 1 <= n <= 36 for n in numbers):
            raise ValueError(f"invalid numbers for {key}")
        if type(threshold) is not int or threshold < 1:
            raise ValueError(f"invalid threshold for {key}")
        groups[key] = Group(key, item["name"], frozenset(numbers), threshold)
    if not groups:
        raise ValueError("at least one group is required")
    count = data["last_numbers_count"]
    if type(count) is not int or count < 1:
        raise ValueError("last_numbers_count must be positive")
    backoff = tuple(data["reconnect_backoff_seconds"])
    if not backoff or any(type(n) not in (int, float) or n <= 0 for n in backoff):
        raise ValueError("reconnect_backoff_seconds must be positive")
    db = Path(data["database"])
    if not db.is_absolute():
        db = path.parent / db
    return Config(data["websocket_url"], data["game_kind"], backoff, db, count, groups)


def load_token(path: Path = Path(__file__).with_name(".env")) -> str:
    values: dict[str, str] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip('"').strip("'")
    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("BOT_TOKEN")
             or values.get("TELEGRAM_BOT_TOKEN") or values.get("BOT_TOKEN"))
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN or BOT_TOKEN is missing")
    return token
