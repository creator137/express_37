"""Console monitor for BetBoom Express 37 results."""

from __future__ import annotations

import argparse
from collections import deque
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
from threading import Event
import time
from typing import Callable

from websockets.sync.client import connect
from websockets.exceptions import WebSocketException

from config import Config


GROUPS = {
    "G1": set(range(1, 13)),
    "G2": set(range(13, 25)),
    "G3": set(range(25, 37)),
    "G4": {13, 14, 16, 17, 18, 19, 20, 21, 22, 23},
    "G5": {25, 26, 28, 29, 30, 31, 32, 33, 34, 35},
}


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Monitor:
    def __init__(self, threshold: int) -> None:
        self.threshold = threshold
        self.misses: dict[str, int | None] = {name: 0 for name in GROUPS}
        self.last_numbers: deque[int] = deque(maxlen=max(15, threshold))
        self.last_round_id: int | None = None

    def accept(self, round_id: int, number: int, ended_at: datetime | None, source: str = "live") -> None:
        if self.last_round_id is not None and round_id <= self.last_round_id:
            return
        if not 0 <= number <= 36:
            raise ValueError(f"unexpected result {number!r} for round {round_id}")

        if self.last_round_id is not None and round_id > self.last_round_id + 1:
            print(f"WARNING: round gap {self.last_round_id} -> {round_id}; counters unknown until each group appears", flush=True)
            self.misses = {name: None for name in GROUPS}

        self.last_round_id = round_id
        self.last_numbers.append(number)
        received_at = datetime.now(timezone.utc)
        latency = (received_at - ended_at).total_seconds() if ended_at else None
        lag_text = f" latency={latency:.3f}s" if latency is not None else " latency=unknown"
        print(f"{received_at.isoformat(timespec='milliseconds')} round={round_id} number={number} source={source}{lag_text}", flush=True)

        for name, members in GROUPS.items():
            if number in members:
                self.misses[name] = 0
            elif self.misses[name] is not None:
                self.misses[name] += 1
                if self.misses[name] == self.threshold:
                    recent = list(self.last_numbers)[-self.threshold:]
                    print(f"SIGNAL: {name}, misses={self.threshold}, last_numbers={recent}", flush=True)


def completed_round(message: dict) -> tuple[int, int, datetime | None] | None:
    """Use the final start_expected state, which includes the round end time."""
    if message.get("type") != "state":
        return None
    state_message = message.get("state", {})
    if state_message.get("status") != "OK":
        return None
    state = state_message.get("state") or {}
    if state_message.get("action") != "start_expected" or state.get("rand_stage") != "start_expected":
        return None
    values = state.get("rand_values") or []
    if len(values) != 1:
        return None
    return int(state["game_id"]), int(values[0]), parse_time(state.get("dttm_end"))


def stream_rounds(config: Config,
                  publish: Callable[[str, tuple], None],
                  status: Callable[[str], None], stop: Event) -> None:
    """Read the existing BetBoom subscription protocol in a background thread."""
    log = logging.getLogger(__name__)
    attempt = 0
    while not stop.is_set():
        try:
            with connect(config.websocket_url, open_timeout=10,
                         ping_interval=15, ping_timeout=10) as ws:
                status("connected")
                log.info("WebSocket connected")
                ws.send(json.dumps({"type": "subscribe_game", "subscribe_game": {
                    "game_kind": config.game_kind, "uid": "1"
                }}))
                while not stop.is_set():
                    try:
                        raw = ws.recv(timeout=1)
                    except TimeoutError:
                        continue
                    message = json.loads(raw)
                    if message.get("type") == "subscribe_game":
                        payload = message.get("subscribe_game", {})
                        if payload.get("status") != "OK":
                            raise RuntimeError(f"subscription failed: {payload}")
                        attempt = 0
                        snapshot = payload.get("current_state", {})
                        state = snapshot.get("state") or {}
                        history = payload.get("history") or []
                        if snapshot.get("action") == "start_expected" and len(state.get("rand_values") or []) == 1:
                            publish("snapshot", (int(state["game_id"]),
                                int(state["rand_values"][0]), parse_time(state.get("dttm_end")), history))
                        elif snapshot.get("action") == "start" and history:
                            publish("snapshot", (int(state["game_id"]) - 1,
                                None, None, history))
                        continue
                    result = completed_round(message)
                    if result is not None:
                        publish("live", result)
        except (OSError, ConnectionError, TimeoutError, ValueError, KeyError,
                RuntimeError, WebSocketException) as exc:
            log.warning("WebSocket disconnected: %s", exc)
        if not stop.is_set():
            status("disconnected")
            delay = config.reconnect_backoff_seconds[min(attempt, len(config.reconnect_backoff_seconds) - 1)]
            attempt += 1
            log.info("WebSocket reconnect in %s seconds", delay)
            stop.wait(delay)


def run(config: dict) -> None:
    if "miss_threshold" in config:
        threshold = config["miss_threshold"]
    else:
        threshold = max(item["threshold"] for item in config["groups"].values())
    if not isinstance(threshold, int) or isinstance(threshold, bool) or threshold < 1:
        raise ValueError("miss_threshold must be a positive integer")
    monitor = Monitor(threshold)
    url = config["websocket_url"]
    game_kind = config["game_kind"]
    reconnect_seconds = config.get("reconnect_seconds", config.get("reconnect_backoff_seconds", [2])[0])

    while True:
        try:
            with connect(url, open_timeout=10, ping_interval=15, ping_timeout=10) as ws:
                print("Connected to BetBoom Express 37 WebSocket", flush=True)
                # The public demo client sends login with an empty token, but
                # the server rejects it. Subscription itself needs no token.
                ws.send(json.dumps({"type": "subscribe_game", "subscribe_game": {
                    "game_kind": game_kind, "uid": "1"
                }}))
                for raw in ws:
                    message = json.loads(raw)
                    if message.get("type") == "subscribe_game":
                        payload = message.get("subscribe_game", {})
                        if payload.get("status") != "OK":
                            raise RuntimeError(f"subscription failed: {payload}")
                        snapshot = payload.get("current_state", {})
                        state = snapshot.get("state") or {}
                        if snapshot.get("action") == "start_expected" and state.get("rand_values"):
                            snapshot_id = int(state["game_id"])
                            if monitor.last_round_id is None:
                                monitor.last_round_id = snapshot_id
                                print(f"Snapshot round={snapshot_id}; waiting for a new result", flush=True)
                            elif snapshot_id > monitor.last_round_id:
                                monitor.accept(snapshot_id, int(state["rand_values"][0]), parse_time(state.get("dttm_end")), source="snapshot")
                        continue
                    result = completed_round(message)
                    if result is not None:
                        monitor.accept(*result)
        except KeyboardInterrupt:
            raise
        except (OSError, ConnectionError, TimeoutError, ValueError, KeyError, RuntimeError, WebSocketException) as exc:
            print(f"Connection/problem: {exc}; reconnecting in {reconnect_seconds}s", flush=True)
            time.sleep(reconnect_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).with_name("config.json"))
    args = parser.parse_args()
    config = json.loads(args.config.read_text(encoding="utf-8"))
    try:
        run(config)
    except KeyboardInterrupt:
        print("Stopped")


if __name__ == "__main__":
    main()
