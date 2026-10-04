"""Behavior checks for groups, persistence, subscriptions and delivery."""

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

from config import Group, load_config
from instance_lock import single_instance
from signals import advance
from storage import Storage
from telegram_bot import deliver_pending, make_router
import pytest


def small_config(tmp_path):
    base = load_config()
    groups = dict(base.groups)
    groups["G1"] = replace(groups["G1"], threshold=2)
    return replace(base, database=tmp_path / "state.sqlite3", groups=groups)


def test_all_groups_and_zero(tmp_path):
    config = small_config(tmp_path)
    assert set(config.groups) == {"G1", "G2", "G3", "G4", "G5"}
    assert config.groups["G1"].numbers == frozenset(range(1, 13))
    assert config.groups["G2"].numbers == frozenset(range(13, 25))
    assert config.groups["G3"].numbers == frozenset(range(25, 37))
    assert config.groups["G4"].numbers == {13, 14, 16, 17, 18, 19, 20, 21, 22, 23}
    assert config.groups["G5"].numbers == {25, 26, 28, 29, 30, 31, 32, 33, 34, 35}
    misses = {key: 0 for key in config.groups}
    misses, _ = advance(misses, 13, config.groups)
    assert misses == {"G1": 1, "G2": 0, "G3": 1, "G4": 0, "G5": 1}
    misses, triggered = advance(misses, 0, config.groups)
    assert misses == {"G1": 2, "G2": 1, "G3": 2, "G4": 1, "G5": 2}
    assert triggered == ["G1"]


def test_threshold_reset_and_no_repeat(tmp_path):
    config = small_config(tmp_path)
    misses = {key: 0 for key in config.groups}
    signals = []
    for number in [13, 25, 0, 0, 1, 13, 25]:
        misses, triggered = advance(misses, number, config.groups)
        signals.extend(triggered)
    assert signals.count("G1") == 2
    assert misses["G1"] == 2


def test_sqlite_restores_and_deduplicates(tmp_path):
    config = small_config(tmp_path)
    first = Storage(config.database, config)
    first.subscribe(111)
    first.subscribe(111)
    assert first.set_baseline(9)
    assert not first.set_baseline(9)
    assert first.apply_round(10, 13, None)
    result = first.apply_round(11, 25, datetime.now(timezone.utc))
    assert result[1] == ["G1"]
    assert first.apply_round(11, 25, None) is None
    assert first.last_round_id() == 11
    assert first.recent_numbers() == [13, 25]
    first.close()

    restored = Storage(config.database, config)
    assert restored.is_subscribed(111)
    assert restored.misses()["G1"] == 2
    assert restored.last_round_id() == 11
    assert restored.apply_round(11, 25, None) is None
    assert restored.db.execute("SELECT count(*) FROM notifications").fetchone()[0] == 1
    restored.close()


def test_gap_marks_counters_unknown_until_hit(tmp_path):
    config = small_config(tmp_path)
    store = Storage(config.database, config)
    store.apply_round(1, 1, None)
    store.apply_round(4, 0, None)
    assert all(value is None for value in store.misses().values())
    store.apply_round(5, 13, None)
    assert store.misses()["G2"] == 0
    assert store.misses()["G4"] == 0
    assert store.misses()["G1"] is None
    store.close()


def test_history_recovers_missed_rounds_without_old_notifications(tmp_path):
    config = small_config(tmp_path)
    store = Storage(config.database, config)
    store.subscribe(111)
    store.apply_round(10, 13, None)
    assert store.recover_history(13, [27, 26, 25, 13]) == 3
    assert store.last_round_id() == 13
    assert store.recent_numbers() == [13, 25, 26, 27]
    assert store.misses()["G1"] == 4
    assert store.db.execute("SELECT count(1) FROM notifications").fetchone()[0] == 0
    assert store.recover_history(13, [27, 26, 25, 13]) == 0
    store.close()


def test_history_requires_matching_anchor(tmp_path):
    config = small_config(tmp_path)
    store = Storage(config.database, config)
    store.apply_round(10, 13, None)
    assert store.recover_history(12, [25, 26, 99]) == 0
    assert store.recover_history(12, [25, 26, 12]) == 0
    assert store.last_round_id() == 10
    store.close()


def test_start_stop_status_and_real_delivery_path(tmp_path):
    config = small_config(tmp_path)
    store = Storage(config.database, config)
    status = SimpleNamespace(ws_status="connected")
    router = make_router(store, config, status)

    class FakeMessage:
        chat = SimpleNamespace(id=111)
        answers = []

        async def answer(self, text):
            self.answers.append(text)

    class FakeBot:
        messages = []

        async def send_message(self, chat_id, text):
            self.messages.append((chat_id, text))

    message = FakeMessage()
    bot = FakeBot()
    async def scenario():
        await router.message.handlers[0].callback(message)  # /start
        await router.message.handlers[0].callback(message)  # repeated /start
        assert store.db.execute("SELECT count(*) FROM subscribers").fetchone()[0] == 1
        assert "Express37 Signals" in message.answers[-1]
        store.apply_round(1, 13, None)
        store.apply_round(2, 25, None)
        await deliver_pending(store, bot)
        await deliver_pending(store, bot)
        assert len(bot.messages) == 1
        assert "1-я дюжина" in bot.messages[0][1]
        assert store.db.execute("SELECT status FROM notifications").fetchone()[0] == "sent"
        await router.message.handlers[2].callback(message)  # /status
        assert "WebSocket: connected" in message.answers[-1]
        assert "13, 25" in message.answers[-1]
        await router.message.handlers[1].callback(message)  # /stop
        assert not store.is_subscribed(111)
    asyncio.run(scenario())
    store.close()


def test_second_instance_is_rejected(tmp_path):
    path = tmp_path / "app.lock"
    with single_instance(path):
        with pytest.raises(RuntimeError, match="already running"):
            with single_instance(path):
                pass
    with single_instance(path):
        pass
