"""Run Telegram long polling alongside the BetBoom WebSocket monitor."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from threading import Event, Thread

from aiogram import Bot, Dispatcher

from config import load_config, load_token
from instance_lock import single_instance
from monitor import stream_rounds
from storage import Storage
from telegram_bot import deliver_pending, make_router


log = logging.getLogger(__name__)


@dataclass
class RuntimeStatus:
    ws_status: str = "disconnected"


async def run() -> None:
    config = load_config()
    storage = Storage(config.database, config)
    status = RuntimeStatus()
    queue: asyncio.Queue[tuple[str, tuple]] = asyncio.Queue()
    loop = asyncio.get_running_loop()
    stop = Event()
    bot = Bot(token=load_token())
    dispatcher = Dispatcher()
    dispatcher.include_router(make_router(storage, config, status))

    def publish(kind: str, result: tuple) -> None:
        loop.call_soon_threadsafe(queue.put_nowait, (kind, result))

    def set_status(value: str) -> None:
        loop.call_soon_threadsafe(setattr, status, "ws_status", value)

    async def consume() -> None:
        await deliver_pending(storage, bot)
        while True:
            kind, payload = await queue.get()
            round_id, number, ended_at = payload[:3]
            try:
                if kind == "snapshot":
                    history = payload[3]
                    if storage.set_baseline(round_id):
                        log.info("Initial snapshot round=%s; waiting for live result", round_id)
                        continue
                    recovered = storage.recover_history(round_id, history)
                    if recovered:
                        log.info("Recovered %s rounds from WebSocket history through round=%s",
                                 recovered, round_id)
                    if number is None:
                        continue
                result = storage.apply_round(round_id, number, ended_at, kind)
                if result is None:
                    continue
                misses, triggered = result
                latency = ((datetime.now(timezone.utc) - ended_at).total_seconds()
                           if ended_at else None)
                log.info("round=%s number=%s source=%s latency=%s misses=%s",
                         round_id, number, kind,
                         f"{latency:.3f}s" if latency is not None else "unknown", misses)
                for key in triggered:
                    log.info("SIGNAL group=%s round=%s misses=%s", key, round_id, misses[key])
                await deliver_pending(storage, bot)
            except Exception:
                log.exception("Failed to process round=%s", round_id)
            finally:
                queue.task_done()

    consumer = asyncio.create_task(consume())
    monitor = Thread(target=stream_rounds, args=(config, publish, set_status, stop), daemon=True)
    try:
        me = await bot.get_me()
        log.info("Telegram connected bot=@%s", me.username)
        await bot.delete_webhook(drop_pending_updates=False)
        monitor.start()
        log.info("Express37 started")
        await dispatcher.start_polling(bot, handle_signals=False, allowed_updates=["message"])
    finally:
        stop.set()
        if monitor.is_alive():
            await asyncio.to_thread(monitor.join, 3)
        consumer.cancel()
        try:
            await consumer
        except asyncio.CancelledError:
            pass
        await bot.session.close()
        storage.close()
        log.info("Express37 stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        config = load_config()
        with single_instance(config.database.with_suffix(config.database.suffix + ".lock")):
            asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
