"""aiogram handlers and signal delivery."""

import logging
from typing import Protocol

from aiogram import Bot, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from config import Config
from storage import Storage


log = logging.getLogger(__name__)


class Status(Protocol):
    ws_status: str


def make_router(storage: Storage, config: Config, status: Status) -> Router:
    router = Router()

    @router.message(CommandStart())
    async def start(message: Message):
        chat_id = message.chat.id
        storage.subscribe(chat_id)
        log.info("/start chat_id=%s", chat_id)
        thresholds = "\n".join(
            f"{group.name}: {group.threshold}" for group in config.groups.values())
        await message.answer(
            "✅ Express37 Signals запущен.\n\n"
            "Вы будете получать уведомления, когда один из секторов достигнет "
            "заданного количества раундов без выпадения.\n\n"
            f"Текущие пороги:\n{thresholds}\n\n"
            "/status — состояние, /stop — отключить сигналы."
        )

    @router.message(Command("stop"))
    async def stop(message: Message):
        storage.unsubscribe(message.chat.id)
        log.info("/stop chat_id=%s", message.chat.id)
        await message.answer("🔕 Сигналы отключены. Для повторной подписки отправьте /start.")

    @router.message(Command("status"))
    async def show_status(message: Message):
        recent = storage.recent_numbers(5)
        misses = storage.misses()
        counts = "\n".join(
            f"{group.name}: {misses[key] if misses[key] is not None else 'неизвестно'}"
            for key, group in config.groups.items())
        last_time = storage.last_round_time() or "ещё нет"
        subscribed = "да" if storage.is_subscribed(message.chat.id) else "нет"
        await message.answer(
            f"Подписка: {subscribed}\n"
            f"Последние 5 чисел: {', '.join(map(str, recent)) if recent else 'ещё нет'}\n"
            f"Счётчики:\n{counts}\n"
            f"WebSocket: {status.ws_status}\n"
            f"Последний раунд получен: {last_time} (UTC)"
        )

    return router


async def deliver_pending(storage: Storage, bot: Bot) -> None:
    """Send each queued signal once; attempted sends are never replayed on restart."""
    while row := storage.claim_next():
        try:
            await bot.send_message(row["chat_id"], row["text"])
        except Exception as exc:
            # Telegram/network exceptions can contain request details. Log only
            # their class name so the bot token never reaches application logs.
            storage.mark_delivery(row, False, type(exc).__name__)
            log.error("Telegram send failed chat_id=%s round=%s group=%s error=%s",
                      row["chat_id"], row["round_id"], row["group_key"], type(exc).__name__)
        else:
            storage.mark_delivery(row, True)
            log.info("Telegram sent chat_id=%s round=%s group=%s",
                     row["chat_id"], row["round_id"], row["group_key"])
