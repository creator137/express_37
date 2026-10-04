"""Pure miss-counter and notification formatting logic."""

from config import Group


def advance(misses: dict[str, int | None], number: int,
            groups: dict[str, Group]) -> tuple[dict[str, int | None], list[str]]:
    if type(number) is not int or not 0 <= number <= 36:
        raise ValueError(f"invalid result: {number!r}")
    updated: dict[str, int | None] = {}
    triggered: list[str] = []
    for key, group in groups.items():
        previous = misses[key]
        if number in group.numbers:
            updated[key] = 0
        elif previous is None:
            updated[key] = None
        else:
            updated[key] = previous + 1
            if updated[key] == group.threshold:
                triggered.append(key)
    return updated, triggered


def signal_text(group: Group, misses: int, recent: list[int]) -> str:
    return ("🚨 СИГНАЛ ДЛЯ ВХОДА!\n\n"
            f"Сектор: {group.name}\n"
            f"Не выпадает уже: {misses} раундов подряд!\n"
            f"Последние числа: {', '.join(map(str, recent))}")
