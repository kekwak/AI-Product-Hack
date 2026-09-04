import os
import threading
from typing import Iterable

from langchain_openrouter import ChatOpenRouter

from run_inference import FULL_PROMPT, Findings

ALL_IDS = [*(f"D{i:02d}" for i in range(1, 9)), "T01", *(f"U{i:02d}" for i in range(1, 20))]
_review_slot = threading.Lock()


class ReviewError(RuntimeError):
    pass


class ReviewBusyError(ReviewError):
    pass


def run_review(
    document: str,
    model: str,
    enabled_families: Iterable[str],
    cap: int,
    max_output_tokens: int = 2048,
) -> list[dict]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ReviewError("Переменная окружения OPENROUTER_API_KEY не задана.")

    families = set(enabled_families)
    enabled_ids = [item for item in ALL_IDS if item[0] in families]
    if not enabled_ids:
        return []
    filter_prompt = (
        "\n\n# НАСТРОЙКИ ЭТОЙ ПРОВЕРКИ\n"
        f"Проверяй и возвращай только следующие критерии: {', '.join(enabled_ids)}. "
        "Остальные критерии отключены пользователем и не должны попадать в errors. "
        f"Верни не более {cap} наиболее значимых замечаний. Если кандидатов больше, "
        "выбери те, которые сильнее всего влияют на однозначность реализации и тестирования. "
        "Пиши title и problem кратко, чтобы полный JSON гарантированно поместился в ответ."
    )
    if not _review_slot.acquire(blocking=False):
        raise ReviewBusyError("Другой документ уже проверяется. Дождитесь завершения текущей проверки.")
    try:
        chat = ChatOpenRouter(
            model=model, api_key=api_key, temperature=0, max_tokens=max_output_tokens,
            # A retry can overlap with a request still running at the provider
            # and make OpenRouter reserve the account balance twice.
            max_retries=0, timeout=120, reasoning={"effort": "high", "exclude": True},
            openrouter_provider={"require_parameters": True},
        )
        structured = chat.with_structured_output(Findings, method="json_schema", strict=True, include_raw=True)
        response = structured.invoke([
            ("system", FULL_PROMPT + filter_prompt),
            ("human", f"# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```markdown\n{document}\n```"),
        ])
        parsed = response.get("parsed") if isinstance(response, dict) else response
        if parsed is None:
            parsing_error = response.get("parsing_error") if isinstance(response, dict) else None
            detail = str(parsing_error).splitlines()[0] if parsing_error else ""
            if "EOF" in detail or "unterminated" in detail.lower() or "json" in detail.lower():
                raise ReviewError(
                    "Ответ модели оборвался или содержит некорректный JSON. Уменьшите максимум "
                    "замечаний в админке либо увеличьте лимит токенов ответа."
                )
            raise ReviewError(
                "Выбранная модель не смогла вернуть структурированный JSON. "
                "Повторите запрос или выберите другую модель в админке."
            )
        raw = [item.model_dump() for item in parsed.errors]
    except ReviewBusyError:
        raise
    except Exception as exc:
        message = str(exc).splitlines()[0]
        if "in-flight requests" in message:
            message = (
                "OpenRouter ещё обрабатывает предыдущий запрос и временно зарезервировал "
                "доступный баланс. Подождите 30–60 секунд и повторите проверку."
            )
        elif "requires more credits" in message or "exceed your available credits" in message:
            message = (
                "Недостаточно кредитов OpenRouter для выбранной модели. Уменьшите лимит "
                "токенов в админке, выберите более дешёвую модель или пополните баланс."
            )
        raise ReviewError(message) from exc
    finally:
        _review_slot.release()

    # Сервер повторно применяет фильтры: инструкциям модели нельзя доверять как ACL.
    valid = []
    for item in raw:
        error_id = item.get("error_type_id", "")
        quote = item.get("evidence_quote", "")
        if error_id in enabled_ids and quote and quote in document:
            item["family"] = error_id[0]
            valid.append(item)
    return valid[:cap]
