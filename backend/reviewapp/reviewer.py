import os
import time

from langchain_openrouter import ChatOpenRouter
from run_inference import BASE_INSTRUCTIONS, Findings, build_prompt


class ReviewError(RuntimeError):
    pass


def _invoke_schema(
    model: str,
    schema,
    messages: list[tuple[str, str]],
    *,
    max_tokens: int = 65536,
    reasoning_effort: str = "high",
    no_reasoning: bool = False,
    temperature: float = 0.0,
    no_temperature: bool = False,
    provider: str = "",
):
    chat_options = {
        "model": model,
        "api_key": os.getenv("OPENROUTER_API_KEY"),
        "max_tokens": max_tokens,
        "max_retries": 0,
    }
    chat_options["reasoning"] = (
        {"enabled": False}
        if no_reasoning
        else {"effort": reasoning_effort, "exclude": True}
    )
    if not no_temperature:
        chat_options["temperature"] = temperature
    if provider:
        chat_options["openrouter_provider"] = {
            "only": [provider],
            "allow_fallbacks": False,
            "require_parameters": True,
        }

    chat = ChatOpenRouter(**chat_options)
    structured = chat.with_structured_output(
        schema, method="json_schema", strict=True, include_raw=True,
    )
    response = structured.invoke(messages)
    parsed = response.get("parsed")
    if not isinstance(parsed, schema):
        raise ValueError(response.get("parsing_error") or "модель вернула пустой ответ")
    return parsed


def _run_candidate_review(
    document: str,
    model: str,
    *,
    max_tokens: int = 65536,
    reasoning_effort: str = "high",
    no_reasoning: bool = False,
    temperature: float = 0.0,
    no_temperature: bool = False,
    provider: str = "",
    retries: int = 2,
) -> list[dict]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ReviewError("Переменная окружения OPENROUTER_API_KEY не задана.")

    messages = [
        ("system", build_prompt(BASE_INSTRUCTIONS)),
        ("human", f"# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```\n{document}\n```"),
    ]
    for attempt in range(retries + 1):
        try:
            parsed = _invoke_schema(
                model,
                Findings,
                messages,
                max_tokens=max_tokens,
                reasoning_effort=reasoning_effort,
                no_reasoning=no_reasoning,
                temperature=temperature,
                no_temperature=no_temperature,
                provider=provider,
            )
            return [item.model_dump() for item in parsed.errors]
        except Exception as exc:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            message = str(exc).splitlines()[0]
            if "in-flight requests" in message:
                message = (
                    "OpenRouter ещё обрабатывает предыдущий запрос и временно зарезервировал "
                    "доступный баланс. Подождите 30–60 секунд и повторите проверку."
                )
            elif "requires more credits" in message or "exceed your available credits" in message:
                message = (
                    "Недостаточно кредитов OpenRouter для выбранной модели. Выберите более "
                    "дешёвую модель или пополните баланс."
                )
            raise ReviewError(message) from exc


def run_review(document: str, model: str, **options) -> list[dict]:
    return _run_candidate_review(document, model, **options)
