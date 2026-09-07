import os
import re
import time
from difflib import SequenceMatcher

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


def _resolve_evidence_quote(document: str, quote: str) -> str | None:
    """Map harmless whitespace changes back to one exact source substring."""
    if quote and document.count(quote) == 1:
        return quote
    pieces = [re.escape(piece) for piece in re.split(r"\s+", quote.strip()) if piece]
    if not pieces:
        return None
    matches = list(re.finditer(r"\s+".join(pieces), document, flags=re.MULTILINE))
    if len(matches) == 1:
        return matches[0].group(0)
    if len(matches) > 1:
        return None

    quote_lines = [line for line in quote.splitlines() if line.strip()]
    document_lines = list(re.finditer(r"[^\n]+(?:\n|$)", document))
    if not quote_lines or not document_lines:
        return None
    normalized_quote = " ".join(quote.split()).casefold()
    expected_lines = len(quote_lines)
    best_score, best_candidate = 0.0, ""
    for start in range(len(document_lines)):
        for line_count in range(max(1, expected_lines - 2), expected_lines + 3):
            end_index = min(start + line_count, len(document_lines))
            start_offset = document_lines[start].start()
            end_offset = document_lines[end_index - 1].end()
            candidate = document[start_offset:end_offset].strip()
            score = SequenceMatcher(
                None, normalized_quote, " ".join(candidate.split()).casefold()
            ).ratio()
            if score > best_score:
                best_score, best_candidate = score, candidate
    return best_candidate if best_score >= 0.88 else None


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
            results = [item.model_dump() for item in parsed.errors]
            for result in results:
                resolved_quote = _resolve_evidence_quote(document, result.get("evidence_quote", ""))
                if resolved_quote is not None:
                    result["evidence_quote"] = resolved_quote
            return results
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
