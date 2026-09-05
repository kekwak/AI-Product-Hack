import os
import json
import re
from typing import Iterable

import httpx
from pydantic import BaseModel, ConfigDict

from run_inference import FULL_PROMPT, Findings

ALL_IDS = [*(f"D{i:02d}" for i in range(1, 9)), "T01", *(f"U{i:02d}" for i in range(1, 20))]


class ReviewError(RuntimeError):
    pass


class JudgeItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    candidate_index: int
    reasoning: str
    recommendation: str


class JudgeResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accepted: list[JudgeItem]


def _raw_text(response) -> str:
    raw = response.get("raw") if isinstance(response, dict) else None
    content = getattr(raw, "content", raw)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return ""


def _recover_parsed_response(response, schema=Findings):
    """Recover valid JSON that a provider wrapped in prose or a code fence."""
    return _parse_schema_text(_raw_text(response), schema)


def _content_text(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            part.get("text", "") if isinstance(part, dict) else str(part)
            for part in content
        )
    return ""


def _parse_schema_text(text: str | None, schema):
    text = _content_text(text)
    if not text:
        return None
    text = text.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    candidate = fenced.group(1) if fenced else text[text.find("{"):text.rfind("}") + 1]
    if not candidate:
        return None
    try:
        return schema.model_validate_json(candidate)
    except Exception:
        return None


def _invoke_schema(model: str, schema, messages: list[dict], max_tokens: int, reasoning_effort: str):
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
        "reasoning": (
            {"enabled": False}
            if reasoning_effort == "low"
            else {"effort": reasoning_effort, "exclude": True}
        ),
        "provider": {"require_parameters": True},
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema.__name__,
                "strict": True,
                "schema": schema.model_json_schema(),
            },
        },
    }
    try:
        response = httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=httpx.Timeout(120, connect=15),
        )
    except httpx.TimeoutException as exc:
        raise ReviewError("OpenRouter не ответил за 120 секунд. Повторите запрос или выберите другую модель.") from exc
    if response.is_error:
        try:
            message = (response.json().get("error") or {}).get("message")
        except ValueError:
            message = None
        raise ReviewError(f"OpenRouter HTTP {response.status_code}: {message or response.reason_phrase}")
    data = response.json()
    try:
        message = data["choices"][0]["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise ReviewError("OpenRouter вернул ответ без текста модели.") from exc
    content = _content_text(message.get("content"))
    if not content:
        refusal = message.get("refusal")
        finish_reason = data.get("choices", [{}])[0].get("finish_reason")
        detail = f" Причина завершения: {finish_reason}." if finish_reason else ""
        if refusal:
            detail += f" Отказ модели: {refusal}"
        raise ReviewError(
            "OpenRouter вернул пустой ответ. Возможно, лимит токенов ушёл на reasoning или "
            "провайдер не сформировал structured output." + detail
        )
    parsed = _parse_schema_text(content, schema)
    if parsed is None:
        raise ReviewError("Ответ модели оборвался или содержит некорректный JSON.")
    return parsed


def _resolve_evidence_quote(document: str, quote: str) -> str | None:
    """Map harmless whitespace changes back to one exact source substring."""
    if quote and document.count(quote) == 1:
        return quote
    pieces = [re.escape(piece) for piece in re.split(r"\s+", quote.strip()) if piece]
    if not pieces:
        return None
    matches = list(re.finditer(r"\s+".join(pieces), document, flags=re.MULTILINE))
    if len(matches) != 1:
        return None
    return matches[0].group(0)


def _run_candidate_review(
    document: str,
    model: str,
    enabled_families: Iterable[str],
    cap: int,
    max_output_tokens: int = 2048,
    reasoning_effort: str = "low",
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
    try:
        parsed = _invoke_schema(model, Findings, [
            {"role": "system", "content": FULL_PROMPT + filter_prompt},
            {"role": "user", "content": f"# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```markdown\n{document}\n```"},
        ], max_output_tokens, reasoning_effort)
        raw = [item.model_dump() for item in parsed.errors]
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
    # Сервер повторно применяет фильтры: инструкциям модели нельзя доверять как ACL.
    valid = []
    seen = set()
    for item in raw:
        error_id = item.get("error_type_id", "")
        quote = _resolve_evidence_quote(document, item.get("evidence_quote", ""))
        identity = (error_id, quote)
        if error_id in enabled_ids and quote and identity not in seen:
            seen.add(identity)
            item["evidence_quote"] = quote
            item["family"] = error_id[0]
            valid.append(item)
    valid.sort(key=lambda item: ALL_IDS.index(item["error_type_id"]))
    return valid[:cap]


def _candidate_context(document: str, quote: str, radius: int = 240) -> str:
    start = document.find(quote)
    if start < 0:
        return quote
    return document[max(0, start - radius):min(len(document), start + len(quote) + radius)]


def _judge_candidates(
    document: str, candidates: list[dict], model: str, cap: int,
    max_output_tokens: int, reasoning_effort: str,
) -> list[dict]:
    if not candidates:
        return []
    payload = [{
        "candidate_index": index,
        **{key: item[key] for key in ("error_type_id", "evidence_quote", "title", "problem")},
        "context": _candidate_context(document, item["evidence_quote"]),
    } for index, item in enumerate(candidates)]
    prompt = (
        "Ты — финальный judge пре-ревью ТЗ. Проверь кандидатов, удали стилистические придирки, "
        "дубли одной корневой причины и неподтвержденные выводы. Приоритет — реальные дефекты, "
        "мешающие однозначной реализации или тестированию. Верни не более " + str(cap) +
        " принятых кандидатов. Для каждого дай краткое reasoning, почему это проблема, и "
        "конкретную рекомендацию аналитику, не переписывая документ автоматически. "
        "candidate_index обязан ссылаться на индекс из входного массива."
    )
    parsed = _invoke_schema(model, JudgeResult, [
        {"role": "system", "content": prompt},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ], max_output_tokens, reasoning_effort)
    result, seen = [], set()
    for decision in parsed.accepted:
        index = decision.candidate_index
        if index < 0 or index >= len(candidates) or index in seen:
            continue
        seen.add(index)
        item = dict(candidates[index])
        item["reasoning"] = decision.reasoning
        item["recommendation"] = decision.recommendation
        result.append(item)
    return result[:cap]


def run_review(
    document: str, model: str, enabled_families: Iterable[str], cap: int,
    max_output_tokens: int = 2048, reasoning_effort: str = "low",
    pipeline_mode: str = "fast",
) -> list[dict]:
    families = list(dict.fromkeys(enabled_families))
    if pipeline_mode != "quality":
        return _run_candidate_review(
            document, model, families, cap, max_output_tokens, reasoning_effort
        )

    candidates = []
    for family in families:
        candidates.extend(_run_candidate_review(
            document, model, [family], cap, max_output_tokens, reasoning_effort
        ))
    unique = {}
    for item in candidates:
        unique.setdefault((item["error_type_id"], item["evidence_quote"]), item)
    aggregated = sorted(unique.values(), key=lambda item: ALL_IDS.index(item["error_type_id"]))
    return _judge_candidates(
        document, aggregated, model, cap, max_output_tokens, reasoning_effort
    )
