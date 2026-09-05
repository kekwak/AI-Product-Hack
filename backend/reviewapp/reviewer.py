import os
import re
import time
import httpx

from run_inference import FULL_PROMPT, Findings

ALL_IDS = [*(f"D{i:02d}" for i in range(1, 9)), "T01", *(f"U{i:02d}" for i in range(1, 20)), "OTHER"]


def family_of(error_id: str) -> str:
    return "OTHER" if error_id == "OTHER" else error_id[:1]


class ReviewError(RuntimeError):
    pass


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


def _invoke_schema(model: str, schema, messages: list[dict]):
    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": schema.__name__,
                "strict": True,
                "schema": schema.model_json_schema(),
            },
        },
    }
    def send(request_payload):
        return httpx.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {os.getenv('OPENROUTER_API_KEY')}",
                "Content-Type": "application/json",
            },
            json=request_payload,
            timeout=httpx.Timeout(120, connect=15),
        )

    try:
        for attempt in range(2):
            try:
                response = send(payload)
                break
            except httpx.ConnectError:
                if attempt:
                    raise
                time.sleep(1)
    except httpx.TimeoutException as exc:
        raise ReviewError("OpenRouter не ответил за 120 секунд. Повторите запрос или выберите другую модель.") from exc
    except httpx.ConnectError as exc:
        raise ReviewError(
            "Не удалось подключиться к OpenRouter: DNS или интернет-соединение временно недоступны. "
            "Проверьте сеть и повторите запрос."
        ) from exc
    if response.is_error:
        try:
            message = (response.json().get("error") or {}).get("message")
        except ValueError:
            message = None
        if response.status_code == 404 and message and "No endpoints found" in message:
            raise ReviewError(
                "Для выбранной модели OpenRouter не нашёл совместимого провайдера. "
                "Выберите другую модель."
            )
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
            "OpenRouter вернул пустой ответ или не сформировал structured output." + detail
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


def _run_candidate_review(document: str, model: str) -> list[dict]:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ReviewError("Переменная окружения OPENROUTER_API_KEY не задана.")

    enabled_ids = ALL_IDS
    try:
        parsed = _invoke_schema(model, Findings, [
            {"role": "system", "content": FULL_PROMPT},
            {"role": "user", "content": f"# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```markdown\n{document}\n```"},
        ])
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
                "Недостаточно кредитов OpenRouter для выбранной модели. Выберите более "
                "дешёвую модель или пополните баланс."
            )
        raise ReviewError(message) from exc
    # Keep only schema IDs and evidence that can be mapped back to the document.
    valid = []
    seen = set()
    for item in raw:
        error_id = item.get("error_type_id", "")
        quote = _resolve_evidence_quote(document, item.get("evidence_quote", ""))
        identity = (error_id, quote)
        if error_id in enabled_ids and quote and identity not in seen:
            seen.add(identity)
            item["evidence_quote"] = quote
            item["family"] = family_of(error_id)
            valid.append(item)
    valid.sort(key=lambda item: ALL_IDS.index(item["error_type_id"]))
    return valid


def run_review(document: str, model: str) -> list[dict]:
    return _run_candidate_review(document, model)
