import html
import re

import bleach
import markdown


def _marked_source(document: str, findings: list[dict], escape: bool) -> str:
    """Insert stable mark elements around uniquely identifiable evidence."""
    spans = []
    for index, item in enumerate(findings):
        quote = item.get("evidence_quote", "")
        if quote and document.count(quote) == 1:
            spans.append((document.index(quote), index, quote))
    spans.sort()
    result, cursor = [], 0
    encode = html.escape if escape else lambda value: value
    for start, index, quote in spans:
        if start < cursor:
            continue
        result.append(encode(document[cursor:start]))
        end = start + len(quote)
        family = html.escape(item_family(findings[index]), quote=True)
        result.append(f'<mark data-index="{index}" data-family="{family}">{encode(document[start:end])}</mark>')
        cursor = end
    result.append(encode(document[cursor:]))
    return "".join(result)


def highlighted_source(document: str, findings: list[dict]) -> str:
    return _marked_source(document, findings, escape=True)


def item_family(item: dict) -> str:
    return item.get("family") or item.get("error_type_id", "")[:1]


def highlight_terms(evidence: str) -> list[str]:
    """Return source snippets that survive Markdown rendering as text nodes."""
    terms: list[str] = []
    for raw_line in evidence.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("```") or re.fullmatch(r"\|?[\s:|-]+\|?", line):
            continue
        if "|" in line:
            candidates = [cell.strip() for cell in line.strip("|").split("|")]
        else:
            candidates = [re.sub(r"^(?:#{1,6}|[-*+] |\d+[.)] )\s*", "", line)]
        for candidate in candidates:
            candidate = re.sub(r"\[([^]]+)]\([^)]+\)", r"\1", candidate)
            candidate = candidate.replace("`", "").strip("*~ ")
            if len(candidate) >= 3 and candidate not in terms:
                terms.append(candidate)
    return terms


def _normalize_loose_markdown(document: str) -> str:
    """Separate list blocks when authors omit Markdown's optional blank lines."""
    lines = document.splitlines()
    normalized: list[str] = []
    in_fence = False

    def is_list_item(value: str) -> bool:
        return bool(re.match(r"^\s*(?:[-+*]|\d+[.)])\s+\S", value))

    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_fence = not in_fence
            normalized.append(line)
            continue
        if not in_fence and normalized and line.strip() and normalized[-1].strip():
            current_is_list = is_list_item(line)
            previous_is_list = is_list_item(normalized[-1])
            if current_is_list != previous_is_list:
                normalized.append("")
        normalized.append(line)
    return "\n".join(normalized)


def _render_input_with_markers(document: str, findings: list[dict]) -> str:
    """Place inert markers around exact evidence after Markdown normalization."""
    normalized_document = _normalize_loose_markdown(document)
    spans: list[tuple[int, int, int]] = []
    for index, item in enumerate(findings):
        quote = item.get("evidence_quote", "")
        if not quote:
            continue
        normalized_quote = _normalize_loose_markdown(quote)
        if normalized_document.count(normalized_quote) == 1:
            start = normalized_document.index(normalized_quote)
            spans.append((start, start + len(normalized_quote), index))

    non_overlapping: list[tuple[int, int, int]] = []
    cursor = 0
    for span in sorted(spans):
        if span[0] >= cursor:
            non_overlapping.append(span)
            cursor = span[1]

    for start, end, index in reversed(non_overlapping):
        normalized_document = (
            normalized_document[:start]
            + f"\ue000R{index}S\ue001"
            + normalized_document[start:end]
            + f"\ue000R{index}E\ue001"
            + normalized_document[end:]
        )
    return normalized_document


def rendered_markdown(document: str, findings: list[dict]) -> str:
    # Highlighting is applied to the resulting DOM in the browser so mark tags
    # cannot break tables, fenced code blocks, lists, or other Markdown syntax.
    rendered = markdown.markdown(
        _render_input_with_markers(document, findings),
        extensions=["tables", "fenced_code", "sane_lists"],
    )
    tags = set(bleach.sanitizer.ALLOWED_TAGS) | {
        "p", "pre", "h1", "h2", "h3", "h4", "h5", "h6", "hr", "br",
        "table", "thead", "tbody", "tr", "th", "td", "mark",
    }
    return bleach.clean(
        rendered,
        tags=tags,
        attributes={"a": ["href", "title"], "mark": ["data-index"]},
        protocols={"http", "https", "mailto"},
        strip=True,
    )
