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


def rendered_markdown(document: str, findings: list[dict]) -> str:
    # Highlighting is applied to the resulting DOM in the browser so mark tags
    # cannot break tables, fenced code blocks, lists, or other Markdown syntax.
    rendered = markdown.markdown(document, extensions=["tables", "fenced_code", "sane_lists"])
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
