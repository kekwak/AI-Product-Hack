import html
import re

import bleach
import markdown


def _marked_source(document: str, findings: list[dict], escape: bool) -> str:
    """Insert stable mark elements around uniquely identifiable evidence."""
    spans = []
    for index, item in enumerate(findings):
        quote = item.get("evidence_quote", "")
        if quote and quote in document:
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
    # Highlight both views in the browser from the same normalized term list.
    # Marking a whole raw quote here made source and rendered Markdown disagree:
    # Markdown turns a table row into several independent text nodes.
    return html.escape(document)


def item_family(item: dict) -> str:
    return item.get("family") or item.get("error_type_id", "")[:1]


def highlight_terms(evidence: str) -> list[str]:
    """Return source snippets that survive Markdown rendering as text nodes."""
    terms: list[str] = []
    for raw_line in evidence.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("```") or re.fullmatch(r"\|?[\s:|-]+\|?", line):
            continue
        is_table_row = "|" in line
        if is_table_row:
            candidates = [cell.strip() for cell in line.strip("|").split("|")]
        else:
            candidates = [re.sub(r"^(?:#{1,6}|[-*+] |\d+[.)] )\s*", "", line)]
        for candidate in candidates:
            # Inline code and links become separate DOM text nodes after
            # Markdown rendering, so search for each visible part separately.
            segments = re.split(r"(`[^`]+`|\[[^]]+]\([^)]+\))", candidate)
            for segment in segments:
                if not segment:
                    continue
                link = re.fullmatch(r"\[([^]]+)]\(([^)]+)\)", segment)
                matchable_parts = [link.group(1), link.group(2)] if link else [segment]
                for part in matchable_parts:
                    normalized = " ".join(part.replace("`", "").strip("*~ ").split())
                    # Standalone numbers and ellipsis placeholders have no useful
                    # context. Matching them globally highlights unrelated values
                    # (for example `100` inside `100 000`) before the actual row.
                    if (
                        (len(normalized) >= 3 or (is_table_row and normalized == "-"))
                        and normalized not in {"...", "…"}
                        and (normalized == "-" or not re.fullmatch(r"[\d\s.,:+-]+", normalized))
                    ):
                        # Do not deduplicate: repeated table cells must highlight
                        # the same number of occurrences as in the evidence quote.
                        terms.append(normalized)
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


def rendered_markdown(document: str, findings: list[dict]) -> str:
    # Render pristine Markdown. Markers inserted before parsing can split table
    # delimiters and produce malformed rows with enormous cells.
    rendered = markdown.markdown(
        _normalize_loose_markdown(document),
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
