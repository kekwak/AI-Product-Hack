import re
from dataclasses import dataclass, field

HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$")


_PLAIN_HEADING_MAX_LEN = 100
_PLAIN_HEADING_EXCLUDE_PREFIXES = ("|", "-", "*", "+", ">", "```", "~~~")
_PLAIN_HEADING_NUMBERED_RE = re.compile(r"^\d+[.)]\s")


@dataclass
class Heading:
    level: int
    title: str
    line: int
    implicit: bool = False


@dataclass
class RuleResult:
    rule_title: str
    rule_level: int | None
    is_required: bool
    matched: bool
    matched_heading: Heading | None = None


@dataclass
class CheckReport:
    headings: list[Heading] = field(default_factory=list)
    results: list[RuleResult] = field(default_factory=list)

    @property
    def missing_required(self) -> list[RuleResult]:
        return [r for r in self.results if r.is_required and not r.matched]

    @property
    def is_valid(self) -> bool:
        return len(self.missing_required) == 0


def _looks_like_plain_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped or len(stripped) > _PLAIN_HEADING_MAX_LEN:
        return False
    if stripped.startswith(_PLAIN_HEADING_EXCLUDE_PREFIXES):
        return False
    if _PLAIN_HEADING_NUMBERED_RE.match(stripped):
        return False
    if "://" in stripped:
        return False
    if "|" in stripped:
        return False
    return True


def extract_headings(markdown_text: str) -> list[Heading]:
    lines = markdown_text.splitlines()
    headings: list[Heading] = []
    in_code_fence = False

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            in_code_fence = not in_code_fence
            continue
        if in_code_fence:
            continue

        match = HEADING_RE.match(line)
        if match:
            level = len(match.group(1))
            title = match.group(2).strip()
            headings.append(Heading(level=level, title=title, line=i + 1))
            continue

        preceded_by_blank = i == 0 or lines[i - 1].strip() == ""
        if preceded_by_blank and _looks_like_plain_heading(line):
            headings.append(Heading(level=1, title=stripped, line=i + 1, implicit=True))

    return headings


def _normalize(text: str) -> str:
    return " ".join(text.strip().lower().split())


def check_document(markdown_text: str, rules) -> CheckReport:
    headings = extract_headings(markdown_text)
    report = CheckReport(headings=headings)

    for rule in rules:
        matched_heading = None
        for heading in headings:
            if rule.level is not None and heading.level != rule.level:
                continue
            if _normalize(heading.title) == _normalize(rule.title):
                matched_heading = heading
                break
        report.results.append(
            RuleResult(
                rule_title=rule.title,
                rule_level=rule.level,
                is_required=rule.is_required,
                matched=matched_heading is not None,
                matched_heading=matched_heading,
            )
        )

    return report


def _rule_level_label(level: int | None) -> str:
    return f"H{level}" if level else "любой уровень"


def build_annotated_markdown(original_text: str, report: CheckReport) -> str:
    """Return the document with an inline check report and per-line markers."""

    matched_lines: dict[int, list[bool]] = {}
    for result in report.results:
        if result.matched and result.matched_heading is not None:
            matched_lines.setdefault(result.matched_heading.line, []).append(result.is_required)

    lines = original_text.splitlines()
    annotated_lines = []
    for idx, line in enumerate(lines, start=1):
        if idx in matched_lines:
            marker = "✅" if any(matched_lines[idx]) else "ℹ️"
            annotated_lines.append(f"{line}  <!-- {marker} заголовок найден -->")
        else:
            annotated_lines.append(line)

    missing_required = [r for r in report.results if r.is_required and not r.matched]
    missing_optional = [r for r in report.results if not r.is_required and not r.matched]

    status = (
        "✅ Все обязательные заголовки найдены"
        if report.is_valid
        else "❌ Отсутствуют обязательные заголовки"
    )

    report_block = ["> ## Отчёт проверки формата", f"> **Статус:** {status}", ">"]

    if missing_required:
        report_block.append("> **❌ Отсутствуют обязательные заголовки:**")
        for r in missing_required:
            report_block.append(f"> - ❌ **{r.rule_title}** ({_rule_level_label(r.rule_level)})")
        report_block.append(">")

    if missing_optional:
        report_block.append("> **⚠️ Отсутствуют опциональные заголовки:**")
        for r in missing_optional:
            report_block.append(f"> - ⚠️ {r.rule_title} ({_rule_level_label(r.rule_level)})")
        report_block.append(">")

    if not missing_required and not missing_optional:
        report_block.append("> Все проверяемые заголовки на месте.")

    report_block += ["", "---", ""]

    return "\n".join(report_block) + "\n".join(annotated_lines) + "\n"
