#!/usr/bin/env python3
"""Structural and metadata checks for the generated hackathon dataset."""

from __future__ import annotations

import collections
import hashlib
import json
import re
import sys
from pathlib import Path

from paths import DATASET_DIR

ROOT = DATASET_DIR
CLEAN_DIR = ROOT / "clean"
CORRUPTED_DIR = ROOT / "corrupted"
CATALOG_PATH = ROOT / "error_catalog.json"

REQUIRED_HEADINGS = (
    "Потоковые данные/витрины",
    "Общие сведения",
    "Решаемая проблема",
    "Продуктовые метрики",
    "Заказчики",
    "Нефункциональные требования",
    "Системы-источники",
    "Data Catalog",
    "Исходники проекта",
    "Команда",
    "JIRA",
    "Источники данных",
    "Источники обогащения данных",
    "Приемники данных",
    "Схема потоков данных",
    "Алгоритм обработки потока",
    "Формирование ключа (kafka) / партиции (hdfs)",
    "Структура данных",
    "Пример данных",
    "DDL",
    "FAQ",
    "История изменений",
)

ERROR_FIELDS = {
    "instance_id",
    "mutation_id",
    "error_type_id",
    "source_type",
    "difficulty",
    "evidence_quote",
    "evidence_relation",
    "title",
    "problem",
    "roles",
    "priority",
    "clarification_needed",
    "affected_sections",
    "injected_change",
}

METADATA_FIELDS = {
    "schema_version", "document_id", "document_file", "clean_document_id",
    "clean_document_file", "generator_seed", "generator_version",
    "document_sha256", "clean_document_sha256", "expected_error_count",
    "counts_by_source_type", "counts_by_difficulty", "counts_by_error_type", "errors",
}

EXPECTED_ERROR_TYPES = {
    *(f"D{index:02d}" for index in range(1, 9)),
    "T01",
    *(f"U{index:02d}" for index in range(1, 20)),
}

EXPECTED_SOURCE_BY_ERROR_TYPE = {
    **{f"D{index:02d}": 1 for index in range(1, 9)},
    "T01": 2,
    **{f"U{index:02d}": 3 for index in range(1, 20)},
}


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_catalog() -> dict[str, dict]:
    rows = json.loads(CATALOG_PATH.read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("error catalog must be a list")
    result = {row["mutation_id"]: row for row in rows}
    if len(result) != len(rows):
        raise ValueError("duplicate mutation_id in error catalog")
    return result


def normalized_lines(text: str) -> list[str]:
    result: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"^#{1,6}\s+", "", raw_line.strip())
        if line.startswith("|"):
            first_cell = line.strip("|").split("|", 1)[0].strip()
            line = first_cell.strip("*").strip()
        result.append(line)
    return result


def locate_heading(lines: list[str], heading: str) -> int | None:
    for index, line in enumerate(lines):
        if line == heading or (heading == "Структура данных" and line.startswith(heading)):
            return index
    return None


def section(text: str, start: str, end: str) -> str:
    lines = text.splitlines()
    normalized = normalized_lines(text)
    start_index = locate_heading(normalized, start)
    end_index = locate_heading(normalized, end)
    if start_index is None or end_index is None or end_index <= start_index:
        return ""
    return "\n".join(lines[start_index + 1 : end_index])


def markdown_rows(block: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in block.splitlines():
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        cells = [cell.strip() for cell in re.split(r"(?<!\\)\|", stripped[1:-1])]
        if all(re.fullmatch(r":?-{3,}:?", cell) for cell in cells):
            continue
        rows.append(cells)
    return rows


def validate_clean(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    lines = normalized_lines(text)
    errors: list[str] = []

    for heading in REQUIRED_HEADINGS:
        if locate_heading(lines, heading) is None:
            errors.append(f"missing heading: {heading}")
    if not any(line.startswith("Шаг 1.") and "Фильтрац" in line for line in lines):
        errors.append("missing filter step")
    if not any(line.startswith("Шаг 2.") and "Обогащ" in line for line in lines):
        errors.append("missing enrichment step")
    if not any(line.startswith("Шаг 3.") for line in lines):
        errors.append("missing transformation step")

    forbidden = ("LINK_DASHBOARD", "LINK_GITLAB", "LINK_JIRA", "<Наименование", "Краткое описание", "Описание условий фильтрации")
    for token in forbidden:
        if token in text:
            errors.append(f"placeholder remains: {token}")

    if "кластер" not in text.lower() or "kafka" not in text.lower():
        errors.append("Kafka cluster is not explicit")
    receiver_block = section(text, "Приемники данных", "Схема потоков данных")
    has_file_storage = bool(re.search(r"\b(?:HDFS|Parquet|ORC|Iceberg)\b", receiver_block, re.IGNORECASE))
    if has_file_storage and not re.search(r"/(?:data|warehouse)/[A-Za-z0-9_./=-]+", text):
        errors.append("full HDFS-style path not found")
    if text.count("http://") + text.count("https://") < 4:
        errors.append("too few direct links")
    if "NOT NULL" not in text and "NULLABLE" not in text:
        errors.append("nullability contract incomplete")
    if not re.search(r"CREATE\s+(?:EXTERNAL\s+)?TABLE", text, re.IGNORECASE):
        errors.append("DDL statement not found")

    structure_rows = markdown_rows(section(text, "Структура данных", "Пример данных"))
    data_rows = structure_rows[2:] if len(structure_rows) >= 2 else []
    for row in data_rows:
        if len(row) >= 3 and "NOT NULL" not in row[2] and "NULLABLE" not in row[2]:
            errors.append(f"target field lacks nullability: {row[0]}")

    example_rows = markdown_rows(section(text, "Пример данных", "DDL"))
    example_data_rows = example_rows[1:] if example_rows else []
    if len(example_data_rows) != 10:
        errors.append(f"example must contain 10 rows, found {len(example_data_rows)}")

    target_fields = [row[0].strip("` ") for row in data_rows if row]
    example_fields = [cell.strip("` ") for cell in example_rows[0]] if example_rows else []
    if target_fields != example_fields:
        errors.append("structure fields and example columns differ")
    if len(target_fields) != len(set(field.lower() for field in target_fields)):
        errors.append("duplicate target fields in structure")

    ddl_block = section(text, "DDL", "FAQ")
    ddl_match = re.search(r"```sql\s*(.*?)```", ddl_block, re.IGNORECASE | re.DOTALL)
    if not ddl_match:
        errors.append("DDL SQL code block not found")
    else:
        ddl_lower = ddl_match.group(1).lower()
        for field in target_fields:
            if not re.search(rf"(?m)^\s*[`\"]?{re.escape(field.lower())}[`\"]?\s+", ddl_lower):
                errors.append(f"target field absent from DDL: {field}")

    return errors


def validate_corrupted(md_path: Path, json_path: Path, catalog: dict[str, dict]) -> list[str]:
    text = md_path.read_text(encoding="utf-8")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    errors: list[str] = []
    missing_metadata = METADATA_FIELDS - set(payload)
    if missing_metadata:
        errors.append(f"metadata missing fields: {sorted(missing_metadata)}")
    findings = payload.get("errors")
    if not isinstance(findings, list):
        return ["metadata.errors is not a list"]
    if payload.get("expected_error_count") != len(findings):
        errors.append("expected_error_count does not equal len(errors)")
    if not 0 <= len(findings) <= 25:
        errors.append("error count is outside 0..25")

    type_counts = collections.Counter(str(item.get("source_type")) for item in findings)
    difficulty_counts = collections.Counter(item.get("difficulty") for item in findings)
    error_type_counts = collections.Counter(item.get("error_type_id") for item in findings)
    expected_source_counts = {str(key): type_counts[str(key)] for key in (1, 2, 3)}
    expected_difficulty_counts = {key: difficulty_counts[key] for key in ("low", "medium", "high")}
    if payload.get("counts_by_source_type") != expected_source_counts:
        errors.append("counts_by_source_type mismatch")
    if payload.get("counts_by_difficulty") != expected_difficulty_counts:
        errors.append("counts_by_difficulty mismatch")
    if payload.get("counts_by_error_type") != dict(sorted(error_type_counts.items())):
        errors.append("counts_by_error_type mismatch")

    seen_instance_ids: set[str] = set()
    seen_error_types: set[str] = set()
    for index, item in enumerate(findings, start=1):
        missing = ERROR_FIELDS - set(item)
        if missing:
            errors.append(f"error {index} missing fields: {sorted(missing)}")
        instance_id = item.get("instance_id")
        if instance_id in seen_instance_ids:
            errors.append(f"duplicate instance_id: {instance_id}")
        seen_instance_ids.add(instance_id)
        error_type_id = item.get("error_type_id")
        if error_type_id not in EXPECTED_ERROR_TYPES:
            errors.append(f"error {index} has unknown error_type_id")
        if error_type_id in seen_error_types:
            errors.append(f"duplicate error_type_id in one document: {error_type_id}")
        seen_error_types.add(error_type_id)
        if item.get("source_type") not in (1, 2, 3):
            errors.append(f"error {index} has invalid source_type")
        if EXPECTED_SOURCE_BY_ERROR_TYPE.get(error_type_id) != item.get("source_type"):
            errors.append(f"error {index} source_type contradicts error_type_id")
        catalog_item = catalog.get(item.get("mutation_id"))
        if catalog_item is None:
            errors.append(f"error {index} mutation_id absent from catalog")
        else:
            for field in (
                "error_type_id", "source_type", "difficulty", "title", "problem",
                "roles", "priority", "clarification_needed", "affected_sections",
            ):
                if item.get(field) != catalog_item.get(field):
                    errors.append(f"error {index} field {field} contradicts catalog")
        if item.get("difficulty") not in ("low", "medium", "high"):
            errors.append(f"error {index} has invalid difficulty")
        if item.get("evidence_relation") not in ("at", "before", "after"):
            errors.append(f"error {index} has invalid evidence_relation")
        roles = item.get("roles")
        allowed_roles = {"Data Analyst", "Data Engineer", "QA"}
        if not isinstance(roles, list) or not roles or not set(roles) <= allowed_roles:
            errors.append(f"error {index} has invalid roles")
        if item.get("priority") not in ("P0", "P1", "P2"):
            errors.append(f"error {index} has invalid priority")
        quote = item.get("evidence_quote", "")
        if not quote or text.count(quote) != 1:
            errors.append(f"error {index} evidence occurs {text.count(quote)} times")

    clean_id = payload.get("clean_document_id")
    clean_path = CLEAN_DIR / f"{clean_id}.md" if isinstance(clean_id, str) else None
    if clean_path is None or not clean_path.exists():
        errors.append("clean_document_id does not exist")
    else:
        clean_text = clean_path.read_text(encoding="utf-8")
        if payload.get("clean_document_sha256") != sha256_text(clean_text):
            errors.append("clean_document_sha256 mismatch")
        if not findings and text != clean_text:
            errors.append("zero-error document differs from clean source")
    if payload.get("document_sha256") != sha256_text(text):
        errors.append("document_sha256 mismatch")
    if payload.get("document_id") != md_path.stem:
        errors.append("document_id does not match filename")
    if payload.get("document_file") != f"corrupted/{md_path.name}":
        errors.append("document_file does not match filename")
    if isinstance(clean_id, str) and payload.get("clean_document_file") != f"clean/{clean_id}.md":
        errors.append("clean_document_file does not match clean_document_id")
    return errors


def main() -> int:
    failures: list[str] = []
    try:
        catalog = load_catalog()
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as error:
        print(f"VALIDATION FAILED\n- invalid error catalog: {error}")
        return 1
    clean_files = sorted(CLEAN_DIR.glob("clean_*.md"))
    if len(clean_files) != 10:
        failures.append(f"expected 10 clean files, found {len(clean_files)}")
    for path in clean_files:
        for error in validate_clean(path):
            failures.append(f"{path.relative_to(ROOT)}: {error}")

    md_files = sorted(CORRUPTED_DIR.glob("case_*.md")) if CORRUPTED_DIR.exists() else []
    json_files = sorted(CORRUPTED_DIR.glob("case_*.json")) if CORRUPTED_DIR.exists() else []
    if md_files or json_files:
        if len(md_files) != 50 or len(json_files) != 50:
            failures.append(f"expected 50 md/json pairs, found {len(md_files)}/{len(json_files)}")
        counts: list[int] = []
        clean_usage: collections.Counter[str] = collections.Counter()
        clean_error_load: collections.Counter[str] = collections.Counter()
        covered_error_types: set[str] = set()
        covered_mutations: set[str] = set()
        combination_signatures: list[tuple[str, ...]] = []
        global_instance_ids: list[str] = []
        for md_path in md_files:
            json_path = md_path.with_suffix(".json")
            if not json_path.exists():
                failures.append(f"missing sidecar for {md_path.name}")
                continue
            payload = json.loads(json_path.read_text(encoding="utf-8"))
            counts.append(payload.get("expected_error_count", -1))
            clean_usage.update([payload.get("clean_document_id")])
            clean_error_load[payload.get("clean_document_id")] += payload.get("expected_error_count", 0)
            covered_error_types.update(item.get("error_type_id") for item in payload.get("errors", []))
            covered_mutations.update(item.get("mutation_id") for item in payload.get("errors", []))
            combination_signatures.append(tuple(sorted(item.get("mutation_id") for item in payload.get("errors", []))))
            global_instance_ids.extend(item.get("instance_id") for item in payload.get("errors", []))
            for error in validate_corrupted(md_path, json_path, catalog):
                failures.append(f"{md_path.relative_to(ROOT)}: {error}")
        expected_counts = sorted([0, 25] + list(range(1, 25)) * 2)
        if sorted(counts) != expected_counts:
            failures.append("error-count distribution is not uniform over 0..25")
        expected_usage = {path.stem: 5 for path in clean_files}
        if dict(sorted(clean_usage.items())) != expected_usage:
            failures.append("each clean source must be used exactly five times")
        if sorted(clean_error_load.values()) != [62] * 5 + [63] * 5:
            failures.append("clean-source error load must be balanced to 62 or 63")
        if len(combination_signatures) != len(set(combination_signatures)):
            failures.append("corrupted documents do not have unique mutation combinations")
        if len(global_instance_ids) != len(set(global_instance_ids)):
            failures.append("instance_id values are not globally unique")
        if covered_error_types != EXPECTED_ERROR_TYPES:
            missing = sorted(EXPECTED_ERROR_TYPES - covered_error_types)
            extra = sorted(covered_error_types - EXPECTED_ERROR_TYPES)
            failures.append(f"error type coverage mismatch; missing={missing}, extra={extra}")
        if covered_mutations != set(catalog):
            missing = sorted(set(catalog) - covered_mutations)
            extra = sorted(covered_mutations - set(catalog))
            failures.append(f"mutation coverage mismatch; missing={missing}, extra={extra}")

    if failures:
        print("VALIDATION FAILED")
        print("\n".join(f"- {failure}" for failure in failures))
        return 1
    print(f"VALIDATION PASSED: {len(clean_files)} clean, {len(md_files)} corrupted pairs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
