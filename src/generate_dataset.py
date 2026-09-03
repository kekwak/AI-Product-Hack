#!/usr/bin/env python3
"""Create deterministic corrupted variants and auditable sidecar metadata."""

from __future__ import annotations

import collections
import hashlib
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from paths import DATASET_DIR

ROOT = DATASET_DIR
CLEAN_DIR = ROOT / "clean"
OUT_DIR = ROOT / "corrupted"
SEED = 20260903


def normalized_heading(line: str) -> str:
    return re.sub(r"^#{1,6}\s+", "", line.strip())


def find_heading(lines: list[str], heading: str, *, prefix: bool = False) -> int:
    matches = []
    for index, line in enumerate(lines):
        normalized = normalized_heading(line)
        if normalized == heading or (prefix and normalized.startswith(heading)):
            matches.append(index)
    if len(matches) != 1:
        raise ValueError(f"expected one heading {heading!r}, found {len(matches)}")
    return matches[0]


def split_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        raise ValueError(f"not a Markdown table row: {line!r}")
    # A Markdown table may legitimately contain an escaped pipe (for example,
    # a composite Kafka key written as `left\|right`). It is content, not a
    # column delimiter.
    return [cell.strip() for cell in re.split(r"(?<!\\)\|", stripped[1:-1])]


def join_row(cells: list[str]) -> str:
    return "| " + " | ".join(cells) + " |"


def append_intro_field(text: str, label: str, sentence: str) -> tuple[str, str]:
    lines = text.splitlines()
    pattern = re.compile(rf"^\|\s*\*\*{re.escape(label)}\*\*\s*\|")
    matches = [index for index, line in enumerate(lines) if pattern.match(line)]
    if len(matches) != 1:
        raise ValueError(f"expected one intro field {label!r}, found {len(matches)}")
    index = matches[0]
    cells = split_row(lines[index])
    cells[1] = cells[1].rstrip() + " " + sentence
    lines[index] = join_row(cells)
    return "\n".join(lines) + "\n", sentence


def insert_after_heading(text: str, heading: str, sentence: str, *, prefix: bool = False) -> tuple[str, str]:
    lines = text.splitlines()
    index = find_heading(lines, heading, prefix=prefix)
    insertion = index + 1
    if insertion < len(lines) and not lines[insertion].strip():
        insertion += 1
    lines[insertion:insertion] = [sentence, ""]
    return "\n".join(lines) + "\n", sentence


def replace_section_body(text: str, heading: str, body: str, *, prefix: bool = False) -> tuple[str, str]:
    lines = text.splitlines()
    index = find_heading(lines, heading, prefix=prefix)
    current_level = len(lines[index]) - len(lines[index].lstrip("#"))
    end = len(lines)
    for candidate in range(index + 1, len(lines)):
        stripped = lines[candidate].lstrip()
        if not stripped.startswith("#"):
            continue
        level = len(stripped) - len(stripped.lstrip("#"))
        if level <= current_level:
            end = candidate
            break
    replacement = ["", body, ""]
    lines[index + 1 : end] = replacement
    return "\n".join(lines) + "\n", body


def table_data_row_indices(lines: list[str], heading: str) -> list[int]:
    heading_index = find_heading(lines, heading, prefix=heading == "Структура данных")
    table_start = next(
        (index for index in range(heading_index + 1, len(lines)) if lines[index].strip().startswith("|")),
        None,
    )
    if table_start is None:
        raise ValueError(f"table not found after {heading!r}")
    table_indices: list[int] = []
    for index in range(table_start, len(lines)):
        if not lines[index].strip().startswith("|"):
            break
        table_indices.append(index)
    separator_positions = [
        position
        for position, index in enumerate(table_indices)
        if all(re.fullmatch(r":?-{3,}:?", cell) for cell in split_row(lines[index]))
    ]
    if not separator_positions:
        raise ValueError(f"separator not found after {heading!r}")
    candidates = table_indices[separator_positions[0] + 1 :]
    return [
        index
        for index in candidates
        if not any(token in " ".join(split_row(lines[index])) for token in ("**Атрибут**", "Приемники"))
    ]


def mutate_table_cell(
    text: str,
    heading: str,
    cell_index: int,
    value: str,
    *,
    predicate: Callable[[list[str]], bool] | None = None,
) -> tuple[str, str]:
    lines = text.splitlines()
    candidates = table_data_row_indices(lines, heading)
    chosen = None
    for index in candidates:
        cells = split_row(lines[index])
        if predicate is None or predicate(cells):
            chosen = index
            break
    if chosen is None:
        raise ValueError(f"no applicable row in {heading!r}")
    cells = split_row(lines[chosen])
    if cell_index >= len(cells):
        raise ValueError(f"cell {cell_index} absent in row {lines[chosen]!r}")
    cells[cell_index] = value
    lines[chosen] = join_row(cells)
    return "\n".join(lines) + "\n", value


def remove_first_nullability(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    for index in table_data_row_indices(lines, "Структура данных"):
        cells = split_row(lines[index])
        original = cells[2]
        cells[2] = re.sub(r"[;,]?\s*`?(?:NOT NULL|NULLABLE)`?", "", original, count=1).strip()
        if cells[2] != original:
            evidence = join_row(cells)
            lines[index] = evidence
            return "\n".join(lines) + "\n", evidence
    raise ValueError("no nullability marker found")


def corrupt_reference_catalog(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    candidates = table_data_row_indices(lines, "Источники обогащения данных")
    if not candidates:
        raise ValueError("enrichment row not found")
    index = candidates[0]
    cells = split_row(lines[index])
    cells[0] = "Неуказанный справочник"
    cells[1] = "Ссылка на справочник отсутствует"
    cells[2] = "Используется для обогащения; поля и версия не перечислены"
    lines[index] = join_row(cells)
    return "\n".join(lines) + "\n", "Ссылка на справочник отсутствует"


def rename_heading(text: str, heading: str, replacement: str, *, prefix: bool = False) -> tuple[str, str]:
    lines = text.splitlines()
    index = find_heading(lines, heading, prefix=prefix)
    hashes = re.match(r"^(#{1,6}\s+)", lines[index].strip())
    prefix_text = hashes.group(1) if hashes else ""
    lines[index] = prefix_text + replacement
    return "\n".join(lines) + "\n", replacement


def drop_structure_comment_column(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    heading_index = find_heading(lines, "Структура данных", prefix=True)
    table_start = next(index for index in range(heading_index + 1, len(lines)) if lines[index].strip().startswith("|"))
    table_end = table_start
    while table_end < len(lines) and lines[table_end].strip().startswith("|"):
        cells = split_row(lines[table_end])
        if len(cells) >= 7:
            cells.pop()
            lines[table_end] = join_row(cells)
        table_end += 1
    # Keep the mandatory section name intact: the single T01 violation here is
    # the missing required table column itself.
    evidence = lines[table_start]
    return "\n".join(lines) + "\n", evidence


def corrupt_first_ddl_type(text: str) -> tuple[str, str]:
    lines = text.splitlines()
    structure_indices = table_data_row_indices(lines, "Структура данных")
    field_name = split_row(lines[structure_indices[0]])[0].strip("`")
    ddl_index = find_heading(lines, "DDL")
    field_pattern = re.compile(rf"^(\s*{re.escape(field_name)}\s+)([A-Za-z]+(?:\([0-9, ]+\))?)(.*)$", re.IGNORECASE)
    for index in range(ddl_index + 1, len(lines)):
        match = field_pattern.match(lines[index])
        if not match:
            continue
        old_type = match.group(2).upper()
        new_type = "BIGINT" if any(token in old_type for token in ("CHAR", "STRING", "TEXT")) else "STRING"
        lines[index] = match.group(1) + new_type + match.group(3)
        evidence = lines[index].strip()
        return "\n".join(lines) + "\n", evidence
    raise ValueError(f"DDL field {field_name!r} not found")


@dataclass(frozen=True)
class Mutation:
    mutation_id: str
    error_type_id: str
    source_type: int
    difficulty: str
    title: str
    description: str
    expected_clarification: str
    apply: Callable[[str], tuple[str, str]]


def mutation(
    mutation_id: str,
    error_type_id: str,
    source_type: int,
    difficulty: str,
    title: str,
    description: str,
    expected_clarification: str,
    apply: Callable[[str], tuple[str, str]],
) -> Mutation:
    return Mutation(
        mutation_id,
        error_type_id,
        source_type,
        difficulty,
        title,
        description,
        expected_clarification,
        apply,
    )


DOMAIN_MUTATIONS = [
    mutation("D01_SIMPLE_SERIALIZATION", "D01", 1, "medium", "Неполная спецификация сериализации", "Для одного входного потока оставлен только формат без схемы, версии и способа десериализации.", "Уточнить схему, версию и механизм сериализации/десериализации.", lambda text: mutate_table_cell(text, "Источники данных", 3, "JSON; схема и версия не указаны")),
    mutation("D02_MISSING_CATALOG_LINK", "D02", 1, "low", "Нет прямой ссылки на Data Catalog", "У одного источника удалена прямая ссылка на карточку каталога.", "Добавить точную ссылку на объект источника в Data Catalog.", lambda text: mutate_table_cell(text, "Источники данных", 2, "Data Catalog: ссылка отсутствует")),
    mutation("D03_MISSING_NULLABILITY", "D03", 1, "low", "Не указана обязательность поля", "Для одного целевого поля удален признак NOT NULL/NULLABLE.", "Указать обязательность поля и согласовать ее с алгоритмом.", remove_first_nullability),
    mutation("D04_VAGUE_FILTER", "D04", 1, "medium", "Не описаны точные фильтры", "Точные условия отбора заменены субъективным требованием о корректности записей.", "Перечислить проверяемые условия включения и исключения данных.", lambda text: replace_section_body(text, "Шаг 1. Фильтрация данных", "В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.", prefix=True)),
    mutation("D05_UNSPECIFIED_ENRICHMENT_STAGE", "D05", 1, "low", "Этап не заполнен и не отмечен как неприменимый", "Раздел обогащения оставлен без алгоритма и без явного «не применимо».", "Описать обогащение либо явно указать, что оно не применяется.", lambda text: replace_section_body(text, "Шаг 2. Обогащение данных", "Описание этого этапа будет согласовано после начала разработки.", prefix=True)),
    mutation("D06_MISSING_KAFKA_CLUSTER", "D06", 1, "low", "Не указан Kafka-кластер", "Для одного Kafka-источника удалено имя кластера.", "Указать точное имя Kafka-кластера.", lambda text: mutate_table_cell(text, "Источники данных", 1, "Kafka; кластер не указан", predicate=lambda cells: any("kafka" in cell.lower() for cell in cells))),
    mutation("D07_MISSING_HDFS_PATH", "D07", 1, "low", "Не указан полный путь файлового приемника", "Для одного HDFS-приемника удален полный путь хранения.", "Указать кластер, полный HDFS-путь и формат хранения.", lambda text: mutate_table_cell(text, "Приемники данных", 1, "HDFS; путь не указан", predicate=lambda cells: len(cells) > 1 and "hdfs" in cells[1].lower())),
    mutation("D08_UNIDENTIFIED_REFERENCE", "D08", 1, "medium", "Не идентифицирован справочник", "Источник обогащения заменен общим упоминанием без точного имени, ссылки, версии и состава полей.", "Указать точное имя, ссылку, назначение и версию справочника.", corrupt_reference_catalog),
]


TEMPLATE_MUTATIONS = [
    mutation("T01_RENAME_FAQ", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел FAQ переименован в несопоставимый заголовок.", "Вернуть раздел с названием FAQ.", lambda text: rename_heading(text, "FAQ", "Прочие сведения для пользователя")),
    mutation("T01_RENAME_SOURCES", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел источников переименован.", "Вернуть точное название «Источники данных».", lambda text: rename_heading(text, "Источники данных", "Входные сущности проекта")),
    mutation("T01_RENAME_RECEIVERS", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел приемников переименован.", "Вернуть точное название «Приемники данных».", lambda text: rename_heading(text, "Приемники данных", "Итоговые объекты проекта")),
    mutation("T01_RENAME_FLOW", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел схемы потока переименован.", "Вернуть точное название «Схема потоков данных».", lambda text: rename_heading(text, "Схема потоков данных", "Общая архитектура")),
    mutation("T01_STEP3_PLACEHOLDER", "T01", 2, "low", "В шаблоне остался плейсхолдер", "Название третьего шага заменено незаполненной подсказкой шаблона.", "Указать фактическое название третьего шага.", lambda text: rename_heading(text, "Шаг 3.", "Шаг 3. <Наименование шага 3>", prefix=True)),
    mutation("T01_RENAME_KEYS", "T01", 2, "low", "Нарушен обязательный шаблон", "Раздел формирования Kafka key/HDFS partition невозможно сопоставить по названию.", "Вернуть обязательное название раздела ключа и партиции.", lambda text: rename_heading(text, "Формирование ключа (kafka) / партиции (hdfs)", "Технические параметры размещения")),
    mutation("T01_RENAME_EXAMPLE", "T01", 2, "low", "Нарушен обязательный шаблон", "Раздел примера данных переименован в несопоставимый заголовок.", "Вернуть обязательное название «Пример данных».", lambda text: rename_heading(text, "Пример данных", "Демонстрационный фрагмент")),
    mutation("T01_DROP_STRUCTURE_COLUMN", "T01", 2, "medium", "Нарушена таблица обязательного шаблона", "Из таблицы маппинга удалена обязательная колонка комментариев.", "Восстановить семь колонок таблицы структуры данных.", drop_structure_comment_column),
]


UNIVERSAL_MUTATIONS = [
    mutation("U01_SCOPE_OUTPUT_GAP", "U01", 3, "high", "Заявленный результат не обеспечен контрактом", "В цель добавлен прогноз, которого нет в выходной схеме и алгоритме.", "Уточнить, входит ли прогноз в скоуп, и описать его выход и расчет либо исключить обещание.", lambda text: append_intro_field(text, "Решаемая проблема", "Результат также должен обеспечивать прогноз значений на следующие 30 дней.")),
    mutation("U02_UNDEFINED_ACTIVE", "U02", 3, "medium", "Не определен термин «активная запись»", "Введен влияющий на выборку термин без формального определения.", "Определить проверяемые признаки активной записи.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Во всех расчетах используются только активные записи источников.")),
    mutation("U03_CONFLICTING_SOURCES", "U03", 3, "high", "Не задан источник истины", "При конфликте источников разрешен произвольный выбор.", "Назначить авторитетный источник и детерминированное правило разрешения конфликтов.", lambda text: append_intro_field(text, "Системы-источники", "При расхождении значений между источниками допускается использовать значение любого из них.")),
    mutation("U04_GRAIN_CONTRADICTION", "U04", 3, "high", "Противоречиво определена гранулярность", "Одна строка одновременно названа событием и агрегатом за период.", "Зафиксировать одну единицу строки и полный бизнес-ключ.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Одна строка результата одновременно соответствует отдельному событию и агрегату за расчетный период.")),
    mutation("U05_AMBIGUOUS_MAPPING", "U05", 3, "medium", "Неоднозначен маппинг поля", "Разрешено брать значение из любого одноименного атрибута без приоритета.", "Указать точный источник и атрибут каждого целевого поля.", lambda text: insert_after_heading(text, "Структура данных", "Если одноименное поле найдено в нескольких источниках, выбирается любое доступное значение.")),
    mutation("U06_ARBITRARY_OPERATION_ORDER", "U06", 3, "high", "Не определен порядок операций", "Фильтрация и обогащение разрешены в любом порядке, хотя это может менять состав данных.", "Зафиксировать последовательность операций и слой применения каждого фильтра.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.")),
    mutation("U07_OPEN_TIME_BOUNDARY", "U07", 3, "medium", "Не определены границы периода", "Относительный период не задает опорное время и включение граничных значений.", "Указать опорное время и точный полуинтервал отбора.", lambda text: insert_after_heading(text, "Шаг 1. Фильтрация данных", "Дополнительно учитываются события только за последние 7 дней." , prefix=True)),
    mutation("U08_JOIN_CARDINALITY_GAP", "U08", 3, "high", "Не определена кардинальность обогащения", "При нескольких совпадениях сохраняются все строки, что может размножить результат.", "Указать полный ключ, ожидаемую кардинальность и правило для нуля и нескольких совпадений.", lambda text: insert_after_heading(text, "Шаг 2. Обогащение данных", "При нескольких совпадениях со справочником в результат передаются все найденные варианты.", prefix=True)),
    mutation("U09_NONDETERMINISTIC_LAST", "U09", 3, "high", "Недетерминирован выбор последней записи", "При одинаковом времени выбор оставлен произвольным.", "Задать дополнительный уникальный tie-break и полный порядок сортировки.", lambda text: insert_after_heading(text, "Шаг 3.", "Если несколько последних записей имеют одинаковое время, сохраняется любая из них.", prefix=True)),
    mutation("U10_CONFLICTING_ZERO_RULES", "U10", 3, "high", "Конфликтуют правила для нулевого значения", "Для одного состояния одновременно заданы два разных результата.", "Определить единственный результат и приоритет правил для нуля.", lambda text: insert_after_heading(text, "Шаг 3.", "Нулевой показатель записывается как 0; одновременно нулевое значение считается неизвестным и записывается как NULL.", prefix=True)),
    mutation("U11_TIMEZONE_AMBIGUITY", "U11", 3, "high", "Не определен часовой пояс", "Один timestamp разрешено интерпретировать в двух часовых поясах.", "Зафиксировать timezone источника, результата и правила преобразования.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Временные метки могут интерпретироваться как UTC или московское время в зависимости от реализации.")),
    mutation("U12_UNSPECIFIED_UNITS", "U12", 3, "medium", "Не определены единицы числовых полей", "Единицы значимых показателей переданы на усмотрение потребителя.", "Зафиксировать единицу измерения и преобразование для каждого числового поля.", lambda text: insert_after_heading(text, "Структура данных", "Единицы измерения числовых показателей определяются каждым потребителем самостоятельно.")),
    mutation("U13_ARBITRARY_BAD_DATA", "U13", 3, "medium", "Не определена обработка невалидных данных", "Одинаково невалидные записи могут быть сохранены или удалены.", "Назначить однозначное действие для каждого класса невалидных данных.", lambda text: insert_after_heading(text, "Шаг 1. Фильтрация данных", "Некорректную запись разрешается либо исключить, либо сохранить без изменений.", prefix=True)),
    mutation("U14_NON_IDEMPOTENT_RETRY", "U14", 3, "high", "Не определена семантика повторного запуска", "Повтор может выполнять append или overwrite и поэтому менять число строк.", "Указать единую стратегию записи и доказуемое правило идемпотентности.", lambda text: insert_after_heading(text, "Формирование ключа (kafka) / партиции (hdfs)", "При повторном запуске партиция может дополняться или перезаписываться по выбору оператора.")),
    mutation("U15_UNVERSIONED_SCHEMA_CHANGE", "U15", 3, "high", "Допущено несовместимое изменение схемы", "Тип поля разрешено менять без новой версии контракта.", "Определить правила версионирования, совместимости и миграции потребителей.", lambda text: insert_after_heading(text, "FAQ", "Тип существующего поля можно изменить без выпуска новой версии, если его имя сохраняется.")),
    mutation("U16_IMPOSSIBLE_ZERO_LATENCY", "U16", 3, "medium", "Задано нереализуемое требование задержки", "При пиковом объеме требуется строго нулевая задержка, что невозможно объективно выполнить.", "Указать измеримый ненулевой SLA и точку его измерения.", lambda text: append_intro_field(text, "Нефункциональные требования", "При любом пиковом объеме задержка обработки должна быть строго 0 секунд.")),
    mutation("U17_DDL_TYPE_CONFLICT", "U17", 3, "low", "Тип поля расходится с DDL", "Тип первого целевого поля изменен только в DDL и конфликтует с таблицей структуры.", "Согласовать тип поля во всех частях документа.", corrupt_first_ddl_type),
    mutation("U18_VAGUE_ACCEPTANCE", "U18", 3, "low", "Непроверяемый критерий качества", "Добавлена метрика без формулы, порога и периода измерения.", "Задать формулу, измеримый порог, период и источник проверки.", lambda text: append_intro_field(text, "Продуктовые метрики", "Дополнительная метрика: качество результата должно быть высоким.")),
    mutation("U19_UNMASKED_SENSITIVE_LOG", "U19", 3, "medium", "Чувствительные данные сохраняются без ограничений", "Полный payload с идентификаторами разрешено бессрочно писать в журнал.", "Уточнить маскирование, состав логов, права доступа и срок хранения.", lambda text: insert_after_heading(text, "FAQ", "Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.")),
]


def roles_for(item: Mutation) -> list[str]:
    if item.error_type_id == "T01":
        return ["Data Analyst", "QA"]
    if item.error_type_id in {"D01", "D03", "D06", "D07", "U11", "U14", "U15", "U17", "U19"}:
        return ["Data Engineer", "QA"]
    return ["Data Analyst", "Data Engineer", "QA"]


def priority_for(item: Mutation) -> str:
    p0_types = {
        "D01", "D04", "D06", "D07", "U01", "U03", "U04", "U06", "U08",
        "U09", "U10", "U11", "U14", "U15", "U17", "U19",
    }
    return "P0" if item.error_type_id in p0_types else "P1"


def affected_sections_for(item: Mutation) -> list[str]:
    by_type = {
        "D01": ["Источники данных"], "D02": ["Источники данных"],
        "D03": ["Структура данных"], "D04": ["Шаг 1. Фильтрация данных"],
        "D05": ["Шаг 2. Обогащение данных"], "D06": ["Источники данных"],
        "D07": ["Приемники данных"], "D08": ["Источники обогащения данных"],
        "U01": ["Решаемая проблема"], "U02": ["Алгоритм обработки потока"],
        "U03": ["Системы-источники"], "U04": ["Алгоритм обработки потока"],
        "U05": ["Структура данных"], "U06": ["Алгоритм обработки потока"],
        "U07": ["Шаг 1. Фильтрация данных"], "U08": ["Шаг 2. Обогащение данных"],
        "U09": ["Шаг 3"], "U10": ["Шаг 3"],
        "U11": ["Алгоритм обработки потока"], "U12": ["Структура данных"],
        "U13": ["Шаг 1. Фильтрация данных"],
        "U14": ["Формирование ключа (kafka) / партиции (hdfs)"],
        "U15": ["FAQ"], "U16": ["Нефункциональные требования"],
        "U17": ["Структура данных", "DDL"], "U18": ["Продуктовые метрики"],
        "U19": ["FAQ"],
    }
    if item.error_type_id == "T01":
        return [item.title]
    return by_type[item.error_type_id]


def write_error_catalog() -> None:
    catalog = []
    for item in DOMAIN_MUTATIONS + TEMPLATE_MUTATIONS + UNIVERSAL_MUTATIONS:
        catalog.append(
            {
                "mutation_id": item.mutation_id,
                "error_type_id": item.error_type_id,
                "source_type": item.source_type,
                "difficulty": item.difficulty,
                "title": item.title,
                "problem": item.description,
                "roles": roles_for(item),
                "priority": priority_for(item),
                "clarification_needed": item.expected_clarification,
                "affected_sections": affected_sections_for(item),
            }
        )
    (ROOT / "error_catalog.json").write_text(
        json.dumps(catalog, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def applicable(mutations: list[Mutation], clean_text: str) -> list[Mutation]:
    result = []
    for item in mutations:
        try:
            mutated, evidence = item.apply(clean_text)
        except (ValueError, IndexError):
            continue
        if mutated != clean_text and mutated.count(evidence) == 1:
            result.append(item)
    return result


def choose_mutations(count: int, case_index: int, rng: random.Random, clean_text: str) -> list[Mutation]:
    if count == 0:
        return []
    domain_pool = applicable(DOMAIN_MUTATIONS, clean_text)
    universal_pool = applicable(UNIVERSAL_MUTATIONS, clean_text)
    template_pool = applicable(TEMPLATE_MUTATIONS, clean_text)
    include_template = count >= 4 and (case_index % 3 != 0 or count >= 24)
    template_count = 1 if include_template else 0
    remaining = count - template_count
    min_domain = max(0, remaining - len(universal_pool))
    max_domain = min(len(domain_pool), remaining)
    desired_domain = round(remaining * (0.28 + 0.06 * (case_index % 3)))
    domain_count = min(max(desired_domain, min_domain), max_domain)
    universal_count = remaining - domain_count
    if universal_count > len(universal_pool) or template_count > len(template_pool):
        raise ValueError(f"not enough applicable mutations for case {case_index}")
    selected = rng.sample(domain_pool, domain_count)
    selected += rng.sample(universal_pool, universal_count)
    if template_count:
        selected.append(rng.choice(template_pool))
    return selected


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> None:
    clean_files = sorted(CLEAN_DIR.glob("clean_*.md"))
    if len(clean_files) != 10:
        raise SystemExit(f"expected 10 clean documents, found {len(clean_files)}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_error_catalog()

    # The schedule is maximally uniform over 0..25 and also balances the total
    # injected load of each five-variant clean-document family to 62 or 63.
    counts = [
        16, 23, 5, 8, 11,
        4, 14, 19, 0, 25,
        17, 13, 9, 20, 3,
        23, 12, 3, 7, 18,
        14, 17, 1, 20, 10,
        6, 19, 22, 15, 1,
        2, 24, 10, 21, 6,
        24, 7, 16, 4, 11,
        13, 21, 9, 18, 2,
        22, 15, 8, 5, 12,
    ]
    manifest_cases = []

    for case_index in range(1, 51):
        clean_path = clean_files[(case_index - 1) // 5]
        clean_text = clean_path.read_text(encoding="utf-8")
        error_count = counts[case_index - 1]
        case_seed = SEED * 100 + case_index
        rng = random.Random(case_seed)
        selected = choose_mutations(error_count, case_index, rng, clean_text)
        if len(selected) != error_count:
            raise RuntimeError(f"selection mismatch for case {case_index}")

        text = clean_text
        findings = []
        # The column-removal template mutation must run before cell-level
        # mutations so their final evidence rows remain searchable verbatim.
        def mutation_phase(item: Mutation) -> int:
            if item.mutation_id == "T01_DROP_STRUCTURE_COLUMN":
                return 0
            return {1: 1, 3: 2, 2: 3}[item.source_type]

        ordered = sorted(selected, key=mutation_phase)
        for error_index, item in enumerate(ordered, start=1):
            text, evidence = item.apply(text)
            findings.append(
                {
                    "instance_id": f"case_{case_index:03d}-E{error_index:02d}",
                    "mutation_id": item.mutation_id,
                    "error_type_id": item.error_type_id,
                    "source_type": item.source_type,
                    "difficulty": item.difficulty,
                    "evidence_quote": evidence,
                    "evidence_relation": "at",
                    "title": item.title,
                    "problem": item.description,
                    "roles": roles_for(item),
                    "priority": priority_for(item),
                    "clarification_needed": item.expected_clarification,
                    "affected_sections": affected_sections_for(item),
                    "injected_change": item.description,
                }
            )

        for finding in findings:
            occurrences = text.count(finding["evidence_quote"])
            if occurrences != 1:
                raise RuntimeError(
                    f"{finding['instance_id']} evidence occurs {occurrences} times: "
                    f"{finding['evidence_quote']!r}"
                )

        document_id = f"case_{case_index:03d}"
        md_path = OUT_DIR / f"{document_id}.md"
        json_path = OUT_DIR / f"{document_id}.json"
        md_path.write_text(text, encoding="utf-8")
        raw_source_type_counts = collections.Counter(str(item["source_type"]) for item in findings)
        raw_difficulty_counts = collections.Counter(item["difficulty"] for item in findings)
        source_type_counts = {str(key): raw_source_type_counts[str(key)] for key in (1, 2, 3)}
        difficulty_counts = {key: raw_difficulty_counts[key] for key in ("low", "medium", "high")}
        error_type_counts = collections.Counter(item["error_type_id"] for item in findings)
        metadata = {
            "schema_version": "1.0",
            "document_id": document_id,
            "document_file": f"corrupted/{document_id}.md",
            "clean_document_id": clean_path.stem,
            "clean_document_file": f"clean/{clean_path.name}",
            "generator_seed": case_seed,
            "generator_version": "synthetic-tz-v1",
            "document_sha256": sha256_text(text),
            "clean_document_sha256": sha256_text(clean_text),
            "expected_error_count": len(findings),
            "counts_by_source_type": source_type_counts,
            "counts_by_difficulty": difficulty_counts,
            "counts_by_error_type": dict(sorted(error_type_counts.items())),
            "errors": findings,
        }
        json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        manifest_cases.append(
            {
                "document_id": document_id,
                "clean_document_id": clean_path.stem,
                "expected_error_count": len(findings),
                "counts_by_source_type": metadata["counts_by_source_type"],
                "counts_by_difficulty": metadata["counts_by_difficulty"],
            }
        )

    overall_source_types = collections.Counter()
    overall_difficulties = collections.Counter()
    for case in manifest_cases:
        overall_source_types.update(case["counts_by_source_type"])
        overall_difficulties.update(case["counts_by_difficulty"])
    manifest = {
        "generation_seed": SEED,
        "clean_document_count": len(clean_files),
        "corrupted_document_count": len(manifest_cases),
        "error_count_distribution": dict(sorted(collections.Counter(counts).items())),
        "overall_source_type_counts": dict(sorted(overall_source_types.items())),
        "overall_difficulty_counts": dict(sorted(overall_difficulties.items())),
        "cases": manifest_cases,
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
