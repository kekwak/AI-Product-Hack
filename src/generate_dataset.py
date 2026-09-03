#!/usr/bin/env python3
"""Create deterministic corrupted variants and auditable sidecar metadata."""

from __future__ import annotations

import json
import random
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from paths import DATASET_DIR

ROOT = DATASET_DIR
CLEAN_DIR = ROOT / "clean"
OUT_DIR = ROOT / "corrupted"
SEED = 20260903
ERROR_COUNTS = (
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
)


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
    occurrence: int = 0,
) -> tuple[str, str]:
    lines = text.splitlines()
    candidates = [
        index
        for index in table_data_row_indices(lines, heading)
        if predicate is None or predicate(split_row(lines[index]))
    ]
    if occurrence >= len(candidates):
        raise ValueError(f"no applicable row in {heading!r}")
    chosen = candidates[occurrence]
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
        cleaned = re.sub(r"[;,]?\s*`?(?:NOT NULL|NULLABLE)`?", "", original, count=1).strip()
        if cleaned != original:
            evidence = f"{cleaned}; обязательность поля `{cells[0]}` не определена"
            cells[2] = evidence
            lines[index] = join_row(cells)
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
    return "\n".join(lines) + "\n", "Неуказанный справочник"


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


def sequence(*edits: Callable[[str], tuple[str, str]]) -> Callable[[str], tuple[str, str]]:
    """Combine several edits into one logical mutation; the last edit is its evidence."""

    def apply(text: str) -> tuple[str, str]:
        evidence = ""
        for edit in edits:
            text, evidence = edit(text)
        return text, evidence

    return apply


def intro_edit(label: str, sentence: str) -> Callable[[str], tuple[str, str]]:
    return lambda text: append_intro_field(text, label, sentence)


def section_edit(heading: str, sentence: str, *, prefix: bool = False) -> Callable[[str], tuple[str, str]]:
    return lambda text: insert_after_heading(text, heading, sentence, prefix=prefix)


def rename_edit(heading: str, replacement: str, *, prefix: bool = False) -> Callable[[str], tuple[str, str]]:
    return lambda text: rename_heading(text, heading, replacement, prefix=prefix)


def cell_edit(
    heading: str,
    cell_index: int,
    value: str,
    *,
    predicate: Callable[[list[str]], bool] | None = None,
    occurrence: int = 0,
) -> Callable[[str], tuple[str, str]]:
    return lambda text: mutate_table_cell(
        text, heading, cell_index, value, predicate=predicate, occurrence=occurrence
    )


@dataclass(frozen=True)
class Mutation:
    mutation_id: str
    error_type_id: str
    source_type: int
    difficulty: str
    title: str
    description: str
    apply: Callable[[str], tuple[str, str]]


DOMAIN_MUTATIONS = [
    Mutation("D01_SIMPLE_SERIALIZATION", "D01", 1, "medium", "Неполная спецификация сериализации", "Для одного входного потока оставлен только формат без схемы, версии и способа десериализации.", lambda text: mutate_table_cell(text, "Источники данных", 3, "JSON; схема и версия не указаны")),
    Mutation("D02_MISSING_CATALOG_LINK", "D02", 1, "low", "Нет прямой ссылки на Data Catalog", "У одного источника удалена прямая ссылка на карточку каталога.", lambda text: mutate_table_cell(text, "Источники данных", 2, "Data Catalog: ссылка отсутствует")),
    Mutation("D03_MISSING_NULLABILITY", "D03", 1, "low", "Не указана обязательность поля", "Для одного целевого поля удален признак NOT NULL/NULLABLE.", remove_first_nullability),
    Mutation("D04_VAGUE_FILTER", "D04", 1, "medium", "Не описаны точные фильтры", "Точные условия отбора заменены субъективным требованием о корректности записей.", lambda text: replace_section_body(text, "Шаг 1. Фильтрация данных", "В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.", prefix=True)),
    Mutation("D05_UNSPECIFIED_ENRICHMENT_STAGE", "D05", 1, "low", "Этап не заполнен и не отмечен как неприменимый", "Раздел обогащения оставлен без алгоритма и без явного «не применимо».", lambda text: replace_section_body(text, "Шаг 2. Обогащение данных", "Описание этого этапа будет согласовано после начала разработки.", prefix=True)),
    Mutation("D06_MISSING_KAFKA_CLUSTER", "D06", 1, "low", "Не указан Kafka-кластер", "Для одного Kafka-источника удалено имя кластера.", lambda text: mutate_table_cell(text, "Источники данных", 1, "Kafka; кластер не указан", predicate=lambda cells: any("kafka" in cell.lower() for cell in cells))),
    Mutation("D07_MISSING_HDFS_PATH", "D07", 1, "low", "Не указан полный путь файлового приемника", "Для одного HDFS-приемника удален полный путь хранения.", lambda text: mutate_table_cell(text, "Приемники данных", 1, "HDFS; путь не указан", predicate=lambda cells: len(cells) > 1 and "hdfs" in cells[1].lower())),
    Mutation("D08_UNIDENTIFIED_REFERENCE", "D08", 1, "medium", "Не идентифицирован справочник", "Источник обогащения заменен общим упоминанием без точного имени, ссылки, версии и состава полей.", corrupt_reference_catalog),
    Mutation("D01_INPUT_OUTPUT_FORMAT_ONLY", "D01", 1, "high", "Сериализация не определена на обоих концах", "У источника и приемника одновременно оставлен только формат без версии схемы и механизма чтения/записи.", sequence(section_edit("FAQ", "Приемник читает данные в формате writer по умолчанию; версия модели и framing не фиксируются."), cell_edit("Источники данных", 3, "JSON; схема, версия и framing не указаны"))),
    Mutation("D01_TWO_INPUTS_USE_LATEST", "D01", 1, "high", "Два потока читаются по плавающей схеме", "Для двух входов зафиксирован формат, но reader использует неопределенную latest-версию вместо совместимого контракта.", sequence(cell_edit("Источники данных", 3, "Avro; reader всегда использует latest-схему из registry"), cell_edit("Источники данных", 3, "Protobuf; версия сообщения выбирается автоматически", occurrence=1))),
    Mutation("D02_SOURCE_AND_SINK_LINKS", "D02", 1, "medium", "Не прослеживаются источник и результат", "Прямые ссылки на карточки Data Catalog удалены одновременно у входа и приемника.", sequence(cell_edit("Источники данных", 2, "Карточка каталога не указана"), cell_edit("Приемники данных", 2, "Карточка каталога не указана для результата"))),
    Mutation("D02_REFERENCE_AND_SINK_LINKS", "D02", 1, "medium", "Нет ссылок на справочник и приемник", "У справочника и результирующего объекта отсутствуют конкретные ссылки Data Catalog.", sequence(cell_edit("Источники обогащения данных", 1, "Ссылка будет добавлена позднее"), cell_edit("Приемники данных", 2, "Ссылка будет добавлена после релиза"))),
    Mutation("D03_TWO_FIELDS_WITHOUT_NULLABILITY", "D03", 1, "medium", "Обязательность нескольких полей не определена", "Признак NULLABLE/NOT NULL удален сразу у двух полей результата.", sequence(remove_first_nullability, remove_first_nullability)),
    Mutation("D03_THREE_FIELDS_WITHOUT_NULLABILITY", "D03", 1, "high", "Контракт NULL нарушен системно", "Обязательность не задана для трех последовательных полей, поэтому поведение отсутствующих значений нельзя проверить.", sequence(remove_first_nullability, remove_first_nullability, remove_first_nullability)),
    Mutation("D04_FILTERS_SPLIT_AND_VAGUE", "D04", 1, "high", "Фильтры оставлены на усмотрение реализации", "Основной фильтр заменен субъективным правилом, а дополнительные исключения объявлены без условий.", sequence(lambda text: replace_section_body(text, "Шаг 1. Фильтрация данных", "Система должна оставлять качественные записи по правилам реализации.", prefix=True), section_edit("Шаг 1. Фильтрация данных", "Дополнительные исключения команда определяет после анализа первых запусков.", prefix=True))),
    Mutation("D04_ONLINE_EXPORT_FILTER_GAP", "D04", 1, "high", "Не согласованы фильтры расчета и выгрузки", "Для онлайн-расчета и повторной выгрузки заявлены разные наборы фильтров, но ни один набор не перечислен.", sequence(section_edit("Шаг 1. Фильтрация данных", "Онлайн применяет стандартные фильтры качества, перечень которых хранится в коде.", prefix=True), section_edit("Шаг 1. Фильтрация данных", "Replay использует отдельные выгрузочные фильтры, которые будут согласованы позднее.", prefix=True))),
    Mutation("D05_TWO_UNDEFINED_STAGES", "D05", 1, "high", "Два этапа оставлены незаполненными", "Обогащение и целевая трансформация обозначены как будущая работа без явного «не применимо».", sequence(lambda text: replace_section_body(text, "Шаг 2. Обогащение данных", "Логика будет определена командой разработки.", prefix=True), lambda text: replace_section_body(text, "Шаг 3.", "Описание появится после проверки прототипа.", prefix=True))),
    Mutation("D05_ENRICHMENT_AND_TRANSFORMATION_TBD", "D05", 1, "high", "Обязательные этапы заменены TBD", "Обогащение и целевая трансформация сохранены формально, но не описаны и не отмечены как неприменимые.", sequence(lambda text: replace_section_body(text, "Шаг 2. Обогащение данных", "TBD после выбора справочника.", prefix=True), lambda text: replace_section_body(text, "Шаг 3.", "TBD после согласования выходного расчета.", prefix=True))),
    Mutation("D06_TWO_KAFKA_CLUSTERS_MISSING", "D06", 1, "medium", "Не идентифицированы два Kafka-подключения", "Имена кластеров удалены сразу у двух Kafka-источников.", sequence(cell_edit("Источники данных", 1, "Kafka; кластер выбирается окружением", predicate=lambda cells: any("kafka" in cell.lower() for cell in cells)), cell_edit("Источники данных", 1, "Kafka; кластер выбирается конфигурацией", predicate=lambda cells: any("kafka" in cell.lower() for cell in cells), occurrence=1))),
    Mutation("D06_PRIMARY_AND_BACKUP_UNKNOWN", "D06", 1, "medium", "Не определены основной и резервный Kafka-кластеры", "В таблице потеряно имя рабочего кластера, а в общих сведениях добавлен неидентифицированный резервный.", sequence(cell_edit("Источники данных", 1, "Kafka; конкретный кластер не зафиксирован", predicate=lambda cells: any("kafka" in cell.lower() for cell in cells)), intro_edit("Системы-источники", "При недоступности используется резервный Kafka-кластер, имя которого выбирает эксплуатация."))),
    Mutation("D07_PATH_AND_FORMAT_MISSING", "D07", 1, "high", "У файлового приемника нет пути и формата", "В одной строке приемника одновременно удалены полный путь и параметры формата хранения.", sequence(cell_edit("Приемники данных", 3, "Формат файла выбирается writer по умолчанию", predicate=lambda cells: len(cells) > 1 and "hdfs" in cells[1].lower()), cell_edit("Приемники данных", 1, "HDFS-кластер указан в runtime; полный путь отсутствует", predicate=lambda cells: len(cells) > 1 and "hdfs" in cells[1].lower()))),
    Mutation("D07_BASE_AND_PARTITION_PATH_GAP", "D07", 1, "high", "Не определены базовый и партиционный пути", "У приемника отсутствует базовый HDFS-путь, а раздел партиционирования содержит только шаблон без физического адреса.", sequence(cell_edit("Приемники данных", 1, "HDFS; базовый каталог будет создан при запуске", predicate=lambda cells: len(cells) > 1 and "hdfs" in cells[1].lower()), section_edit("Формирование ключа (kafka) / партиции (hdfs)", "Физический путь партиции формируется по внутреннему шаблону, который в документе не приведен."))),
    Mutation("D08_REFERENCE_SET_INCOMPLETE", "D08", 1, "high", "Состав справочников неполон", "Один справочник обезличен в таблице, а алгоритм дополнительно ссылается на второй неописанный нормативный источник.", sequence(section_edit("Шаг 2. Обогащение данных", "После основного JOIN выполняется проверка по дополнительному корпоративному справочнику; его имя и версия не зафиксированы.", prefix=True), corrupt_reference_catalog)),
    Mutation("D08_REFERENCE_IDENTITY_AND_VERSION", "D08", 1, "high", "Справочник нельзя однозначно выбрать", "В карточке обогащения одновременно потеряны идентификатор справочника, ссылка и версия используемого среза.", sequence(cell_edit("Источники обогащения данных", 0, "Корпоративный справочник"), cell_edit("Источники обогащения данных", 1, "Ссылка отсутствует"), cell_edit("Источники обогащения данных", 2, "Используется актуальная версия с необходимыми полями"))),
]


TEMPLATE_MUTATIONS = [
    Mutation("T01_RENAME_FAQ", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел FAQ переименован в несопоставимый заголовок.", lambda text: rename_heading(text, "FAQ", "Прочие сведения для пользователя")),
    Mutation("T01_RENAME_SOURCES", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел источников переименован.", lambda text: rename_heading(text, "Источники данных", "Входные сущности проекта")),
    Mutation("T01_RENAME_RECEIVERS", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел приемников переименован.", lambda text: rename_heading(text, "Приемники данных", "Итоговые объекты проекта")),
    Mutation("T01_RENAME_FLOW", "T01", 2, "low", "Нарушен обязательный шаблон", "Обязательный раздел схемы потока переименован.", lambda text: rename_heading(text, "Схема потоков данных", "Общая архитектура")),
    Mutation("T01_STEP3_PLACEHOLDER", "T01", 2, "low", "В шаблоне остался плейсхолдер", "Название третьего шага заменено незаполненной подсказкой шаблона.", lambda text: rename_heading(text, "Шаг 3.", "Шаг 3. <Наименование шага 3>", prefix=True)),
    Mutation("T01_RENAME_KEYS", "T01", 2, "low", "Нарушен обязательный шаблон", "Раздел формирования Kafka key/HDFS partition невозможно сопоставить по названию.", lambda text: rename_heading(text, "Формирование ключа (kafka) / партиции (hdfs)", "Технические параметры размещения")),
    Mutation("T01_RENAME_EXAMPLE", "T01", 2, "low", "Нарушен обязательный шаблон", "Раздел примера данных переименован в несопоставимый заголовок.", lambda text: rename_heading(text, "Пример данных", "Демонстрационный фрагмент")),
    Mutation("T01_DROP_STRUCTURE_COLUMN", "T01", 2, "medium", "Нарушена таблица обязательного шаблона", "Из таблицы маппинга удалена обязательная колонка комментариев.", drop_structure_comment_column),
    Mutation("T01_RENAME_SOURCE_AND_RECEIVER", "T01", 2, "medium", "Не распознаются входы и выходы", "Одновременно переименованы два обязательных раздела шаблона: источники и приемники.", sequence(rename_edit("Источники данных", "Входной контур"), rename_edit("Приемники данных", "Выходной контур"))),
    Mutation("T01_RENAME_FLOW_AND_ALGORITHM", "T01", 2, "medium", "Нарушена структура описания обработки", "Схема потока и алгоритм обработки одновременно вынесены под нестандартные заголовки.", sequence(rename_edit("Схема потоков данных", "Архитектурный набросок"), rename_edit("Алгоритм обработки потока", "Внутренняя реализация"))),
    Mutation("T01_RENAME_STRUCTURE_AND_EXAMPLE", "T01", 2, "medium", "Не распознаются контракт и пример", "Обязательные разделы структуры результата и примера данных получили несопоставимые названия.", sequence(rename_edit("Структура данных", "Описание колонок", prefix=True), rename_edit("Пример данных", "Тестовая выборка"))),
    Mutation("T01_RENAME_DDL_AND_FAQ", "T01", 2, "medium", "Потеряны два обязательных раздела", "DDL и FAQ присутствуют по содержанию, но их обязательные заголовки заменены произвольными.", sequence(rename_edit("DDL", "Физическая реализация"), rename_edit("FAQ", "Заметки команды"))),
    Mutation("T01_ALL_STEPS_AS_PLACEHOLDERS", "T01", 2, "high", "Не заполнены названия этапов алгоритма", "Все три обязательных шага сохранены как шаблонные плейсхолдеры вместо фактических названий.", sequence(rename_edit("Шаг 1.", "Шаг 1. <Наименование шага 1>", prefix=True), rename_edit("Шаг 2.", "Шаг 2. <Наименование шага 2>", prefix=True), rename_edit("Шаг 3.", "Шаг 3. <Наименование шага 3>", prefix=True))),
    Mutation("T01_RENAME_KEYS_AND_HISTORY", "T01", 2, "medium", "Нарушены технический и служебный разделы", "Раздел ключей/партиций и история изменений переименованы так, что не сопоставляются с шаблоном.", sequence(rename_edit("Формирование ключа (kafka) / партиции (hdfs)", "Размещение результата"), rename_edit("История изменений", "Журнал документа"))),
    Mutation("T01_RENAME_THREE_DATA_SECTIONS", "T01", 2, "high", "Нарушен блок описания данных", "Сразу три обязательных раздела источников, справочников и приемников заменены внутренними названиями.", sequence(rename_edit("Источники данных", "Поставщики"), rename_edit("Источники обогащения данных", "Lookup-объекты"), rename_edit("Приемники данных", "Публикации"))),
    Mutation("T01_DROP_COLUMN_AND_RENAME_HISTORY", "T01", 2, "high", "Одновременно нарушены таблица и служебный раздел", "У таблицы структуры удалена обязательная колонка, а история изменений переименована.", sequence(drop_structure_comment_column, rename_edit("История изменений", "Журнал документа"))),
]


UNIVERSAL_MUTATIONS = [
    Mutation("U01_SCOPE_OUTPUT_GAP", "U01", 3, "high", "Заявленный результат не обеспечен контрактом", "В цель добавлен прогноз, которого нет в выходной схеме и алгоритме.", lambda text: append_intro_field(text, "Решаемая проблема", "Результат также должен обеспечивать прогноз значений на следующие 30 дней.")),
    Mutation("U02_UNDEFINED_ACTIVE", "U02", 3, "medium", "Не определен термин «активная запись»", "Введен влияющий на выборку термин без формального определения.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Во всех расчетах используются только активные записи источников.")),
    Mutation("U03_CONFLICTING_SOURCES", "U03", 3, "high", "Не задан источник истины", "При конфликте источников разрешен произвольный выбор.", lambda text: append_intro_field(text, "Системы-источники", "При расхождении значений между источниками допускается использовать значение любого из них.")),
    Mutation("U04_GRAIN_CONTRADICTION", "U04", 3, "high", "Противоречиво определена гранулярность", "Одна строка одновременно названа событием и агрегатом за период.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Одна строка результата одновременно соответствует отдельному событию и агрегату за расчетный период.")),
    Mutation("U05_AMBIGUOUS_MAPPING", "U05", 3, "medium", "Неоднозначен маппинг поля", "Разрешено брать значение из любого одноименного атрибута без приоритета.", lambda text: insert_after_heading(text, "Структура данных", "Если одноименное поле найдено в нескольких источниках, выбирается любое доступное значение.")),
    Mutation("U06_ARBITRARY_OPERATION_ORDER", "U06", 3, "high", "Не определен порядок операций", "Фильтрация и обогащение разрешены в любом порядке, хотя это может менять состав данных.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.")),
    Mutation("U07_OPEN_TIME_BOUNDARY", "U07", 3, "medium", "Не определены границы периода", "Относительный период не задает опорное время и включение граничных значений.", lambda text: insert_after_heading(text, "Шаг 1. Фильтрация данных", "Дополнительно учитываются события только за последние 7 дней." , prefix=True)),
    Mutation("U08_JOIN_CARDINALITY_GAP", "U08", 3, "high", "Не определена кардинальность обогащения", "При нескольких совпадениях сохраняются все строки, что может размножить результат.", lambda text: insert_after_heading(text, "Шаг 2. Обогащение данных", "При нескольких совпадениях со справочником в результат передаются все найденные варианты.", prefix=True)),
    Mutation("U09_NONDETERMINISTIC_LAST", "U09", 3, "high", "Недетерминирован выбор последней записи", "При одинаковом времени выбор оставлен произвольным.", lambda text: insert_after_heading(text, "Шаг 3.", "Если несколько последних записей имеют одинаковое время, сохраняется любая из них.", prefix=True)),
    Mutation("U10_CONFLICTING_ZERO_RULES", "U10", 3, "high", "Конфликтуют правила для нулевого значения", "Для одного состояния одновременно заданы два разных результата.", lambda text: insert_after_heading(text, "Шаг 3.", "Нулевой показатель записывается как 0; одновременно нулевое значение считается неизвестным и записывается как NULL.", prefix=True)),
    Mutation("U11_TIMEZONE_AMBIGUITY", "U11", 3, "high", "Не определен часовой пояс", "Один timestamp разрешено интерпретировать в двух часовых поясах.", lambda text: insert_after_heading(text, "Алгоритм обработки потока", "Временные метки могут интерпретироваться как UTC или московское время в зависимости от реализации.")),
    Mutation("U12_UNSPECIFIED_UNITS", "U12", 3, "medium", "Не определены единицы числовых полей", "Единицы значимых показателей переданы на усмотрение потребителя.", lambda text: insert_after_heading(text, "Структура данных", "Единицы измерения числовых показателей определяются каждым потребителем самостоятельно.")),
    Mutation("U13_ARBITRARY_BAD_DATA", "U13", 3, "medium", "Не определена обработка невалидных данных", "Одинаково невалидные записи могут быть сохранены или удалены.", lambda text: insert_after_heading(text, "Шаг 1. Фильтрация данных", "Некорректную запись разрешается либо исключить, либо сохранить без изменений.", prefix=True)),
    Mutation("U14_NON_IDEMPOTENT_RETRY", "U14", 3, "high", "Не определена семантика повторного запуска", "Повтор может выполнять append или overwrite и поэтому менять число строк.", lambda text: insert_after_heading(text, "Формирование ключа (kafka) / партиции (hdfs)", "При повторном запуске партиция может дополняться или перезаписываться по выбору оператора.")),
    Mutation("U15_UNVERSIONED_SCHEMA_CHANGE", "U15", 3, "high", "Допущено несовместимое изменение схемы", "Тип поля разрешено менять без новой версии контракта.", lambda text: insert_after_heading(text, "FAQ", "Тип существующего поля можно изменить без выпуска новой версии, если его имя сохраняется.")),
    Mutation("U16_IMPOSSIBLE_ZERO_LATENCY", "U16", 3, "medium", "Задано нереализуемое требование задержки", "При пиковом объеме требуется строго нулевая задержка, что невозможно объективно выполнить.", lambda text: append_intro_field(text, "Нефункциональные требования", "При любом пиковом объеме задержка обработки должна быть строго 0 секунд.")),
    Mutation("U17_DDL_TYPE_CONFLICT", "U17", 3, "low", "Тип поля расходится с DDL", "Тип первого целевого поля изменен только в DDL и конфликтует с таблицей структуры.", corrupt_first_ddl_type),
    Mutation("U18_VAGUE_ACCEPTANCE", "U18", 3, "low", "Непроверяемый критерий качества", "Добавлена метрика без формулы, порога и периода измерения.", lambda text: append_intro_field(text, "Продуктовые метрики", "Дополнительная метрика: качество результата должно быть высоким.")),
    Mutation("U19_UNMASKED_SENSITIVE_LOG", "U19", 3, "medium", "Чувствительные данные сохраняются без ограничений", "Полный payload с идентификаторами разрешено бессрочно писать в журнал.", lambda text: insert_after_heading(text, "FAQ", "Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.")),
    Mutation("U01_ALERTS_WITHOUT_OUTPUT", "U01", 3, "high", "Обещаны алерты без выходного контракта", "В цели появились персональные алерты, но ни приемник, ни схема доставки, ни поля уведомления не определены.", sequence(intro_edit("Решаемая проблема", "Результат обязан отправлять персональные алерты владельцам затронутых объектов."), section_edit("Приемники данных", "Канал уведомлений, адресаты и контракт сообщения будут определены после запуска."))),
    Mutation("U01_HISTORY_OUTSIDE_RESULT", "U01", 3, "high", "Заявлена история, которой нет в результате", "Документ обещает аудит всех прошлых состояний, хотя приемник хранит только актуальную версию и алгоритм истории не формирует.", sequence(intro_edit("Решаемая проблема", "Пользователь должен видеть полную историю каждого изменения результата."), section_edit("Приемники данных", "В приемнике сохраняется только последняя опубликованная версия без журнала изменений."))),
    Mutation("U02_TWO_MEANINGS_OF_CONFIRMED", "U02", 3, "high", "Термин «подтвержденный» определен противоречиво", "Один термин в разных частях документа означает прохождение проверки и наличие двух событий, что меняет состав результата.", sequence(section_edit("Алгоритм обработки потока", "Подтвержденной считается запись, прошедшая синтаксическую валидацию."), section_edit("FAQ", "Подтвержденная запись — запись, для которой получено не менее двух событий из источника."))),
    Mutation("U02_UNDEFINED_CURRENT_VERSION", "U02", 3, "medium", "Не определена «текущая версия»", "Выбор данных и повторный расчет зависят от текущей версии, но точка времени и правило ее определения отсутствуют.", sequence(section_edit("Шаг 2. Обогащение данных", "Для обогащения всегда используется текущая версия записи.", prefix=True), section_edit("FAQ", "Понятие текущей версии определяется владельцем источника при каждом запуске."))),
    Mutation("U03_SOURCE_PRIORITY_CHANGES", "U03", 3, "high", "Приоритет источников меняется между этапами", "До обогащения главным объявлен первый источник, после него — второй; правила разрешения расхождений нет.", sequence(section_edit("Алгоритм обработки потока", "До обогащения авторитетным считается первый источник из таблицы."), section_edit("Шаг 2. Обогащение данных", "После JOIN при расхождении всегда используется значение второго источника.", prefix=True))),
    Mutation("U03_UNDECLARED_AUTHORITATIVE_FEED", "U03", 3, "high", "Алгоритм зависит от неописанного источника истины", "Финальные значения должны сверяться с внешним master-feed, которого нет среди источников и контракт которого неизвестен.", sequence(intro_edit("Системы-источники", "Финальным источником истины является корпоративный master-feed."), section_edit("Шаг 3.", "Перед записью значения заменяются данными master-feed; способ подключения и поля не описаны.", prefix=True))),
    Mutation("U04_KEY_OMITS_DECLARED_DIMENSION", "U04", 3, "high", "Бизнес-ключ не соответствует гранулярности", "В гранулярность добавлено измерение, но бизнес-ключ и дедупликация его не учитывают, поэтому разные строки схлопываются.", sequence(section_edit("Алгоритм обработки потока", "Гранулярность дополнительно включает канал поступления source_channel."), section_edit("Формирование ключа (kafka) / партиции (hdfs)", "При построении бизнес-ключа source_channel намеренно не учитывается."))),
    Mutation("U04_TWO_GRAINS_FOR_RECEIVER", "U04", 3, "high", "Для одного приемника заданы две гранулярности", "Алгоритм публикует строки по событиям, а описание приемника требует суточный агрегат без правила преобразования между уровнями.", sequence(section_edit("Приемники данных", "Одна строка приемника является суточным агрегатом объекта."), section_edit("Алгоритм обработки потока", "Каждое прошедшее фильтр событие записывается отдельной строкой без агрегации."))),
    Mutation("U05_TWO_FORMULAS_FOR_TARGET", "U05", 3, "high", "Одно поле рассчитывается двумя способами", "В маппинге и алгоритме заявлены несовместимые формулы одного выходного показателя без приоритета.", sequence(section_edit("Структура данных", "Основной показатель рассчитывается как сумма исходных значений."), section_edit("Шаг 3.", "Тот же основной показатель рассчитывается как среднее исходных значений; приоритет формул не задан.", prefix=True))),
    Mutation("U05_HIDDEN_INTERMEDIATE_FIELD", "U05", 3, "high", "Маппинг зависит от необъявленного промежуточного поля", "Расчет использует нормализованный атрибут, которого нет ни в источниках, ни среди шагов его получения.", sequence(section_edit("Структура данных", "Для заполнения результата требуется промежуточное поле normalized_source_value."), section_edit("Шаг 3.", "normalized_source_value передается в приемник без дополнительного преобразования.", prefix=True))),
    Mutation("U06_DEDUP_AFTER_AGGREGATION", "U06", 3, "high", "Дедупликация выполняется после агрегации", "Дубли сначала попадают в сумму, а удаляются только из уже агрегированного результата, поэтому восстановить корректные показатели невозможно.", sequence(section_edit("Шаг 3.", "Сначала все входные строки агрегируются без дедупликации.", prefix=True), section_edit("Шаг 3.", "После расчета агрегатов дубли удаляются по идентификатору исходного события.", prefix=True))),
    Mutation("U06_TWO_OPERATION_ORDERS", "U06", 3, "high", "В документе заданы два порядка обработки", "Схема требует фильтрацию до JOIN, а текст алгоритма — JOIN до фильтрации; результаты могут различаться.", sequence(section_edit("Схема потоков данных", "Обязательный порядок: фильтрация → обогащение → агрегация."), section_edit("Алгоритм обработки потока", "Реализация сначала обогащает все записи, затем применяет входные фильтры."))),
    Mutation("U07_OVERLAPPING_PERIODS", "U07", 3, "high", "Соседние периоды пересекаются", "Конец одного расчетного периода и начало следующего включены одновременно, поэтому граничная запись считается дважды.", sequence(section_edit("Шаг 1. Фильтрация данных", "Период M включает записи с timestamp >= start_M и timestamp <= end_M.", prefix=True), section_edit("Шаг 1. Фильтрация данных", "Период M+1 начинается с timestamp >= end_M, включая ту же границу.", prefix=True))),
    Mutation("U07_NULL_FILTER_DISAGREEMENT", "U07", 3, "high", "NULL одновременно включается и исключается", "Два правила фильтрации по-разному обрабатывают одно и то же отсутствующее значение.", sequence(section_edit("Шаг 1. Фильтрация данных", "Записи с NULL status включаются как состояние по умолчанию.", prefix=True), section_edit("Шаг 1. Фильтрация данных", "Любая запись с NULL status исключается до применения default.", prefix=True))),
    Mutation("U08_PARTIAL_KEY_AND_FANOUT", "U08", 3, "high", "Неполный JOIN размножает строки", "Из составного ключа исключена версия, а все найденные совпадения передаются дальше, создавая неконтролируемый fan-out.", sequence(section_edit("Шаг 2. Обогащение данных", "JOIN выполняется только по идентификатору без даты и версии справочника.", prefix=True), section_edit("Шаг 2. Обогащение данных", "При нескольких совпадениях сохраняются все комбинации без дополнительной проверки.", prefix=True))),
    Mutation("U08_INNER_JOIN_WITH_FALLBACK", "U08", 3, "high", "Тип JOIN противоречит fallback", "Алгоритм требует INNER JOIN, но следующая ветка описывает заполнение результата при отсутствии справочника — недостижимое состояние.", sequence(section_edit("Шаг 2. Обогащение данных", "Используется INNER JOIN, поэтому строки без справочника удаляются.", prefix=True), section_edit("Шаг 2. Обогащение данных", "Если соответствие отсутствует, строка сохраняется со значением UNKNOWN.", prefix=True))),
    Mutation("U09_DISTINCT_BEFORE_REVISION", "U09", 3, "high", "DISTINCT уничтожает порядок ревизий", "До выбора последней ревизии удаляются различающиеся версии, после чего победителя предлагается выбирать по уже потерянному полю.", sequence(section_edit("Шаг 3.", "Перед обработкой ревизий применяется DISTINCT по бизнес-полям без revision.", prefix=True), section_edit("Шаг 3.", "Затем для каждого ключа выбирается запись с максимальной revision.", prefix=True))),
    Mutation("U09_GROUPING_MISSES_DIMENSION", "U09", 3, "high", "GROUP BY теряет часть ключа", "Одна размерность объявлена частью гранулярности, но исключена из группировки и выбирается произвольно.", sequence(section_edit("Алгоритм обработки потока", "Полная гранулярность включает дополнительное измерение category_code."), section_edit("Шаг 3.", "GROUP BY выполняется без category_code; в результат берется любое значение категории.", prefix=True))),
    Mutation("U10_OVERLAPPING_PRIORITY", "U10", 3, "high", "Пересекающиеся правила не имеют приоритета", "Для одной записи могут одновременно сработать две ветки с разными результатами, а порядок применения не определен.", sequence(section_edit("Шаг 3.", "Если score >= 50, установить статус REVIEW.", prefix=True), section_edit("Шаг 3.", "Если score <= 80, установить статус ACCEPT; приоритет при 50..80 не задан.", prefix=True))),
    Mutation("U10_MISSING_ELSE", "U10", 3, "medium", "Не определена ветка по умолчанию", "Правила покрывают только два известных состояния, но контракт разрешает другие значения и не задает результат для них.", sequence(section_edit("Шаг 3.", "Для state=A записывается 1, для state=B записывается 0.", prefix=True), section_edit("FAQ", "Новые значения state могут появляться без предварительного уведомления; поведение для них не определено."))),
    Mutation("U11_CURRENT_REFERENCE_FOR_BACKFILL", "U11", 3, "high", "Backfill использует неправильный исторический срез", "Онлайн-расчет использует temporal-версию справочника, а пересчет прошлого — текущий snapshot, поэтому история меняется без входных событий.", sequence(section_edit("Шаг 2. Обогащение данных", "Онлайн выбирает версию справочника на event time.", prefix=True), section_edit("FAQ", "Любой backfill прошлого использует последний доступный snapshot справочника."))),
    Mutation("U11_LATE_EVENT_TWO_POLICIES", "U11", 3, "high", "Позднее событие обрабатывается двумя способами", "Одно правило исключает событие после watermark навсегда, другое требует включить его автоматическим replay.", sequence(section_edit("Алгоритм обработки потока", "События после watermark окончательно отбрасываются и не меняют результат."), section_edit("FAQ", "Все поздние события автоматически включаются ближайшим replay без ограничений по возрасту."))),
    Mutation("U12_SCALE_AND_ROUNDING_CONFLICT", "U12", 3, "high", "Точность поля не поддерживает расчет", "Формула требует сохранить шесть знаков, а целевой контракт ограничен двумя; правило округления или допустимая потеря точности не заданы.", sequence(section_edit("Структура данных", "Расчетный коэффициент должен сохранять ровно шесть знаков после запятой."), section_edit("DDL", "Для этого коэффициента используется DECIMAL с двумя знаками после запятой; режим округления не определен."))),
    Mutation("U12_DEFAULT_OUTSIDE_ENUM", "U12", 3, "high", "Default не входит в допустимый домен", "Для отсутствующего значения задан специальный код, который одновременно запрещен перечислением допустимых значений.", sequence(section_edit("Структура данных", "Допустимые значения status: ACTIVE и INACTIVE."), section_edit("Шаг 3.", "При NULL status записывается значение UNKNOWN.", prefix=True))),
    Mutation("U13_BAD_DATA_BECOMES_VALID", "U13", 3, "high", "Ошибки маскируются валидным default", "Испорченные числовые и временные значения заменяются нулями, неотличимыми от настоящих данных, и проходят дальнейший расчет.", sequence(section_edit("Шаг 1. Фильтрация данных", "Нераспознаваемое число заменяется на 0 и считается валидным.", prefix=True), section_edit("Шаг 1. Фильтрация данных", "Нераспознаваемое время заменяется началом эпохи и передается дальше.", prefix=True))),
    Mutation("U13_EMPTY_INPUT_PUBLISHES_ZERO", "U13", 3, "high", "Пустой вход превращается в реальные нулевые данные", "При отсутствии источника алгоритм публикует синтетические нулевые строки без признака неполноты, скрывая сбой поставки.", sequence(section_edit("Алгоритм обработки потока", "Пустой вход считается успешным расчетом."), section_edit("Шаг 3.", "Для каждой ожидаемой группы при пустом входе создается строка с нулевыми показателями без технического флага.", prefix=True))),
    Mutation("U14_APPEND_RETRY_DUPLICATES", "U14", 3, "high", "Retry повторно добавляет опубликованные строки", "Первый запуск пишет append, а повтор не проверяет идентификатор загрузки и снова добавляет тот же набор.", sequence(section_edit("Формирование ключа (kafka) / партиции (hdfs)", "Каждый запуск записывает результат в режиме append."), section_edit("FAQ", "Retry повторяет запись целиком; batch_id в приемнике не хранится и дубли не удаляются."))),
    Mutation("U14_DELETE_AND_CORRECTION_IGNORED", "U14", 3, "high", "Повторная обработка не применяет исправления", "Replay добавляет новые ревизии, но не удаляет старые строки и игнорирует tombstone, поэтому состояние зависит от истории запусков.", sequence(section_edit("Алгоритм обработки потока", "Replay добавляет исправленные события рядом с ранее опубликованными."), section_edit("FAQ", "DELETE и tombstone подтверждаются, но соответствующие строки приемника не изменяются."))),
    Mutation("U15_REQUIRED_FIELD_WITHOUT_MIGRATION", "U15", 3, "high", "Добавлено обязательное поле без миграции", "Новая версия требует NOT NULL-поле, но не задает default, backfill истории и порядок обновления потребителей.", sequence(section_edit("Структура данных", "В следующем релизе добавляется обязательное поле contract_flag NOT NULL без default."), section_edit("FAQ", "Старые партиции и потребители остаются без изменений; план миграции не предусмотрен."))),
    Mutation("U15_RENAME_CHANGES_MEANING", "U15", 3, "high", "Поле переименовывается вместе со смыслом без версии", "Существующее поле получает новое имя и другую семантику в той же версии схемы, а старые данные не преобразуются.", sequence(section_edit("Структура данных", "Поле total_count переименовывается в successful_count и начинает учитывать только успешные записи."), section_edit("FAQ", "Версия контракта при этом не меняется, исторические значения сохраняют прежний смысл."))),
    Mutation("U16_WATERMARK_EXCEEDS_SLA", "U16", 3, "high", "SLA меньше необходимого ожидания данных", "Результат требуется публиковать раньше закрытия watermark, хотя до watermark он еще может измениться.", sequence(intro_edit("Продуктовые метрики", "Финальный результат публикуется не позднее чем через 2 минуты."), intro_edit("Нефункциональные требования", "Watermark закрывает расчет только через 30 минут после периода."))),
    Mutation("U16_REPLAY_LONGER_THAN_RETENTION", "U16", 3, "high", "Горизонт replay превышает retention", "Документ обещает автоматический пересчет периода, для которого исходные данные уже гарантированно удалены.", sequence(intro_edit("Нефункциональные требования", "Retention авторитетного сырья составляет 7 суток."), section_edit("FAQ", "Автоматический replay гарантирован для любых периодов за последние 90 суток."))),
    Mutation("U17_EXAMPLE_OUTSIDE_CONTRACT", "U17", 3, "medium", "Пример нарушает контракт поля", "В пример записано значение за пределами описанного домена, поэтому пример не может быть получен заявленным алгоритмом.", sequence(cell_edit("Пример данных", 0, "VALUE_OUTSIDE_DOCUMENTED_DOMAIN"), section_edit("Пример данных", "Первая строка считается штатным результатом и должна приниматься без преобразований."))),
    Mutation("U17_SOURCE_FIELD_NOT_DECLARED", "U17", 3, "high", "Маппинг ссылается на отсутствующее поле", "В таблице структуры указан новый source field, которого нет в описании источников и алгоритме, тогда как DDL сохраняет прежний контракт.", sequence(cell_edit("Структура данных", 4, "undeclared_source_attribute"), section_edit("DDL", "DDL остается без изменений; undeclared_source_attribute вычислять или хранить не требуется."))),
    Mutation("U18_NO_PASS_FAIL_THRESHOLDS", "U18", 3, "medium", "Приемка зависит от субъективной оценки", "Два ключевых свойства результата объявлены обязательными, но для них нет формулы, порога, периода и источника проверки.", sequence(intro_edit("Продуктовые метрики", "Полнота результата должна быть достаточной для бизнеса."), intro_edit("Продуктовые метрики", "Свежесть данных должна оцениваться как приемлемая владельцем продукта."))),
    Mutation("U18_CONTROLS_WITHOUT_EXPECTED_VALUES", "U18", 3, "high", "Контрольные показатели нельзя интерпретировать", "Предлагается считать расхождения и дубли, но не заданы допустимые значения и действие при нарушении.", sequence(section_edit("FAQ", "После загрузки рассчитываются число дублей и расхождение с источником."), section_edit("FAQ", "Порог срабатывания, период сравнения и блокировка публикации определяются вручную после проверки."))),
    Mutation("U19_PUBLIC_RAW_IDENTIFIERS", "U19", 3, "high", "Сырые идентификаторы публикуются без ограничений", "Результат содержит прямые идентификаторы, доступ выдан всем сотрудникам, а назначение и маскирование не определены.", sequence(section_edit("Структура данных", "В результат дополнительно передаются исходные персональные идентификаторы без токенизации."), section_edit("FAQ", "Чтение таблицы разрешено общей корпоративной роли без согласования цели доступа."))),
    Mutation("U19_SENSITIVE_DEBUG_AND_BACKUP", "U19", 3, "high", "Чувствительные данные бесконтрольно копируются", "Полный payload пишется в debug-таблицу и резервную копию без владельца, ACL, маскирования и согласованного удаления.", sequence(section_edit("FAQ", "При ошибке полный исходный payload сохраняется в общей debug-таблице."), section_edit("FAQ", "Debug-таблица ежедневно копируется в бессрочный backup; правила доступа и удаления отсутствуют."))),
]


def write_error_catalog() -> None:
    catalog = [
        {
            "mutation_id": item.mutation_id,
            "error_type_id": item.error_type_id,
            "source_type": item.source_type,
            "difficulty": item.difficulty,
            "title": item.title,
            "problem": item.description,
        }
        for item in DOMAIN_MUTATIONS + TEMPLATE_MUTATIONS + UNIVERSAL_MUTATIONS
    ]
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


def applicable_by_type(mutations: list[Mutation], clean_text: str) -> dict[str, list[Mutation]]:
    groups: dict[str, list[Mutation]] = {}
    for item in applicable(mutations, clean_text):
        groups.setdefault(item.error_type_id, []).append(item)
    return groups


def choose_variants(
    groups: dict[str, list[Mutation]], count: int, rng: random.Random, usage: Counter[str]
) -> list[Mutation]:
    candidates = list(groups.values())
    rng.shuffle(candidates)
    candidates.sort(key=lambda variants: sum(usage[item.mutation_id] for item in variants))
    if count > len(candidates):
        raise ValueError("not enough distinct applicable error types")

    selected = []
    for variants in candidates[:count]:
        minimum = min(usage[item.mutation_id] for item in variants)
        selected.append(rng.choice([item for item in variants if usage[item.mutation_id] == minimum]))
    return selected


def choose_mutations(
    count: int,
    case_index: int,
    rng: random.Random,
    clean_text: str,
    usage: Counter[str],
) -> list[Mutation]:
    if count == 0:
        return []

    domain = applicable_by_type(DOMAIN_MUTATIONS, clean_text)
    universal = applicable_by_type(UNIVERSAL_MUTATIONS, clean_text)
    template = applicable_by_type(TEMPLATE_MUTATIONS, clean_text)
    add_template = count >= 4 and (case_index % 3 != 0 or count >= 24)
    remaining = count - add_template
    domain_count = min(
        max(round(remaining * (0.28 + 0.06 * (case_index % 3))), remaining - len(universal), 0),
        len(domain),
        remaining,
    )
    selected = choose_variants(domain, domain_count, rng, usage)
    selected += choose_variants(universal, remaining - domain_count, rng, usage)
    if add_template:
        selected += choose_variants(template, 1, rng, usage)
    return selected


def main() -> None:
    clean_files = sorted(CLEAN_DIR.glob("clean_*.md"))
    if len(clean_files) != 10:
        raise SystemExit(f"expected 10 clean documents, found {len(clean_files)}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write_error_catalog()

    manifest_cases = []
    mutation_usage: Counter[str] = Counter()

    for case_index in range(1, 51):
        clean_path = clean_files[(case_index - 1) // 5]
        clean_text = clean_path.read_text(encoding="utf-8")
        error_count = ERROR_COUNTS[case_index - 1]
        rng = random.Random(SEED * 100 + case_index)
        selected = choose_mutations(error_count, case_index, rng, clean_text, mutation_usage)
        if len(selected) != error_count:
            raise RuntimeError(f"selection mismatch for case {case_index}")
        mutation_usage.update(item.mutation_id for item in selected)

        text = clean_text
        findings = []

        def mutation_phase(item: Mutation) -> int:
            if "DROP_COLUMN" in item.mutation_id:
                return 0
            if item.error_type_id in {"D04", "D05"}:
                return 1
            return {1: 2, 3: 3, 2: 4}[item.source_type]

        ordered = sorted(selected, key=mutation_phase)
        for item in ordered:
            text, evidence = item.apply(text)
            findings.append(
                {
                    "error_type_id": item.error_type_id,
                    "source_type": item.source_type,
                    "difficulty": item.difficulty,
                    "evidence_quote": evidence,
                    "title": item.title,
                    "problem": item.description,
                }
            )

        for finding in findings:
            occurrences = text.count(finding["evidence_quote"])
            if occurrences != 1:
                raise RuntimeError(
                    f"case {case_index}: evidence occurs {occurrences} times: "
                    f"{finding['evidence_quote']!r}; selected={[item.mutation_id for item in ordered]}"
                )

        document_id = f"case_{case_index:03d}"
        md_path = OUT_DIR / f"{document_id}.md"
        json_path = OUT_DIR / f"{document_id}.json"
        md_path.write_text(text, encoding="utf-8")
        metadata = {
            "document_id": document_id,
            "source_clean_document": clean_path.name,
            "error_count": len(findings),
            "errors": findings,
        }
        json_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        manifest_cases.append(
            {
                "document_id": document_id,
                "source_clean_document": clean_path.name,
                "error_count": len(findings),
            }
        )

    manifest = {
        "seed": SEED,
        "error_count_distribution": {
            str(count): ERROR_COUNTS.count(count) for count in sorted(set(ERROR_COUNTS))
        },
        "cases": manifest_cases,
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
