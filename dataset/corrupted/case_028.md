# Потоковые данные/витрины

| **Общие сведения** | Часовая витрина открытых аварий базовых станций. Одна строка содержит число уникальных аварий и затронутых сот сектора сети за один час UTC в разрезе региона, площадки, вендора, серьёзности и кода аварии. |
| :--- | :--- |
| **Решаемая проблема** | Единый проверяемый источник для оперативного мониторинга массовых аварий RAN и расчёта нагрузки региональных дежурных смен. В расчёт входят только события открытия аварии штатных площадок мобильной сети; тестовые и отменённые события не входят. Результат обязан отправлять персональные алерты владельцам затронутых объектов. |
| **Продуктовые метрики** | Доля валидных событий, доступных в витрине не позднее 10 минут после конца часа, не менее 99,5%; расхождение `FIELD_ALARMS_CNT` с принятыми уникальными событиями источника — 0; доля событий без найденной площадки — не более 0,1%. Метрики считаются за календарные сутки UTC. |
| **Заказчики** | Центр управления сетью MTS Big Data, дирекция эксплуатации RAN. |
| **Нефункциональные требования** | До 18 млн входных событий в сутки и 35 тыс. событий/с; расчёт по event time; первичная публикация часа — H+10 минут, финализация — H+2 часа 10 минут; Kafka retention источника — 35 дней, хранение витрины — 400 дней; доступность — 99,9% в месяц; часовая партиция должна допускать безопасный повторный расчёт. При любом пиковом объеме задержка обработки должна быть строго 0 секунд. |
| **Системы-источники** | Платформа `RAN_FAULT_HUB`, публикующая унифицированные события открытия и закрытия аварий 2G/3G/4G/5G. Финальным источником истины является корпоративный master-feed. |
| **Data Catalog** | [Карточка продукта RAN Alarm Hourly](https://datacatalog.corp.mts.ru/products/net-ran-alarm-hourly) |
| **Исходники проекта** | [GitLab: net/ran-alarm-hourly](https://gitlab.corp.mts.ru/bigdata/net/ran-alarm-hourly) |
| **Команда** | Анна Лебедева — аналитик; Сергей Котов — разработчик; Мария Волкова — QA; Илья Орлов — Product Owner. |
| **JIRA** | [NETDATA-6412 — Часовая витрина аварий RAN](https://jira.corp.mts.ru/browse/NETDATA-6412) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_RAN_ALARM_V2`, полные имена топика одинаковы во всех регионах; регион содержится в payload | Kafka; кластер не указан | Data Catalog: ссылка отсутствует | JSON; схема, версия и framing не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка на справочник отсутствует | Используется для обогащения; поля и версия не перечислены |

### Приемники данных

Канал уведомлений, адресаты и контракт сообщения будут определены после запуска.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_NET.TABLE_RAN_ALARM_HOURLY` | HDFS; путь не указан | [Data Catalog: TABLE_RAN_ALARM_HOURLY](https://datacatalog.corp.mts.ru/tables/CDM_NET/TABLE_RAN_ALARM_HOURLY) | Apache Iceberg v2, файлы Parquet с `SNAPPY`; схема `CDM_NET.TABLE_RAN_ALARM_HOURLY` версии 1.0 из Hive Metastore `hms-net-prod-01`; запись Spark DataFrame writer v2 с `overwritePartitions`, чтение Iceberg reader по snapshot metadata. |

### Схема потоков данных

Обязательный порядок: фильтрация → обогащение → агрегация.

`RAN_FAULT_HUB` → Kafka `kafka-net-prod-01.TOPIC_RAN_ALARM_V2` → Flink job `ran-alarm-normalizer` → дедупликация и SCD2-обогащение → Spark job `ran-alarm-hourly` → Iceberg `CDM_NET.TABLE_RAN_ALARM_HOURLY` в `/warehouse/cdm/net/ran_alarm_hourly/`.

Гранулярность результата: одна строка на `(FIELD_BIZ_DATE, FIELD_HOUR_UTC, FIELD_REGION_CODE, FIELD_SITE_ID, FIELD_VENDOR_NAME, FIELD_SEVERITY, FIELD_ALARM_CODE)`. Это полный бизнес-ключ строки.

### Алгоритм обработки потока

Реализация сначала обогащает все записи, затем применяет входные фильтры.

Replay добавляет исправленные события рядом с ранее опубликованными.

Пустой вход считается успешным расчетом.

Полная гранулярность включает дополнительное измерение category_code.

Подтвержденной считается запись, прошедшая синтаксическую валидацию.

Расчёт использует `event_ts` как время события и `ingest_ts` только для дедупликации и контроля задержки. Все timestamps источника имеют формат epoch milliseconds UTC. Интервал часа полуоткрытый: `[H, H + 1 hour)`.

#### Шаг 1. Фильтрация данных

В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.

#### Шаг 2. Обогащение данных

При нескольких совпадениях со справочником в результат передаются все найденные варианты.

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. Трансформация и агрегация

Для каждой ожидаемой группы при пустом входе создается строка с нулевыми показателями без технического флага.

GROUP BY выполняется без category_code; в результат берется любое значение категории.

Перед записью значения заменяются данными master-feed; способ подключения и поля не описаны.

normalized_source_value передается в приемник без дополнительного преобразования.

1. Получить `FIELD_BIZ_DATE = CAST(event_ts AT TIME ZONE 'UTC' AS DATE)` и `FIELD_HOUR_UTC = EXTRACT(HOUR FROM event_ts AT TIME ZONE 'UTC')` со значениями 0–23.
2. Сгруппировать по полному бизнес-ключу результата.
3. Рассчитать `FIELD_ALARMS_CNT = COUNT(DISTINCT alarm_id)` и `FIELD_AFFECTED_CELLS_CNT = COUNT(DISTINCT cell_id)`. Оба значения положительны, так как группы создаются только из принятых событий.
4. Установить `FIELD_FIRST_EVENT_TS = MIN(event_ts)`, `FIELD_LAST_EVENT_TS = MAX(event_ts)`, `FIELD_PROC_TS = processing_ts` в UTC. Для каждой строки выполняется `FIELD_FIRST_EVENT_TS <= FIELD_LAST_EVENT_TS`.

#### Шаг 4. Поздние события, запись и восстановление

1. Watermark равен максимальному наблюдаемому `event_ts` минус 2 часа. События, поступившие не позднее H+2 часа 10 минут, включаются повторной агрегацией и атомарной заменой часовой партиции.
2. События после watermark не меняют автоматически финализированную партицию; они учитываются в `too_late_cnt`. Если за час `too_late_cnt > 0`, создаётся задача на backfill именно этого часа. Backfill читает offsets источника по сохранённому диапазону и заменяет партицию целиком.
3. Запись выполняется атомарным Iceberg snapshot через `overwritePartitions` для `(FIELD_BIZ_DATE, FIELD_HOUR_UTC)`. При частичном сбое snapshot не коммитится. Retry читает тот же диапазон offsets и безопасен благодаря дедупликации и полной замене партиции.
4. Исправления источника с тем же `alarm_id` и большим `ingest_ts` требуют backfill как старого, так и нового часа, если исправился `event_ts`. Удаления и tombstone для append-only источника не применимы и отклоняются как нарушение контракта. Автоматический backfill разрешён в пределах 30 дней; период возрастом 31–35 дней пересчитывается по согласованной заявке; старше 35 дней backfill невозможен из-за retention и отклонение эскалируется владельцу источника.

### Формирование ключа (kafka) / партиции (hdfs)

- Ключ входного Kafka-сообщения: `alarm_id`; producer обязан задавать его непустой UTF-8 строкой. Kafka-приёмник отсутствует.
- Iceberg/HDFS partition transform: `days(FIELD_BIZ_DATE)` и identity `FIELD_HOUR_UTC`. Полный путь таблицы: `/warehouse/cdm/net/ran_alarm_hourly/`; физические имена файлов назначает Iceberg, прямое обращение к ним запрещено.
- Распределение записи внутри партиции: hash `(FIELD_SITE_ID, FIELD_ALARM_CODE)`, 64 buckets; эта настройка не входит в бизнес-ключ.

### Структура данных

Расчетный коэффициент должен сохранять ровно шесть знаков после запятой.

Для заполнения результата требуется промежуточное поле normalized_source_value.

Поле total_count переименовывается в successful_count и начинает учитывать только успешные записи.

| Приемники |  |  | Источники |  |  |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** |
| FIELD_BIZ_DATE | DATE | Дата часа UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT |
| FIELD_HOUR_UTC | TINYINT | Час UTC, 0–23; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT |
| FIELD_REGION_CODE | STRING | Код региона или `UNKNOWN`; `NOT NULL`, 2–16 символов | `DICT_RAN_SITE_SCD` | `region_code` | STRING |
| FIELD_SITE_ID | STRING | Идентификатор площадки; `NOT NULL`, 1–32 символа | `TOPIC_RAN_ALARM_V2` | `site_id` | STRING |
| FIELD_VENDOR_NAME | STRING | Вендор или `UNKNOWN`; `NOT NULL`, до 64 символов | `DICT_RAN_SITE_SCD` | `vendor_name` | STRING |
| FIELD_SEVERITY | STRING | `CRITICAL`, `MAJOR`, `MINOR`, `WARNING`; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `severity` | STRING |
| FIELD_ALARM_CODE | STRING | Код аварии; `NOT NULL`, 2–32 символа | `TOPIC_RAN_ALARM_V2` | `alarm_code` | STRING |
| FIELD_ALARMS_CNT | BIGINT | Число уникальных `alarm_id`, > 0; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `alarm_id` | STRING |
| FIELD_AFFECTED_CELLS_CNT | BIGINT | Число уникальных `cell_id`, > 0; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `cell_id` | STRING |
| FIELD_FIRST_EVENT_TS | TIMESTAMP | Минимальное event time группы, UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT |
| FIELD_LAST_EVENT_TS | TIMESTAMP | Максимальное event time группы, UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT |
| FIELD_PROC_TS | TIMESTAMP | Время успешного запуска, UTC; `NOT NULL` | Spark | `processing_ts` | TIMESTAMP |

### Пример данных

| FIELD_BIZ_DATE | FIELD_HOUR_UTC | FIELD_REGION_CODE | FIELD_SITE_ID | FIELD_VENDOR_NAME | FIELD_SEVERITY | FIELD_ALARM_CODE | FIELD_ALARMS_CNT | FIELD_AFFECTED_CELLS_CNT | FIELD_FIRST_EVENT_TS | FIELD_LAST_EVENT_TS | FIELD_PROC_TS |
| :--- | ---: | :--- | :--- | :--- | :--- | :--- | ---: | ---: | :--- | :--- | :--- |
| 2026-08-15 | 10 | MOS | SITE-1001 | Ericsson | CRITICAL | POWER_FAIL | 3 | 3 | 2026-08-15 10:02:11 | 2026-08-15 10:41:05 | 2026-08-15 11:10:00 |
| 2026-08-15 | 10 | MOS | SITE-1001 | Ericsson | MAJOR | LINK_DOWN | 2 | 2 | 2026-08-15 10:12:00 | 2026-08-15 10:39:44 | 2026-08-15 11:10:00 |
| 2026-08-15 | 10 | SPE | SITE-2044 | Huawei | MINOR | VSWR_HIGH | 4 | 2 | 2026-08-15 10:05:09 | 2026-08-15 10:54:30 | 2026-08-15 11:10:00 |
| 2026-08-15 | 10 | KZN | SITE-3108 | Nokia | WARNING | TEMP_HIGH | 1 | 1 | 2026-08-15 10:22:18 | 2026-08-15 10:22:18 | 2026-08-15 11:10:00 |
| 2026-08-15 | 11 | MOS | SITE-1002 | Ericsson | MAJOR | LINK_DOWN | 5 | 3 | 2026-08-15 11:01:02 | 2026-08-15 11:47:57 | 2026-08-15 12:10:00 |
| 2026-08-15 | 11 | NSK | SITE-4210 | Huawei | CRITICAL | POWER_FAIL | 2 | 2 | 2026-08-15 11:04:00 | 2026-08-15 11:19:08 | 2026-08-15 12:10:00 |
| 2026-08-15 | 11 | EKB | SITE-5117 | Nokia | WARNING | FAN_WARN | 6 | 4 | 2026-08-15 11:00:33 | 2026-08-15 11:58:42 | 2026-08-15 12:10:00 |
| 2026-08-15 | 11 | UNKNOWN | SITE-NEW-77 | UNKNOWN | MINOR | CLOCK_SYNC | 1 | 1 | 2026-08-15 11:33:10 | 2026-08-15 11:33:10 | 2026-08-15 12:10:00 |
| 2026-08-16 | 0 | SAM | SITE-6104 | Ericsson | MAJOR | GPS_LOSS | 3 | 3 | 2026-08-16 00:02:01 | 2026-08-16 00:49:59 | 2026-08-16 01:10:00 |
| 2026-08-16 | 0 | UFA | SITE-7042 | Huawei | WARNING | TEMP_HIGH | 2 | 1 | 2026-08-16 00:14:14 | 2026-08-16 00:45:20 | 2026-08-16 01:10:00 |

### DDL

Для этого коэффициента используется DECIMAL с двумя знаками после запятой; режим округления не определен.

```sql
CREATE TABLE IF NOT EXISTS CDM_NET.TABLE_RAN_ALARM_HOURLY (
    FIELD_BIZ_DATE STRING NOT NULL,
    FIELD_HOUR_UTC TINYINT NOT NULL,
    FIELD_REGION_CODE STRING NOT NULL,
    FIELD_SITE_ID STRING NOT NULL,
    FIELD_VENDOR_NAME STRING NOT NULL,
    FIELD_SEVERITY STRING NOT NULL,
    FIELD_ALARM_CODE STRING NOT NULL,
    FIELD_ALARMS_CNT BIGINT NOT NULL,
    FIELD_AFFECTED_CELLS_CNT BIGINT NOT NULL,
    FIELD_FIRST_EVENT_TS TIMESTAMP NOT NULL,
    FIELD_LAST_EVENT_TS TIMESTAMP NOT NULL,
    FIELD_PROC_TS TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(FIELD_BIZ_DATE), FIELD_HOUR_UTC)
LOCATION '/warehouse/cdm/net/ran_alarm_hourly/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
);
```

Ограничения enum, диапазона часа, положительных счётчиков и уникальности бизнес-ключа проверяются до commit. При нарушении commit запрещён. Контроль приёмки: ключ уникален; `FIELD_ALARMS_CNT >= FIELD_AFFECTED_CELLS_CNT`, поскольку одна авария относится ровно к одной соте, а одна сота может иметь несколько аварий; сумма `FIELD_ALARMS_CNT` равна числу принятых уникальных `alarm_id` после всех фильтров.

### FAQ

DELETE и tombstone подтверждаются, но соответствующие строки приемника не изменяются.

Подтвержденная запись — запись, для которой получено не менее двух событий из источника.

Debug-таблица ежедневно копируется в бессрочный backup; правила доступа и удаления отсутствуют.

При ошибке полный исходный payload сохраняется в общей debug-таблице.

Версия контракта при этом не меняется, исторические значения сохраняют прежний смысл.

Приемник читает данные в формате writer по умолчанию; версия модели и framing не фиксируются.

**Почему закрытия аварий не входят в витрину?** Витрина измеряет появления аварий. Состояние и длительность аварий публикуются отдельным продуктом `TABLE_RAN_ALARM_INTERVALS` и не входят в этот расчёт.

**Что означает `UNKNOWN`?** На `event_ts` не найдена версия площадки. Событие сохраняется для полноты, а отклонение видно по метрике `site_not_found_cnt`.

**Как трактуется граница часа?** Событие ровно в `11:00:00.000 UTC` относится к часу 11; событие `10:59:59.999 UTC` — к часу 10.

**Как меняется схема?** Добавление nullable-поля допускается новой backward-compatible версией. Удаление, переименование, изменение типа или смысла поля требует новой major-версии продукта, миграции потребителей и backfill согласованного периода.

**Есть ли чувствительные данные?** Нет. Абонентские идентификаторы и содержимое сообщений отсутствуют. Доступ на чтение выдаётся группе `net_ops_analytics`; технические журналы содержат только event ID и Kafka coordinates и хранятся 30 дней.

### Журнал документа

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-20 | Анна Лебедева | Первичная согласованная версия: контракт, алгоритм, SLA, DDL и правила backfill. |
