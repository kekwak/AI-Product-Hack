# Потоковые данные/витрины

| **Общие сведения** | Часовая витрина открытых аварий базовых станций. Одна строка содержит число уникальных аварий и затронутых сот сектора сети за один час UTC в разрезе региона, площадки, вендора, серьёзности и кода аварии. |
| :--- | :--- |
| **Решаемая проблема** | Единый проверяемый источник для оперативного мониторинга массовых аварий RAN и расчёта нагрузки региональных дежурных смен. В расчёт входят только события открытия аварии штатных площадок мобильной сети; тестовые и отменённые события не входят. Пользователь должен видеть полную историю каждого изменения результата. |
| **Продуктовые метрики** | Доля валидных событий, доступных в витрине не позднее 10 минут после конца часа, не менее 99,5%; расхождение `FIELD_ALARMS_CNT` с принятыми уникальными событиями источника — 0; доля событий без найденной площадки — не более 0,1%. Метрики считаются за календарные сутки UTC. |
| **Заказчики** | Центр управления сетью MTS Big Data, дирекция эксплуатации RAN. |
| **Нефункциональные требования** | До 18 млн входных событий в сутки и 35 тыс. событий/с; расчёт по event time; первичная публикация часа — H+10 минут, финализация — H+2 часа 10 минут; Kafka retention источника — 35 дней, хранение витрины — 400 дней; доступность — 99,9% в месяц; часовая партиция должна допускать безопасный повторный расчёт. |
| **Системы-источники** | Платформа `RAN_FAULT_HUB`, публикующая унифицированные события открытия и закрытия аварий 2G/3G/4G/5G. При недоступности используется резервный Kafka-кластер, имя которого выбирает эксплуатация. |
| **Data Catalog** | [Карточка продукта RAN Alarm Hourly](https://datacatalog.corp.mts.ru/products/net-ran-alarm-hourly) |
| **Исходники проекта** | [GitLab: net/ran-alarm-hourly](https://gitlab.corp.mts.ru/bigdata/net/ran-alarm-hourly) |
| **Команда** | Анна Лебедева — аналитик; Сергей Котов — разработчик; Мария Волкова — QA; Илья Орлов — Product Owner. |
| **JIRA** | [NETDATA-6412 — Часовая витрина аварий RAN](https://jira.corp.mts.ru/browse/NETDATA-6412) |

### Входной контур

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_RAN_ALARM_V2`, полные имена топика одинаковы во всех регионах; регион содержится в payload | Kafka; конкретный кластер не зафиксирован | [Data Catalog: TOPIC_RAN_ALARM_V2](https://datacatalog.corp.mts.ru/kafka/kafka-net-prod-01/TOPIC_RAN_ALARM_V2) | Apache Avro; схема `com.mts.net.ran.AlarmEvent`, Schema Registry subject `TOPIC_RAN_ALARM_V2-value`, версия 7, backward-compatible; Confluent wire format с magic byte и schema ID, десериализация по schema ID из `schema-registry-net-prod-01`. Ключ — UTF-8 string `alarm_id`. |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DWH_REF.DICT_RAN_SITE_SCD`, версия данных на момент `event_ts` | [Data Catalog: DICT_RAN_SITE_SCD](https://datacatalog.corp.mts.ru/tables/DWH_REF/DICT_RAN_SITE_SCD) | Greenplum `gp-ref-prod-01`, реляционная модель 4.0; чтение Spark JDBC в repeatable-read snapshot, типы декодируются PostgreSQL driver 42.7. SCD2-ключ `(site_id, valid_from_utc)`; интервалы `[valid_from_utc, valid_to_utc)`, `valid_to_utc IS NULL` означает бесконечность. Содержит `region_code`, `vendor_name`, `is_test_site`. |

### Выходной контур

В приемнике сохраняется только последняя опубликованная версия без журнала изменений.

Одна строка приемника является суточным агрегатом объекта.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_NET.TABLE_RAN_ALARM_HOURLY` | HDFS, кластер `hadoop-net-prod-01`, полный путь `/warehouse/cdm/net/ran_alarm_hourly/` | [Data Catalog: TABLE_RAN_ALARM_HOURLY](https://datacatalog.corp.mts.ru/tables/CDM_NET/TABLE_RAN_ALARM_HOURLY) | Apache Iceberg v2, файлы Parquet с `SNAPPY`; схема `CDM_NET.TABLE_RAN_ALARM_HOURLY` версии 1.0 из Hive Metastore `hms-net-prod-01`; запись Spark DataFrame writer v2 с `overwritePartitions`, чтение Iceberg reader по snapshot metadata. |

### Схема потоков данных

`RAN_FAULT_HUB` → Kafka `kafka-net-prod-01.TOPIC_RAN_ALARM_V2` → Flink job `ran-alarm-normalizer` → дедупликация и SCD2-обогащение → Spark job `ran-alarm-hourly` → Iceberg `CDM_NET.TABLE_RAN_ALARM_HOURLY` в `/warehouse/cdm/net/ran_alarm_hourly/`.

Гранулярность результата: одна строка на `(FIELD_BIZ_DATE, FIELD_HOUR_UTC, FIELD_REGION_CODE, FIELD_SITE_ID, FIELD_VENDOR_NAME, FIELD_SEVERITY, FIELD_ALARM_CODE)`. Это полный бизнес-ключ строки.

### Алгоритм обработки потока

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Каждое прошедшее фильтр событие записывается отдельной строкой без агрегации.

Расчёт использует `event_ts` как время события и `ingest_ts` только для дедупликации и контроля задержки. Все timestamps источника имеют формат epoch milliseconds UTC. Интервал часа полуоткрытый: `[H, H + 1 hour)`.

#### Шаг 1. Фильтрация данных

Replay использует отдельные выгрузочные фильтры, которые будут согласованы позднее.

Онлайн применяет стандартные фильтры качества, перечень которых хранится в коде.

1. Десериализовать сообщение по schema ID. Сообщение с неизвестной или несовместимой схемой не попадает в витрину; job увеличивает метрику `schema_reject_cnt` и сохраняет Kafka partition и offset в техническом журнале без payload. Null-value Kafka tombstone запрещён append-only контрактом источника и обрабатывается так же, как schema reject.
2. Среди успешно декодированных записей с непустым `alarm_id` сначала дедуплицировать по `alarm_id`: сохранить запись с максимальным `ingest_ts`; при равенстве — с максимальной парой `(kafka_partition, kafka_offset)`. Исправленная версия источника замещает старую до бизнес-фильтра, поэтому невалидная новая версия не оставляет в результате устаревшую валидную запись.
3. Оставить winning-записи, для которых одновременно: `operation = 'OPEN'`, `site_id` и `cell_id` непустые, `alarm_code` соответствует `^[A-Z0-9_]{2,32}$`, `severity IN ('CRITICAL','MAJOR','MINOR','WARNING')`, `event_ts` находится в диапазоне `[2020-01-01T00:00:00Z, processing_ts + 5 minutes]`.
4. Записи с нарушением любого условия исключить и посчитать в `invalid_event_cnt` по первой причине в порядке условий выше. Пустой вход формирует ноль строк и успешный статус, если источник доступен; недоступность источника завершает запуск ошибкой. Повторная доставка не меняет результат.

#### Шаг 2. Обогащение данных

1. Выполнить `LEFT JOIN` события с `DWH_REF.DICT_RAN_SITE_SCD` по `event.site_id = ref.site_id` и условию `event_ts >= valid_from_utc AND (event_ts < valid_to_utc OR valid_to_utc IS NULL)`.
2. Ожидаемая кардинальность — `many-to-one`: одному событию соответствует ноль или одна версия площадки. Если найдено несколько версий, запуск соответствующего часа завершить ошибкой качества, ничего не публиковать.
3. Если соответствие отсутствует, установить `FIELD_REGION_CODE = 'UNKNOWN'`, `FIELD_VENDOR_NAME = 'UNKNOWN'`, `is_test_site = false` и увеличить `site_not_found_cnt`. Значения `UNKNOWN` являются допустимым явным fallback и не равны `NULL`.
4. После JOIN исключить строки с `is_test_site = true`. Версия справочника выбирается строго на `event_ts`, поэтому последующее изменение атрибутов площадки не меняет историю без явного backfill.

#### Шаг 3. Трансформация и агрегация

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

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_BIZ_DATE | DATE | Дата часа UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT | Epoch ms → UTC date |
| FIELD_HOUR_UTC | TINYINT | Час UTC, 0–23; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT | Epoch ms → UTC hour |
| FIELD_REGION_CODE | STRING | Код региона или `UNKNOWN`; `NOT NULL`, 2–16 символов | `DICT_RAN_SITE_SCD` | `region_code` | STRING | Fallback `UNKNOWN` при отсутствии JOIN |
| FIELD_SITE_ID | STRING | Идентификатор площадки; `NOT NULL`, 1–32 символа | `TOPIC_RAN_ALARM_V2` | `site_id` | STRING | Без преобразования |
| FIELD_VENDOR_NAME | STRING | Вендор или `UNKNOWN`; `NOT NULL`, до 64 символов | `DICT_RAN_SITE_SCD` | `vendor_name` | STRING | Fallback `UNKNOWN` |
| FIELD_SEVERITY | STRING | `CRITICAL`, `MAJOR`, `MINOR`, `WARNING`; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `severity` | STRING | Валидируется фильтром |
| FIELD_ALARM_CODE | STRING | Код аварии; `NOT NULL`, 2–32 символа | `TOPIC_RAN_ALARM_V2` | `alarm_code` | STRING | Валидируется regex |
| FIELD_ALARMS_CNT | BIGINT | Число уникальных `alarm_id`, > 0; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `alarm_id` | STRING | `COUNT(DISTINCT alarm_id)` |
| FIELD_AFFECTED_CELLS_CNT | BIGINT | Число уникальных `cell_id`, > 0; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `cell_id` | STRING | `COUNT(DISTINCT cell_id)` |
| FIELD_FIRST_EVENT_TS | TIMESTAMP | Минимальное event time группы, UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT | Epoch ms → timestamp UTC |
| FIELD_LAST_EVENT_TS | TIMESTAMP | Максимальное event time группы, UTC; `NOT NULL` | `TOPIC_RAN_ALARM_V2` | `event_ts` | BIGINT | Не раньше `FIELD_FIRST_EVENT_TS` |
| FIELD_PROC_TS | TIMESTAMP | Время успешного запуска, UTC; `NOT NULL` | Spark | `processing_ts` | TIMESTAMP | Одинаково для всех строк snapshot запуска |

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

```sql
CREATE TABLE IF NOT EXISTS CDM_NET.TABLE_RAN_ALARM_HOURLY (
    FIELD_BIZ_DATE DATE NOT NULL,
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

**Почему закрытия аварий не входят в витрину?** Витрина измеряет появления аварий. Состояние и длительность аварий публикуются отдельным продуктом `TABLE_RAN_ALARM_INTERVALS` и не входят в этот расчёт.

**Что означает `UNKNOWN`?** На `event_ts` не найдена версия площадки. Событие сохраняется для полноты, а отклонение видно по метрике `site_not_found_cnt`.

**Как трактуется граница часа?** Событие ровно в `11:00:00.000 UTC` относится к часу 11; событие `10:59:59.999 UTC` — к часу 10.

**Как меняется схема?** Добавление nullable-поля допускается новой backward-compatible версией. Удаление, переименование, изменение типа или смысла поля требует новой major-версии продукта, миграции потребителей и backfill согласованного периода.

**Есть ли чувствительные данные?** Нет. Абонентские идентификаторы и содержимое сообщений отсутствуют. Доступ на чтение выдаётся группе `net_ops_analytics`; технические журналы содержат только event ID и Kafka coordinates и хранятся 30 дней.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-20 | Анна Лебедева | Первичная согласованная версия: контракт, алгоритм, SLA, DDL и правила backfill. |
