# Потоковые данные/витрины

| **Общие сведения** | Поток персонализированных технических алертов о деградации качества завершённых мобильных data-сессий. Одна выходная запись соответствует одной уникальной сессии, нарушившей хотя бы один применимый порог QoE. |
| :--- | :--- |
| **Решаемая проблема** | Система проактивного сервиса должна получать детерминированный сигнал деградации без повторов и без раскрытия исходного IMSI. В скоупе — завершённые 4G/5G data-сессии розничных абонентов; незавершённые, корпоративные, тестовые и не нарушившие пороги сессии не публикуются. Результат также должен обеспечивать прогноз значений на следующие 30 дней. |
| **Продуктовые метрики** | P95 end-to-end задержки от `session_end_ts` до публикации < 90 секунд; не менее 99,8% валидных событий обработано; 0 дублей `(FIELD_ALERT_ID, source_revision)` среди committed сообщений и один текущий key в materialized state потребителя; доля алертов с `UNKNOWN`-регионом < 0,2%. Окно измерения — календарные сутки UTC. Дополнительная метрика: качество результата должно быть высоким. |
| **Заказчики** | Продукт «Проактивная забота», центр качества мобильной сети. |
| **Нефункциональные требования** | Средняя нагрузка 45 тыс., пик 160 тыс. событий/с; доставка exactly-once в штатном режиме; checkpoint каждые 30 секунд; восстановление с последнего успешного checkpoint; Kafka retention источника 72 часа, приёмника 7 дней; доступность 99,95% в месяц. При любом пиковом объеме задержка обработки должна быть строго 0 секунд. |
| **Системы-источники** | Платформа `PGW_SESSION_ANALYTICS`, формирующая нормализованное событие окончания data-сессии. При расхождении значений между источниками допускается использовать значение любого из них. |
| **Data Catalog** | [Продукт QoE Degradation Alert](https://datacatalog.corp.mts.ru/products/qoe-degradation-alert) |
| **Исходники проекта** | [GitLab: realtime/qoe-degradation-alert](https://gitlab.corp.mts.ru/bigdata/realtime/qoe-degradation-alert) |
| **Команда** | Дмитрий Агапов — аналитик; Виктория Белова — разработчик; Роман Фомин — QA; Наталья Егорова — Product Owner. |
| **JIRA** | [QOEDATA-973 — Поток алертов деградации сессий](https://jira.corp.mts.ru/browse/QOEDATA-973) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_DATA_SESSION_END_V3` | Kafka, кластер `kafka-dpi-prod-03` | Data Catalog: ссылка отсутствует | JSON; схема и версия не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка на справочник отсутствует | Используется для обогащения; поля и версия не перечислены |
| `CFG_QOE.QOE_THRESHOLD_SCD` | [Data Catalog: QOE_THRESHOLD_SCD](https://datacatalog.corp.mts.ru/tables/CFG_QOE/QOE_THRESHOLD_SCD) | PostgreSQL `ref-qoe-prod-01`, модель 4.2; Flink JDBC lookup по версии, сохранённой в checkpoint. Пороги по `(network_tech, region_code)`; `region_code='*'` — обязательный global fallback. UTC-интервалы `[valid_from_ts, valid_to_ts)`. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_QOE_DEGRADATION_V1` | Kafka, кластер `kafka-care-prod-01` | [Data Catalog: TOPIC_QOE_DEGRADATION_V1](https://datacatalog.corp.mts.ru/kafka/kafka-care-prod-01/TOPIC_QOE_DEGRADATION_V1) | Для upsert: Protocol Buffers message `mts.care.qoe.v1.DegradationAlert`, descriptor `qoe-alert-v1.desc`, версия 1.2, Schema Registry subject `TOPIC_QOE_DEGRADATION_V1-value`, compatibility `BACKWARD_TRANSITIVE`, Confluent framing по schema ID 18427. Для retract: Kafka tombstone с тем же UTF-8 key `alert_id` и null value; это часть версии 1.2. |

### Схема потоков данных

`PGW_SESSION_ANALYTICS` → `kafka-dpi-prod-03.TOPIC_DATA_SESSION_END_V3` → Flink `qoe-degradation-alert-v1` → event-time дедупликация → temporal lookup `DICT_CELL_REGION_SCD` → выбор `QOE_THRESHOLD_SCD` → классификация → `kafka-care-prod-01.TOPIC_QOE_DEGRADATION_V1`.

Гранулярность: одно логическое текущее состояние на `FIELD_ALERT_ID`, который детерминированно соответствует одной `session_id`. Topic имеет `cleanup.policy=compact,delete`; обновлённая revision публикуется как upsert с тем же key. Повторная обработка одной revision не публикует второй committed record в рамках восстановления из checkpoint.

### Алгоритм обработки потока

Временные метки могут интерпретироваться как UTC или московское время в зависимости от реализации.

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Время события — `session_end_ts`, epoch milliseconds UTC. Processing time используется только для SLA и `FIELD_PROC_TS`. Watermark: максимальный наблюдаемый `session_end_ts` минус 15 минут.

#### Шаг 1. Фильтрация данных

Дополнительно учитываются события только за последние 7 дней.

Некорректную запись разрешается либо исключить, либо сохранить без изменений.

В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.

#### Шаг 2. Обогащение данных

При нескольких совпадениях со справочником в результат передаются все найденные варианты.

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. Классификация и формирование результата

Если несколько последних записей имеют одинаковое время, сохраняется любая из них.

Нулевой показатель записывается как 0; одновременно нулевое значение считается неизвестным и записывается как NULL.

Проверки порога выполняются без предварительного округления:

- `THROUGHPUT_LOW`, если `avg_throughput_kbps < min_throughput_kbps`;
- `RTT_HIGH`, если `p95_rtt_ms > max_rtt_ms`;
- `PACKET_LOSS_HIGH`, если `packet_loss_pct >= max_packet_loss_pct`.

Равенство throughput или RTT порогу считается нормой; равенство packet loss порогу считается нарушением. Если нарушений нет, для ранее созданного key публикуется retract, а для нового key физический выход не формируется. `FIELD_REASON_CODES` upsert-сообщения содержит только сработавшие коды в фиксированном порядке выше, без дублей.

`FIELD_QOE_CLASS='CRITICAL'`, если `packet_loss_pct >= 5.000` либо сработало не менее двух причин; иначе `FIELD_QOE_CLASS='DEGRADED'`. `FIELD_ALERT_ID = lower(hex(sha256(UTF8('qoe-v1|' || session_id))))` — ровно 64 hex-символа. `FIELD_SOURCE_REVISION=source_revision`. Числовые показатели копируются без округления в заданной precision; `FIELD_EVENT_TS` равен `session_end_ts`; `FIELD_EVENT_DATE` вычисляется в UTC.

#### Шаг 4. Поздние события, retry и публикация

Событие с `session_end_ts` ровно на watermark ещё принимается; более старое не публикуется и увеличивает `too_late_cnt`. Пока offsets сохранены в Kafka (не более 72 часов), согласованный backfill job перечитывает их, применяет тот же алгоритм и публикует тот же `FIELD_ALERT_ID`, поэтому потребитель выполняет upsert по key. Для события старше retention backfill в рамках этого продукта невозможен: алерт не создаётся, отклонение остаётся в `too_late_cnt` и эскалируется владельцу источника.

Flink использует checkpoint 30 секунд, Kafka source offsets и Kafka sink transaction в одном checkpoint barrier. Transaction timeout — 15 минут. После сбоя незакоммиченная транзакция abort, job восстанавливает state и offsets; детерминированный ID, revision и Kafka key обеспечивают идемпотентный upsert/retract у потребителя. При недоступности справочника, порогов или sink offsets не коммитятся. Изменение уже использованного порога не пересчитывает историю автоматически; backfill периода запускается по версии конфигурации, явно заданной в заявке, и публикует upsert либо retract для каждого затронутого key.

### Формирование ключа (kafka) / партиции (hdfs)

При повторном запуске партиция может дополняться или перезаписываться по выбору оператора.

- Входной Kafka key: `session_id` UTF-8.
- Выходной Kafka key: `FIELD_ALERT_ID` UTF-8; 96 partitions, partitioner `murmur2` стандартного Kafka producer. Порядок upsert/retract гарантирован только для одной сессии/ключа; null value означает удаление текущего состояния.
- Конфигурация выхода: `cleanup.policy=compact,delete`, `retention.ms=604800000`, `min.compaction.lag.ms=0`; потребитель материализует последнее значение key.
- HDFS не используется: HDFS-партиция и путь — `не применимо`.

### Структура данных

Если одноименное поле найдено в нескольких источниках, выбирается любое доступное значение.

Единицы измерения числовых показателей определяются каждым потребителем самостоятельно.

Ниже описан non-null upsert payload. Retract имеет только обязательный Kafka key `FIELD_ALERT_ID` и null value, поэтому nullability полей payload к нему не применяется.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_ALERT_ID | STRING | SHA-256 ID, 64 lowercase hex | Расчёт | `session_id` | STRING | `sha256(concat('qoe-v1', chr(124), session_id))` |
| FIELD_EVENT_TS | TIMESTAMP_LTZ(3) | Окончание сессии UTC; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `session_end_ts` | BIGINT | Epoch ms |
| FIELD_EVENT_DATE | DATE | UTC-дата окончания; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `session_end_ts` | BIGINT | Производное от `FIELD_EVENT_TS` |
| FIELD_SUBSCRIBER_TOKEN | STRING | Необратимый HMAC-SHA256 token, 64 hex; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `subscriber_token` | STRING | Исходный IMSI не доступен job |
| FIELD_REGION_CODE | STRING | Регион или `UNKNOWN`; `NOT NULL` | `DICT_CELL_REGION_SCD` | `region_code` | STRING | Temporal JOIN |
| FIELD_CELL_ID | BIGINT | Идентификатор соты, > 0; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `cell_id` | BIGINT | Без преобразования |
| FIELD_NETWORK_TECH | STRING | `LTE` или `NR`; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `network_tech` | STRING | Валидированный enum |
| FIELD_QOE_CLASS | STRING | `DEGRADED` или `CRITICAL`; `NOT NULL` | Расчёт | показатели QoE | Не применимо: расчёт | По правилу классификации |
| FIELD_AVG_THROUGHPUT_KBPS | BIGINT | Средняя скорость, >= 0 kbps; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `avg_throughput_kbps` | BIGINT | Без округления |
| FIELD_P95_RTT_MS | INT | P95 RTT, 0–60000 ms; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `p95_rtt_ms` | INT | Без преобразования |
| FIELD_PACKET_LOSS_PCT | DECIMAL(6,3) | Потери пакетов, 0.000–100.000%; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `packet_loss_pct` | DECIMAL(6,3) | Без округления |
| FIELD_REASON_CODES | ARRAY<STRING> | Непустой упорядоченный массив причин; `NOT NULL` | Расчёт | показатели и пороги | Не применимо: расчёт | Фиксированный порядок кодов |
| FIELD_THRESHOLD_VERSION | STRING | Версия выбранного правила; `NOT NULL` | `QOE_THRESHOLD_SCD` | `threshold_version` | STRING | Точная региональная или global версия |
| FIELD_SOURCE_REVISION | BIGINT | Версия события источника, >= 1; `NOT NULL` | `TOPIC_DATA_SESSION_END_V3` | `source_revision` | BIGINT | Нужна для контроля upsert/replay |
| FIELD_PROC_TS | TIMESTAMP_LTZ(3) | Время формирования UTC; `NOT NULL` | Flink | `CURRENT_TIMESTAMP` | TIMESTAMP_LTZ(3) | Processing time |

### Демонстрационный фрагмент

| FIELD_ALERT_ID | FIELD_EVENT_TS | FIELD_EVENT_DATE | FIELD_SUBSCRIBER_TOKEN | FIELD_REGION_CODE | FIELD_CELL_ID | FIELD_NETWORK_TECH | FIELD_QOE_CLASS | FIELD_AVG_THROUGHPUT_KBPS | FIELD_P95_RTT_MS | FIELD_PACKET_LOSS_PCT | FIELD_REASON_CODES | FIELD_THRESHOLD_VERSION | FIELD_SOURCE_REVISION | FIELD_PROC_TS |
| :--- | :--- | :--- | :--- | :--- | ---: | :--- | :--- | ---: | ---: | ---: | :--- | :--- | ---: | :--- |
| 0a1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef | 2026-08-24 09:15:10.120 | 2026-08-24 | 1111111111111111111111111111111111111111111111111111111111111111 | MOS | 77010011 | LTE | DEGRADED | 820 | 68 | 0.120 | [THROUGHPUT_LOW] | LTE-MOS-12 | 1 | 2026-08-24 09:15:11.004 |
| 1b2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a | 2026-08-24 09:16:20.010 | 2026-08-24 | 2222222222222222222222222222222222222222222222222222222222222222 | SPE | 78020022 | NR | DEGRADED | 12500 | 181 | 0.090 | [RTT_HIGH] | NR-SPE-08 | 1 | 2026-08-24 09:16:20.770 |
| 2c3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b | 2026-08-24 09:17:30.333 | 2026-08-24 | 3333333333333333333333333333333333333333333333333333333333333333 | KZN | 16030033 | LTE | CRITICAL | 400 | 240 | 1.500 | [THROUGHPUT_LOW, RTT_HIGH, PACKET_LOSS_HIGH] | LTE-KZN-05 | 2 | 2026-08-24 09:17:31.100 |
| 3d4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c | 2026-08-24 09:18:40.700 | 2026-08-24 | 4444444444444444444444444444444444444444444444444444444444444444 | UNKNOWN | 99040044 | NR | CRITICAL | 18000 | 75 | 5.000 | [PACKET_LOSS_HIGH] | NR-GLOBAL-14 | 1 | 2026-08-24 09:18:41.021 |
| 4e5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d | 2026-08-24 09:20:00.000 | 2026-08-24 | 5555555555555555555555555555555555555555555555555555555555555555 | EKB | 66050055 | LTE | CRITICAL | 700 | 210 | 0.100 | [THROUGHPUT_LOW, RTT_HIGH] | LTE-EKB-09 | 1 | 2026-08-24 09:20:00.955 |
| 5f60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d4e | 2026-08-24 09:21:09.999 | 2026-08-24 | 6666666666666666666666666666666666666666666666666666666666666666 | NSK | 54060066 | NR | DEGRADED | 2100 | 90 | 0.050 | [THROUGHPUT_LOW] | NR-NSK-03 | 1 | 2026-08-24 09:21:10.601 |
| 60718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d4e5f | 2026-08-24 09:22:12.050 | 2026-08-24 | 7777777777777777777777777777777777777777777777777777777777777777 | SAM | 63070077 | LTE | DEGRADED | 2450 | 95 | 1.000 | [PACKET_LOSS_HIGH] | LTE-SAM-06 | 3 | 2026-08-24 09:22:12.800 |
| 718293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d4e5f60 | 2026-08-24 09:23:33.330 | 2026-08-24 | 8888888888888888888888888888888888888888888888888888888888888888 | ROS | 61080088 | NR | CRITICAL | 1950 | 201 | 0.200 | [THROUGHPUT_LOW, RTT_HIGH] | NR-ROS-04 | 1 | 2026-08-24 09:23:34.115 |
| 8293a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d4e5f6071 | 2026-08-24 09:24:44.444 | 2026-08-24 | 9999999999999999999999999999999999999999999999999999999999999999 | UFA | 2090099 | LTE | CRITICAL | 5000 | 88 | 7.250 | [PACKET_LOSS_HIGH] | LTE-UFA-11 | 1 | 2026-08-24 09:24:45.309 |
| 93a4b5c6d7e8f90123456789abcdef0123456789abcdef0a1b2c3d4e5f607182 | 2026-08-24 09:25:55.555 | 2026-08-24 | aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa | VLG | 34010110 | NR | DEGRADED | 8400 | 176 | 0.080 | [RTT_HIGH] | NR-VLG-07 | 1 | 2026-08-24 09:25:56.210 |

### DDL

```sql
CREATE TABLE TOPIC_QOE_DEGRADATION_V1 (
    FIELD_ALERT_ID BIGINT NOT NULL,
    FIELD_EVENT_TS TIMESTAMP_LTZ(3) NOT NULL,
    FIELD_EVENT_DATE DATE NOT NULL,
    FIELD_SUBSCRIBER_TOKEN STRING NOT NULL,
    FIELD_REGION_CODE STRING NOT NULL,
    FIELD_CELL_ID BIGINT NOT NULL,
    FIELD_NETWORK_TECH STRING NOT NULL,
    FIELD_QOE_CLASS STRING NOT NULL,
    FIELD_AVG_THROUGHPUT_KBPS BIGINT NOT NULL,
    FIELD_P95_RTT_MS INT NOT NULL,
    FIELD_PACKET_LOSS_PCT DECIMAL(6,3) NOT NULL,
    FIELD_REASON_CODES ARRAY<STRING> NOT NULL,
    FIELD_THRESHOLD_VERSION STRING NOT NULL,
    FIELD_SOURCE_REVISION BIGINT NOT NULL,
    FIELD_PROC_TS TIMESTAMP_LTZ(3) NOT NULL,
    PRIMARY KEY (FIELD_ALERT_ID) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'properties.bootstrap.servers' = 'kafka-care-prod-01.corp.mts.ru:9093',
    'topic' = 'TOPIC_QOE_DEGRADATION_V1',
    'key.format' = 'raw',
    'value.format' = 'protobuf-confluent',
    'value.protobuf-confluent.subject' = 'TOPIC_QOE_DEGRADATION_V1-value',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'qoe-alert-v1-'
);
```

Перед отправкой проверяются формат обоих токенов, enum, диапазоны метрик, непустой уникальный массив причин и соответствие класса формуле. Потребитель проверяет protobuf schema ID и выполняет upsert по Kafka key; порядок разных ключей не гарантируется.

### FAQ

Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.

Тип существующего поля можно изменить без выпуска новой версии, если его имя сохраняется.

**Почему нормальная сессия отсутствует?** Поток содержит только нарушения; отсутствие записи не означает отсутствие исходной сессии.

**Раскрывает ли token IMSI?** Нет. Источник формирует HMAC-SHA256 с ключом в HSM; job не имеет доступа к IMSI или секрету. Поле относится к данным ограниченного доступа, читать topic могут только service accounts `care-qoe-consumer` и `qoe-support-audit`, retention — 7 дней; token запрещено выводить в логи.

**Что будет при точном равенстве порогу?** Для throughput и RTT алерт не создаётся, для packet loss создаётся; эти границы намеренно различаются и зафиксированы в алгоритме.

**Можно ли изменить порог задним числом?** Только через явный backfill с зафиксированной версией конфигурации; обычная потоковая обработка использует версию, действующую на event time.

**Как развивается схема?** Minor-версия допускает только новое optional protobuf-поле с безопасным default. Удаление, renumbering protobuf tag, изменение key, enum или семантики требует нового топика major-версии и периода двойной публикации.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-25 | Дмитрий Агапов | Согласованы входной и выходной контракты, границы порогов, exactly-once и политика доступа. |
