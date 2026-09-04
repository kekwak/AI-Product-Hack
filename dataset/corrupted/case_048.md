# Потоковые данные/витрины

| **Общие сведения** | Почасовая витрина финальных исходов SMS. Одна строка описывает сообщения, получившие финальный статус в одном UTC-часе, для региона маршрута, направления и канала трафика. |
| :--- | :--- |
| **Решаемая проблема** | NOC и продукт Messaging должны одинаково считать доставку SMS и быстро находить деградации маршрутов. Включаются финальные статусы `DELIVERED`, `FAILED`, `EXPIRED`; промежуточные статусы, содержимое SMS и тестовый трафик исключаются. |
| **Продуктовые метрики** | Полнота принятых финальных сообщений — 100%; час опубликован не позднее H+31 минуты; уникальность `message_id` до агрегации — 100%; `FIELD_DELIVERY_RATE_PCT` воспроизводится из счётчиков без расхождения. |
| **Заказчики** | Центр управления Messaging, команда качества цифровых коммуникаций. |
| **Нефункциональные требования** | До 300 млн сообщений/сутки, пик 25 тыс./с; event-time обработка UTC; первичный SLA H+5 минут, финальный H+31 минута; хранение результата 2 года; безопасный replay Kafka за 14 дней; RTO 60 минут. |
| **Системы-источники** | `SMS_STATUS_HUB` — шлюз унифицированных финальных статусов SMSC. |
| **Data Catalog** | [Продукт SMS Delivery Hourly](https://datacatalog.corp.mts.ru/products/sms-delivery-hourly) |
| **Исходники проекта** | [GitLab: messaging/sms-delivery-hourly](https://gitlab.corp.mts.ru/bigdata/messaging/sms-delivery-hourly) |
| **Команда** | Кирилл Зайцев — аналитик; Лилия Фролова — разработчик; Артём Денисов — QA; Оксана Чернова — Product Owner. |
| **JIRA** | [MSGDATA-4421 — Контроль финальных статусов SMS](https://jira.corp.mts.ru/browse/MSGDATA-4421) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_SMS_FINAL_STATUS_V2` | Kafka; кластер не указан | [Data Catalog: TOPIC_SMS_FINAL_STATUS_V2](https://datacatalog.corp.mts.ru/kafka/kafka-messaging-prod-02/TOPIC_SMS_FINAL_STATUS_V2) | JSON Schema draft 2020-12; schema `mts.messaging.sms-final-status`, версия 2.3, Schema Registry subject `TOPIC_SMS_FINAL_STATUS_V2-value`, compatibility `BACKWARD`; Confluent JSON Schema framing с schema ID, UTF-8, десериализация registry-aware Flink format. Kafka key — UTF-8 `message_id`. Payload содержит `operation=UPSERT\|DELETE` и монотонный `status_revision`; DELETE сохраняет исходный `final_status_ts`. Неизвестные свойства разрешены и игнорируются, обязательные поля задаёт schema. |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DWH_REF.DICT_SMS_ROUTE_SCD` | [Data Catalog: DICT_SMS_ROUTE_SCD](https://datacatalog.corp.mts.ru/tables/DWH_REF/DICT_SMS_ROUTE_SCD) | PostgreSQL `ref-messaging-prod-01`, модель 2.0; Flink JDBC temporal lookup, версия snapshot закреплена checkpoint. Поля `route_id`, `region_code`, `traffic_channel`; SCD2-интервал UTC `[valid_from_ts, valid_to_ts)`, enum channel: `P2P`, `A2P`, `SERVICE`. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_MSG.TABLE_SMS_DELIVERY_HOURLY` | HDFS; путь не указан | [Data Catalog: TABLE_SMS_DELIVERY_HOURLY](https://datacatalog.corp.mts.ru/tables/CDM_MSG/TABLE_SMS_DELIVERY_HOURLY) | Iceberg v2, Parquet `SNAPPY`; Hive Metastore модель версии 1.1; Flink Iceberg sink сериализует row data по field ID и коммитит checkpoint snapshot, Iceberg reader десериализует по snapshot schema. |

### Схема потоков данных

Обязательный порядок: фильтрация → обогащение → агрегация.

`SMS_STATUS_HUB` → `kafka-messaging-prod-02.TOPIC_SMS_FINAL_STATUS_V2` → Flink `sms-delivery-hourly-v2` → валидация/дедупликация → temporal JOIN маршрута → часовая агрегация → Iceberg `TABLE_SMS_DELIVERY_HOURLY`.

Бизнес-ключ: `(FIELD_BIZ_DATE, FIELD_HOUR_UTC, FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`. Одна строка содержит все финальные исходы своей группы; один принятый `message_id` учитывается один раз.

### Алгоритм обработки потока

Событие, пришедшее после watermark, окончательно отбрасывается и не меняет опубликованные данные.

Реализация сначала обогащает все записи, затем применяет входные фильтры.

#### Шаг 1. Фильтрация данных

Event time — `final_status_ts`, epoch milliseconds UTC. Сначала принять декодированные change records с `operation IN ('UPSERT','DELETE')`, непустыми `message_id`, `final_status_ts`, `status_revision >= 1` и Kafka key, равным payload `message_id`. Дедуплицировать по `message_id`: выбрать максимальный `status_revision`, затем максимальные `(kafka_partition, kafka_offset)`. Winning `DELETE` удаляет сообщение из агрегата исходного часа. Одинаковый revision и порядок при различном payload блокируют commit checkpoint.

Для winning `UPSERT` применить все условия:

```sql
message_id RLIKE '^[A-Za-z0-9-]{20,80}$'
AND operation = 'UPSERT'
AND status IN ('DELIVERED', 'FAILED', 'EXPIRED')
AND direction IN ('MO', 'MT')
AND route_id IS NOT NULL
AND is_test = false
AND submit_ts <= final_status_ts
AND final_status_ts >= TIMESTAMP '2025-01-01 00:00:00 UTC'
AND final_status_ts <= processing_ts + INTERVAL 2 MINUTES
```

NULL не проходит соответствующее условие. Ошибки schema/JSON, enum, времени и обязательных полей исключаются и учитываются раздельно; payload, адреса и текст не журналируются. Поля MSISDN и message body отсутствуют в контракте источника. Пустой поток штатен; недоступность Kafka или Schema Registry приостанавливает job без commit offsets.

#### Шаг 2. Обогащение данных

Temporal `LEFT JOIN` по `event.route_id = ref.route_id` и `final_status_ts` в `[valid_from_ts, valid_to_ts)`. Ожидаемая кардинальность many-to-zero-or-one. При нескольких совпадениях checkpoint не коммитится и поднимается ошибка справочника.

При отсутствии маршрута установить `FIELD_REGION_CODE='UNKNOWN'` и `FIELD_TRAFFIC_CHANNEL='UNKNOWN'`, сохранить сообщение и увеличить `route_not_found_cnt`. Если строка справочника найдена, но `region_code` пуст или `traffic_channel` не входит в enum, это дефект справочника: публикация останавливается. Другие справочники и НСИ не используются.

#### Шаг 3. Агрегация и расчёт показателей

1. `FIELD_BIZ_DATE` и `FIELD_HOUR_UTC` вычислить из `final_status_ts` в UTC; часовой интервал полуоткрытый `[H, H+1)`.
2. Сгруппировать по бизнес-ключу.
3. Рассчитать `FIELD_MESSAGES_CNT = COUNT(*)`, `FIELD_DELIVERED_CNT = COUNT_IF(status='DELIVERED')`, `FIELD_FAILED_CNT = COUNT_IF(status='FAILED')`, `FIELD_EXPIRED_CNT = COUNT_IF(status='EXPIRED')`.
4. Инвариант: `FIELD_MESSAGES_CNT = FIELD_DELIVERED_CNT + FIELD_FAILED_CNT + FIELD_EXPIRED_CNT`.
5. `FIELD_DELIVERY_RATE_PCT = ROUND(100.0000 * FIELD_DELIVERED_CNT / FIELD_MESSAGES_CNT, 4)` с half-up. Деление на ноль невозможно: пустые группы не создаются.
6. `FIELD_PROC_TS` — UTC-время Iceberg commit; `FIELD_IS_FINAL=false` до закрытия watermark и `true` при финализирующей замене часа.

#### Шаг 4. Поздние данные, запись и replay

Watermark — максимальный `final_status_ts` минус 30 минут; idle Kafka partitions исключаются из его минимума после 5 минут без сообщений. Событие ровно на watermark принимается; после его прохождения действует allowed lateness 1 минута, и час не финализируется раньше H+31. До финализации каждый checkpoint записывает полный aggregate snapshot всех групп затронутой часовой партиции, а не только изменённые строки. Независимый processing-time timer в H+31 минут закрывает час даже при пустом или остановившемся потоке: выполняется атомарная замена и устанавливается `FIELD_IS_FINAL=true`.

Более позднее событие учитывается в `too_late_cnt` и ставит час в очередь replay. Replay доступен за Kafka retention 14 дней: читает полный сохранённый диапазон offsets часа, применяет ту же дедупликацию и атомарно заменяет час. Для периода старше retention авторитетного сырья в скоупе продукта нет: партиция не меняется, отклонение эскалируется владельцу источника. Если correction переносит `final_status_ts` в другой час, replay атомарно пересчитывает обе затронутые партиции; winning `DELETE` пересчитывает исходный час.

Iceberg commit атомарен: частичные файлы не видны читателям. При падении до commit Flink восстанавливает offsets и state из checkpoint; при падении после commit Iceberg commit ID не применяется повторно. Замена целой партиции и детерминированная агрегация делают retry идемпотентным. Исправление revision и tombstone требуют replay часа исходного финального статуса.

### Формирование ключа (kafka) / партиции (hdfs)

- Входной Kafka key: `message_id` UTF-8. Kafka-приёмник отсутствует.
- HDFS/Iceberg partitions: `days(FIELD_BIZ_DATE)` и identity `FIELD_HOUR_UTC`; полный путь `/warehouse/cdm/messaging/sms_delivery_hourly/`.
- Внутренняя сортировка: `(FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`; она не является дополнительной гранулярностью.

### Структура данных

В следующем релизе добавляется обязательное поле contract_flag NOT NULL без default.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_BIZ_DATE | DATE | UTC-дата final status; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `final_status_ts` | BIGINT | Epoch ms → date |
| FIELD_HOUR_UTC | TINYINT | UTC-час 0–23; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `final_status_ts` | BIGINT | Epoch ms → hour |
| FIELD_REGION_CODE | STRING | Регион либо `UNKNOWN`; `NOT NULL` | `DICT_SMS_ROUTE_SCD` | `region_code` | STRING | Temporal JOIN/fallback |
| FIELD_DIRECTION | STRING | `MO` или `MT`; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `direction` | STRING | Валидированный enum |
| FIELD_TRAFFIC_CHANNEL | STRING | `P2P`, `A2P`, `SERVICE` или `UNKNOWN`; `NOT NULL` | `DICT_SMS_ROUTE_SCD` | `traffic_channel` | STRING | Temporal JOIN/fallback |
| FIELD_MESSAGES_CNT | BIGINT | Все финальные сообщения группы, >0; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `message_id` | STRING | `COUNT(*)` после дедупликации |
| FIELD_DELIVERED_CNT | BIGINT | Доставленные, >=0; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `status` | STRING | `COUNT_IF(DELIVERED)` |
| FIELD_FAILED_CNT | BIGINT | Ошибка доставки, >=0; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `status` | STRING | `COUNT_IF(FAILED)` |
| FIELD_EXPIRED_CNT | BIGINT | Истёк TTL, >=0; `NOT NULL` | `TOPIC_SMS_FINAL_STATUS_V2` | `status` | STRING | `COUNT_IF(EXPIRED)` |
| FIELD_DELIVERY_RATE_PCT | DECIMAL(7,4) | Доля доставленных в процентах 0–100; `NOT NULL` | Расчёт | счётчики | Не применимо: расчёт | Half-up до 4 знаков |
| FIELD_IS_FINAL | BOOLEAN | Финализирован ли час; `NOT NULL` | Flink | watermark | Не применимо: системное состояние | `true` после H+31 минуты |
| FIELD_PROC_TS | TIMESTAMP | UTC-время snapshot commit; `NOT NULL` | Flink | `commit_ts` | TIMESTAMP | Одинаково для snapshot |

### Пример данных

| FIELD_BIZ_DATE | FIELD_HOUR_UTC | FIELD_REGION_CODE | FIELD_DIRECTION | FIELD_TRAFFIC_CHANNEL | FIELD_MESSAGES_CNT | FIELD_DELIVERED_CNT | FIELD_FAILED_CNT | FIELD_EXPIRED_CNT | FIELD_DELIVERY_RATE_PCT | FIELD_IS_FINAL | FIELD_PROC_TS |
| :--- | ---: | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- | :--- |
| 2026-08-28 | 12 | MOS | MT | A2P | 125000 | 121875 | 2500 | 625 | 97.5000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 12 | MOS | MO | P2P | 68000 | 66640 | 1020 | 340 | 98.0000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 12 | SPE | MT | SERVICE | 42000 | 41580 | 336 | 84 | 99.0000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 12 | KZN | MT | A2P | 50000 | 48000 | 1500 | 500 | 96.0000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 12 | EKB | MO | P2P | 31000 | 30690 | 217 | 93 | 99.0000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 12 | UNKNOWN | MT | UNKNOWN | 100 | 90 | 8 | 2 | 90.0000 | true | 2026-08-28 13:31:04 |
| 2026-08-28 | 13 | MOS | MT | A2P | 130000 | 126100 | 3120 | 780 | 97.0000 | true | 2026-08-28 14:31:02 |
| 2026-08-28 | 13 | NSK | MT | SERVICE | 27500 | 26950 | 440 | 110 | 98.0000 | true | 2026-08-28 14:31:02 |
| 2026-08-28 | 13 | SAM | MO | P2P | 22000 | 21560 | 330 | 110 | 98.0000 | true | 2026-08-28 14:31:02 |
| 2026-08-28 | 13 | UFA | MT | A2P | 18000 | 17280 | 540 | 180 | 96.0000 | true | 2026-08-28 14:31:02 |

### DDL

```sql
CREATE TABLE IF NOT EXISTS CDM_MSG.TABLE_SMS_DELIVERY_HOURLY (
    FIELD_BIZ_DATE STRING NOT NULL,
    FIELD_HOUR_UTC TINYINT NOT NULL,
    FIELD_REGION_CODE STRING NOT NULL,
    FIELD_DIRECTION STRING NOT NULL,
    FIELD_TRAFFIC_CHANNEL STRING NOT NULL,
    FIELD_MESSAGES_CNT BIGINT NOT NULL,
    FIELD_DELIVERED_CNT BIGINT NOT NULL,
    FIELD_FAILED_CNT BIGINT NOT NULL,
    FIELD_EXPIRED_CNT BIGINT NOT NULL,
    FIELD_DELIVERY_RATE_PCT DECIMAL(7,4) NOT NULL,
    FIELD_IS_FINAL BOOLEAN NOT NULL,
    FIELD_PROC_TS TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(FIELD_BIZ_DATE), FIELD_HOUR_UTC)
LOCATION '/warehouse/cdm/messaging/sms_delivery_hourly/'
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='snappy'
);
```

Приёмка до commit: уникальность бизнес-ключа, отсутствие NULL, допустимые enum/час, неотрицательные компоненты, положительный total, точное равенство суммы исходов total и повторный расчёт rate по формуле. Контрольная сумма принятых уникальных `message_id` равна сумме `FIELD_MESSAGES_CNT`.

### FAQ

Порог срабатывания, период сравнения и блокировка публикации определяются вручную после проверки.

После загрузки рассчитываются число дублей и расхождение с источником.

Любое позднее событие автоматически включается ближайшим replay без ограничения по возрасту.

При сбое после частичной записи партиции повторный запуск не проверяет, какие строки уже опубликованы, и может продублировать или пропустить часть данных.

Старые партиции и потребители остаются без изменений; план миграции не предусмотрен.

**Почему час определяется по финальному статусу, а не отправке?** Продукт мониторит работу контура завершения доставки. Аналитика по часу отправки находится в отдельной витрине и не смешивается с этим SLA.

**Что означает `EXPIRED`?** SMSC завершил попытки после TTL. Это финальный недоставленный исход, но он выделен отдельно от технической ошибки `FAILED`.

**Есть ли персональные данные?** Нет: source contract не содержит MSISDN и текст, а output агрегирован. Чтение разрешено ролям `messaging_noc_read` и `messaging_product_read`; технические event IDs хранятся в state только до 14 дней и не журналируются.

**Как изменяется схема?** Новое nullable-поле допускается minor-версией после проверки читателей. Изменение бизнес-ключа, границ статусов, формулы или типа требует новой major-таблицы и параллельной сверки минимум 14 дней.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-29 | Кирилл Зайцев | Первичная утверждённая версия с часовыми границами, replay и сверками. |
