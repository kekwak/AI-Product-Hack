# Потоковые данные/витрины

| **Общие сведения** | Почасовая витрина финальных исходов SMS. Одна строка описывает сообщения, получившие финальный статус в одном UTC-часе, для региона маршрута, направления и канала трафика. |
| :--- | :--- |
| **Служебное поле 02** | NOC и продукт Messaging должны одинаково считать доставку SMS и быстро находить деградации маршрутов. Включаются финальные статусы `DELIVERED`, `FAILED`, `EXPIRED`; промежуточные статусы, содержимое SMS и тестовый трафик исключаются. Результат также должен обеспечивать прогноз значений на следующие 30 дней. |
| **Продуктовые метрики** | Полнота принятых финальных сообщений — 100%; час опубликован не позднее H+31 минуты; уникальность `message_id` до агрегации — 100%; `FIELD_DELIVERY_RATE_PCT` воспроизводится из счётчиков без расхождения. Полнота результата должна быть достаточной для бизнеса. Свежесть данных должна оцениваться как приемлемая владельцем продукта. |
| **Заказчики** | Центр управления Messaging, команда качества цифровых коммуникаций. |
| **Нефункциональные требования** | До 300 млн сообщений/сутки, пик 25 тыс./с; event-time обработка UTC; первичный SLA H+5 минут, финальный H+31 минута; хранение результата 2 года; безопасный replay Kafka за 14 дней; RTO 60 минут. Retention авторитетного сырья составляет 7 суток. |
| **Системы-источники** | `SMS_STATUS_HUB` — шлюз унифицированных финальных статусов SMSC. Финальным источником истины является корпоративный master-feed. |
| **Data Catalog** | [Продукт SMS Delivery Hourly](https://datacatalog.corp.mts.ru/products/sms-delivery-hourly) |
| **Исходники проекта** | [GitLab: messaging/sms-delivery-hourly](https://gitlab.corp.mts.ru/bigdata/messaging/sms-delivery-hourly) |
| **Команда** | Кирилл Зайцев — аналитик; Лилия Фролова — разработчик; Артём Денисов — QA; Оксана Чернова — Product Owner. |
| **JIRA** | [MSGDATA-4421 — Контроль финальных статусов SMS](https://jira.corp.mts.ru/browse/MSGDATA-4421) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_SMS_FINAL_STATUS_V2` | Kafka; кластер не указан | Карточка каталога не указана | JSON Schema draft 2020-12; schema `mts.messaging.sms-final-status`, версия 2.3, Schema Registry subject `TOPIC_SMS_FINAL_STATUS_V2-value`, compatibility `BACKWARD`; Confluent JSON Schema framing с schema ID, UTF-8, десериализация registry-aware Flink format. Kafka key — UTF-8 `message_id`. Payload содержит `operation=UPSERT\|DELETE` и монотонный `status_revision`; DELETE сохраняет исходный `final_status_ts`. Неизвестные свойства разрешены и игнорируются, обязательные поля задаёт schema. |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка на справочник отсутствует | Используется для обогащения; поля и версия не перечислены |

### Приемники данных

Одна строка приемника является суточным агрегатом объекта.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_MSG.TABLE_SMS_DELIVERY_HOURLY` | HDFS-кластер указан в runtime; полный путь отсутствует | Карточка каталога не указана для результата | Формат файла выбирается writer по умолчанию |

### Схема потоков данных

`SMS_STATUS_HUB` → `kafka-messaging-prod-02.TOPIC_SMS_FINAL_STATUS_V2` → Flink `sms-delivery-hourly-v2` → валидация/дедупликация → temporal JOIN маршрута → часовая агрегация → Iceberg `TABLE_SMS_DELIVERY_HOURLY`.

Бизнес-ключ: `(FIELD_BIZ_DATE, FIELD_HOUR_UTC, FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`. Одна строка содержит все финальные исходы своей группы; один принятый `message_id` учитывается один раз.

### Алгоритм обработки потока

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Каждое прошедшее фильтр событие записывается отдельной строкой без агрегации.

#### Шаг 1. Фильтрация данных

Период M+1 начинается с timestamp >= end_M, включая ту же границу.

Период M включает записи с timestamp >= start_M и timestamp <= end_M.

Нераспознаваемое время заменяется началом эпохи и передается дальше.

Нераспознаваемое число заменяется на 0 и считается валидным.

Дополнительные исключения команда определяет после анализа первых запусков.

Система должна оставлять качественные записи по правилам реализации.

#### Шаг 2. Обогащение данных

При нескольких совпадениях сохраняются все комбинации без дополнительной проверки.

JOIN выполняется только по идентификатору без даты и версии справочника.

Онлайн выбирает версию справочника на event time.

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. Агрегация и расчёт показателей

Тот же основной показатель рассчитывается как среднее исходных значений; приоритет формул не задан.

При NULL status записывается значение UNKNOWN.

Перед записью значения заменяются данными master-feed; способ подключения и поля не описаны.

1. `FIELD_BIZ_DATE` и `FIELD_HOUR_UTC` вычислить из `final_status_ts` в UTC; часовой интервал полуоткрытый `[H, H+1)`.
2. Сгруппировать по бизнес-ключу.
3. Рассчитать `FIELD_MESSAGES_CNT = COUNT(*)`, `FIELD_DELIVERED_CNT = COUNT_IF(status='DELIVERED')`, `FIELD_FAILED_CNT = COUNT_IF(status='FAILED')`, `FIELD_EXPIRED_CNT = COUNT_IF(status='EXPIRED')`.
4. Инвариант: `FIELD_MESSAGES_CNT = FIELD_DELIVERED_CNT + FIELD_FAILED_CNT + FIELD_EXPIRED_CNT`.
5. `FIELD_DELIVERY_RATE_PCT = ROUND(100.0000 * FIELD_DELIVERED_CNT / FIELD_MESSAGES_CNT, 4)` с half-up. Деление на ноль невозможно: пустые группы не создаются.
6. `FIELD_PROC_TS` — UTC-время Iceberg commit; `FIELD_IS_FINAL=false` до закрытия watermark и `true` при финализирующей замене часа.

#### Шаг 4. Поздние данные, запись и replay

Watermark — максимальный `final_status_ts` минус 30 минут. Событие ровно на watermark принимается. До закрытия watermark каждый checkpoint записывает полный aggregate snapshot всех групп затронутой часовой партиции, а не только изменённые строки; в H+31 минут выполняется финализирующая атомарная замена и устанавливается `FIELD_IS_FINAL=true`.

Более позднее событие учитывается в `too_late_cnt` и ставит час в очередь replay. Replay доступен за Kafka retention 14 дней: читает полный сохранённый диапазон offsets часа, применяет ту же дедупликацию и атомарно заменяет час. Для периода старше retention авторитетного сырья в скоупе продукта нет: партиция не меняется, отклонение эскалируется владельцу источника. Если correction переносит `final_status_ts` в другой час, replay атомарно пересчитывает обе затронутые партиции; winning `DELETE` пересчитывает исходный час.

Iceberg commit атомарен: частичные файлы не видны читателям. При падении до commit Flink восстанавливает offsets и state из checkpoint; при падении после commit Iceberg commit ID не применяется повторно. Замена целой партиции и детерминированная агрегация делают retry идемпотентным. Исправление revision и tombstone требуют replay часа исходного финального статуса.

### Формирование ключа (kafka) / партиции (hdfs)

- Входной Kafka key: `message_id` UTF-8. Kafka-приёмник отсутствует.
- HDFS/Iceberg partitions: `days(FIELD_BIZ_DATE)` и identity `FIELD_HOUR_UTC`; полный путь `/warehouse/cdm/messaging/sms_delivery_hourly/`.
- Внутренняя сортировка: `(FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`; она не является дополнительной гранулярностью.

### Структура данных

Основной показатель рассчитывается как сумма исходных значений.

Допустимые значения status: ACTIVE и INACTIVE.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_BIZ_DATE | DATE | UTC-дата final status; обязательность поля `FIELD_BIZ_DATE` не определена | `TOPIC_SMS_FINAL_STATUS_V2` | `final_status_ts` | BIGINT | Epoch ms → date |
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

Первая строка считается штатным результатом и должна приниматься без преобразований.

| FIELD_BIZ_DATE | FIELD_HOUR_UTC | FIELD_REGION_CODE | FIELD_DIRECTION | FIELD_TRAFFIC_CHANNEL | FIELD_MESSAGES_CNT | FIELD_DELIVERED_CNT | FIELD_FAILED_CNT | FIELD_EXPIRED_CNT | FIELD_DELIVERY_RATE_PCT | FIELD_IS_FINAL | FIELD_PROC_TS |
| :--- | ---: | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- | :--- |
| VALUE_OUTSIDE_DOCUMENTED_DOMAIN | 12 | MOS | MT | A2P | 125000 | 121875 | 2500 | 625 | 97.5000 | true | 2026-08-28 13:31:04 |
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
    FIELD_BIZ_DATE DATE NOT NULL,
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

Любой backfill прошлого использует последний доступный snapshot справочника.

Автоматический replay гарантирован для любых периодов за последние 90 суток.

Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.

**Почему час определяется по финальному статусу, а не отправке?** Продукт мониторит работу контура завершения доставки. Аналитика по часу отправки находится в отдельной витрине и не смешивается с этим SLA.

**Что означает `EXPIRED`?** SMSC завершил попытки после TTL. Это финальный недоставленный исход, но он выделен отдельно от технической ошибки `FAILED`.

**Есть ли персональные данные?** Нет: source contract не содержит MSISDN и текст, а output агрегирован. Чтение разрешено ролям `messaging_noc_read` и `messaging_product_read`; технические event IDs хранятся в state только до 14 дней и не журналируются.

**Как изменяется схема?** Новое nullable-поле допускается minor-версией после проверки читателей. Изменение бизнес-ключа, границ статусов, формулы или типа требует новой major-таблицы и параллельной сверки минимум 14 дней.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-29 | Кирилл Зайцев | Первичная утверждённая версия с часовыми границами, replay и сверками. |
