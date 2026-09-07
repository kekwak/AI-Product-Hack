# Потоковые данные/витрины

| **Общие сведения** | Почасовая витрина финальных исходов SMS. Одна строка описывает сообщения, получившие финальный статус в одном UTC-часе, для региона маршрута, направления и канала трафика. |
| :--- | :--- |
| **Решаемая проблема** | Требуется сформировать новый технический набор данных; решаемая бизнес-проблема и ожидаемое изменение процесса пока не определены. |
| **Продуктовые метрики** | Полнота принятых финальных сообщений — 100%; час опубликован не позднее H+31 минуты; уникальность `message_id` до агрегации — 100%; `FIELD_DELIVERY_RATE_PCT` воспроизводится из счётчиков без расхождения. Дополнительно: полнота результата — доля непустых обязательных полей. Финальная приемка результата выполняется экспертно. |
| **Заказчики** | Центр управления Messaging, команда качества цифровых коммуникаций. |
| **Нефункциональные требования** | До 300 млн сообщений/сутки, пик 25 тыс./с; event-time обработка UTC; первичный SLA H+5 минут, финальный H+31 минута; хранение результата 2 года; безопасный replay Kafka за 14 дней; RTO 60 минут. При любом пиковом объеме задержка обработки должна быть строго 0 секунд. |
| **Служебное поле 06** | `SMS_STATUS_HUB` — шлюз унифицированных финальных статусов SMSC. Финальным источником истины является корпоративный master-feed. |
| **Data Catalog** | [Продукт SMS Delivery Hourly](https://datacatalog.corp.mts.ru/products/sms-delivery-hourly) |
| **Исходники проекта** | [GitLab: messaging/sms-delivery-hourly](https://gitlab.corp.mts.ru/bigdata/messaging/sms-delivery-hourly) |
| **Команда** | Кирилл Зайцев — аналитик; Лилия Фролова — разработчик; Артём Денисов — QA; Оксана Чернова — Product Owner. |
| **JIRA** | [MSGDATA-4421 — Контроль финальных статусов SMS](https://jira.corp.mts.ru/browse/MSGDATA-4421) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `TOPIC_SMS_FINAL_STATUS_V2` | Kafka; кластер не указан | Data Catalog: ссылка отсутствует | JSON; схема и версия не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка на справочник отсутствует | Используется для обогащения; поля и версия не перечислены |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_MSG.TABLE_SMS_DELIVERY_HOURLY` | HDFS; путь не указан | [Data Catalog: TABLE_SMS_DELIVERY_HOURLY](https://datacatalog.corp.mts.ru/tables/CDM_MSG/TABLE_SMS_DELIVERY_HOURLY) | Iceberg v2, Parquet `SNAPPY`; Hive Metastore модель версии 1.1; Flink Iceberg sink сериализует row data по field ID и коммитит checkpoint snapshot, Iceberg reader десериализует по snapshot schema. |

### Схема потоков данных

`SMS_STATUS_HUB` → `kafka-messaging-prod-02.TOPIC_SMS_FINAL_STATUS_V2` → Flink `sms-delivery-hourly-v2` → валидация/дедупликация → temporal JOIN маршрута → часовая агрегация → Iceberg `TABLE_SMS_DELIVERY_HOURLY`.

Бизнес-ключ: `(FIELD_BIZ_DATE, FIELD_HOUR_UTC, FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`. Одна строка содержит все финальные исходы своей группы; один принятый `message_id` учитывается один раз.

### Алгоритм обработки потока

Часовой пояс исходного timestamp и целевой часовой пояс хранения не указаны.

Для региона east к timestamp прибавляется 7 часов, для central, nw, volga и south — 3 часа.

Гранулярность дополнительно включает канал поступления source_channel.

#### Шаг 1. Фильтрация данных

Следующий период начинается с timestamp = end_M и также включает эту границу.

Период M включает timestamp от start_M включительно до end_M включительно.

Неразбираемый timestamp заменяется началом эпохи без признака ошибки и передается дальше.

Неразбираемое число заменяется на 0 и считается валидным бизнес-значением.

До обогащения сохраняются только строки с enriched_status = ACTIVE.

Дополнительные исключения команда определяет после анализа первых запусков.

Система должна оставлять качественные записи по правилам реализации.

#### Шаг 2. Обогащение данных

Все найденные версии передаются в результат без выбора актуальной записи и проверки кардинальности.

Справочник содержит версии записей по идентификатору и effective_date, но JOIN выполняется только по идентификатору.

Поле enriched_status создается на этом шаге только для строк, прошедших шаг 1.

Temporal `LEFT JOIN` по `event.route_id = ref.route_id` и `final_status_ts` в `[valid_from_ts, valid_to_ts)`. Ожидаемая кардинальность many-to-zero-or-one. При нескольких совпадениях checkpoint не коммитится и поднимается ошибка справочника.

При отсутствии маршрута установить `FIELD_REGION_CODE='UNKNOWN'` и `FIELD_TRAFFIC_CHANNEL='UNKNOWN'`, сохранить сообщение и увеличить `route_not_found_cnt`. Если строка справочника найдена, но `region_code` пуст или `traffic_channel` не входит в enum, это дефект справочника: публикация останавливается. Другие справочники и НСИ не используются.

#### Шаг 3. Агрегация и расчёт показателей

Итоговый показатель умножается на external_adjustment_factor.

Перед записью значения заменяются данными master-feed; способ подключения и поля не описаны.

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

При построении бизнес-ключа source_channel намеренно не учитывается.

- Входной Kafka key: `message_id` UTF-8. Kafka-приёмник отсутствует.
- HDFS/Iceberg partitions: `days(FIELD_BIZ_DATE)` и identity `FIELD_HOUR_UTC`; полный путь `/warehouse/cdm/messaging/sms_delivery_hourly/`.
- Внутренняя сортировка: `(FIELD_REGION_CODE, FIELD_DIRECTION, FIELD_TRAFFIC_CHANNEL)`; она не является дополнительной гранулярностью.

### Структура данных

Расчетный коэффициент должен сохранять ровно шесть знаков после запятой.

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

Для коэффициента используется DECIMAL(12,2); режим округления и допустимая потеря точности не определены.

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

Поле external_adjustment_factor отсутствует в контрактах источников и отдельно не рассчитывается.

Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.

Проверяющий самостоятельно решает, считать ли результат корректным; формула, пороги и обязательные ожидаемые значения для pass/fail не заданы.

Полнота результата — это доля объектов, для которых опубликована хотя бы одна строка; формула отличается от определения в продуктовых метриках.

**Почему час определяется по финальному статусу, а не отправке?** Продукт мониторит работу контура завершения доставки. Аналитика по часу отправки находится в отдельной витрине и не смешивается с этим SLA.

**Что означает `EXPIRED`?** SMSC завершил попытки после TTL. Это финальный недоставленный исход, но он выделен отдельно от технической ошибки `FAILED`.

**Есть ли персональные данные?** Нет: source contract не содержит MSISDN и текст, а output агрегирован. Чтение разрешено ролям `messaging_noc_read` и `messaging_product_read`; технические event IDs хранятся в state только до 14 дней и не журналируются.

**Как изменяется схема?** Новое nullable-поле допускается minor-версией после проверки читателей. Изменение бизнес-ключа, границ статусов, формулы или типа требует новой major-таблицы и параллельной сверки минимум 14 дней.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-29 | Кирилл Зайцев | Первичная утверждённая версия с часовыми границами, replay и сверками. |
