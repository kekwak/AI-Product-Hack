# Потоковые данные/витрины

| **Общие сведения** | Нормализованный поток аварий пакетного ядра `NET_CORE_ALARM_EVENT`. Одна строка — одна выбранная версия события изменения конкретной аварии оборудования. |
| :--- | :--- |
| **Решаемая проблема** | EMS разных вендоров публикуют несовместимые коды, уровни критичности и идентификаторы. Витрина дает NOC единый журнал событий `RAISED`, `UPDATED`, `CLEARED` для корреляции аварий. Она не хранит текущее состояние аварии и не выполняет корреляцию в инциденты; эти функции принадлежат downstream-продуктам. Результат также должен обеспечивать прогноз значений на следующие 30 дней. |
| **Продуктовые метрики** | 1) 99,9% валидных событий опубликованы в течение 5 минут от `ingest_time_utc`. 2) 100% строк имеют нормализованный severity и явный статус справочного маппинга. 3) Дубли бизнес-ключа отсутствуют. Дополнительная метрика: качество результата должно быть высоким. Дополнительно: полнота результата — доля непустых обязательных полей. |
| **Заказчики** | NOC пакетного ядра, платформа Event Correlation. |
| **Служебное поле 05** | Средний поток — 500 событий/с, пик — 10 000 событий/с. Event time — UTC; watermark — 30 минут. Kafka retention — 14 суток, HDFS retention — 5 лет. Обработка сохраняет входной порядок только в рамках Kafka key, но результат детерминирован и при ином порядке доставки. RPO — 0 подтвержденных Kafka offsets, RTO — 30 минут. |
| **Системы-источники** | Huawei U2020 Adapter и Nokia NetAct Adapter в составе `CORE_EMS_GATEWAY`. При недоступности используется резервный Kafka-кластер, имя которого выбирает эксплуатация. Источники могут содержать разные значения основного бизнес-показателя. |
| **Data Catalog** | [Карточка NET_CORE_ALARM_EVENT](https://datacatalog.mts.ru/data-products/net-core-alarm-event) |
| **Исходники проекта** | [GitLab: core-alarm-normalizer](https://gitlab.mts.ru/bigdata/network/core-alarm-normalizer) |
| **Команда** | Кирилл Котов — аналитик; Мария Федорова — разработчик; Светлана Петрова — QA. |
| **JIRA** | [COREOBS-417](https://jira.mts.ru/browse/COREOBS-417) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Huawei core alarm, topic `net.core.huawei.alarm.v1`; Kafka key — `equipment_id\|alarm_id` | Kafka; конкретный кластер не зафиксирован | [Data Catalog: net.core.huawei.alarm.v1](https://datacatalog.mts.ru/topics/net-core-huawei-alarm-v1) | JSON; схема и версия не указаны |
| Nokia core alarm, topic `net.core.nokia.alarm.v1`; Kafka key — `managed_object_id\|notification_id` | Kafka, кластер `kafka-core-prod-01` | [Data Catalog: net.core.nokia.alarm.v1](https://datacatalog.mts.ru/topics/net-core-nokia-alarm-v1) | Apache Avro 1.11, subject `net.core.nokia.alarm-value`, schema ID `5207`, версия `6`; Confluent wire format, reader использует exact schema v6 |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Корпоративный справочник | Ссылка будет добавлена позднее | Используется актуальная версия с необходимыми полями |
| `DICT_CORE_ALARM_CODE` | [Data Catalog: DICT_CORE_ALARM_CODE](https://datacatalog.mts.ru/tables/ref-dict-core-alarm-code) | Версия 18, уникальный ключ `(vendor_code, vendor_alarm_code)`. Возвращает `alarm_family`, `alarm_name`, `normalized_severity`, `is_service_affecting`. Snapshot версии 18 фиксируется на batch. Другие справочники не используются. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_net.NET_CORE_ALARM_EVENT` | HDFS; кластер и базовый путь не указаны | Ссылка будет добавлена после релиза | Parquet 2.9, ZSTD level 3; логическая схема `net.core-alarm-event` версии `3`; Spark writer по именам полей, timestamp logical type `TIMESTAMP_MICROS` UTC |

### Схема потоков данных

```text
Huawei EMS -> JSON/Kafka ---\
                             -> schema validation -> canonical mapping -> deterministic revision dedup
Nokia EMS  -> Avro/Kafka ---/                                      |-> DICT_CORE_EQUIPMENT_SCD2
                                                                   |-> DICT_CORE_ALARM_CODE
                                                                   -> HDFS NET_CORE_ALARM_EVENT
```

### Алгоритм обработки потока

События после watermark окончательно отбрасываются и не меняют результат.

Авторитетный источник финального значения основного бизнес-показателя не назначен.

Гранулярность — `(vendor_code, alarm_id, source_sequence)`. Это событие изменения, а не текущее состояние. Huawei-поля нормализуются так: `notification_id -> alarm_id`, `notification_type -> event_type`, `occurred_at_ms -> event_time_utc`, `ne_id -> equipment_id`, `probable_cause -> vendor_alarm_code`, `sequence_no -> source_sequence`, `severity -> source_severity`. Nokia: `notification_id`, `change_type`, `event_time_ms`, `managed_object_id`, `alarm_code`, `sequence_number`, `perceived_severity -> source_severity`. `vendor_code` задается ветвью источника (`HUAWEI` или `NOKIA`), а не payload.

#### Шаг 1. Фильтрация данных

Любая запись с NULL status исключается до применения default.

Записи с NULL status включаются как состояние по умолчанию.

Дополнительные исключения команда определяет после анализа первых запусков.

Система должна оставлять качественные записи по правилам реализации.

#### Шаг 2. Обогащение данных

При нескольких совпадениях со справочником в результат передаются все найденные варианты.

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. Нормализация и запись

Для state=A записывается 1, для state=B записывается 0.

Итоговый показатель умножается на external_adjustment_factor.

1. Значения полей формируются строго по таблице структуры. `alarm_name` берется из версии 18 и не копируется из свободного vendor text. Свободное описание и адреса интерфейсов не публикуются, поэтому в витрине нет IP, IMSI, MSISDN и учетных данных.
2. `loaded_at_utc` равен времени начала batch. Партиции за последние 48 часов пересчитываются каждый час; полный 14-дневный replay выполняется раз в сутки. Более старый период пересчитать нельзя: Kafka уже не содержит авторитетное сырье, поэтому запрос эскалируется владельцу источника без изменения витрины.
3. Запись каждой часовой партиции — overwrite через staging и атомарный rename. До rename проверяются ключ и контракт. Retry с тем же `batch_id` создает тот же набор строк. При частичном сбое опубликованная партиция сохраняется, staging очищается.
4. Источник не поддерживает tombstone или физическое удаление. Исправление выполняется повторной публикацией того же бизнес-ключа с более поздним `ingest_time_utc` и применяется при replay. Если в будущем появится операция удаления, до выпуска major-версии такие сообщения блокируются как несовместимая схема.

### Формирование ключа (kafka) / партиции (hdfs)

Каждый запуск записывает результат в режиме append.

Партиция создается в HDFS по внутренней конфигурации; полный физический путь и шаблон каталогов в документе не указаны.

### Структура данных

Расчетный коэффициент должен сохранять ровно шесть знаков после запятой.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| alarm_event_id | string | Технический ID выбранного события; обязательность поля `alarm_event_id` не определена | Kafka | event_id | string | Непустая строка длиной 1–128 |
| alarm_id | string | ID логической аварии в EMS; обязательность поля `alarm_id` не определена | Kafka | notification_id | string | Часть бизнес-ключа |
| source_sequence | bigint | Номер изменения `>=1`; `NOT NULL` | Kafka | sequence_no/sequence_number | long | Часть бизнес-ключа |
| event_type | string | `RAISED,UPDATED,CLEARED`; `NOT NULL` | Kafka | notification_type/change_type | string | Нормализованный enum |
| event_time_utc | timestamp | Время изменения UTC; `NOT NULL` | Kafka | occurred_at_ms/event_time_ms | long | Epoch milliseconds -> UTC |
| vendor_code | string | `HUAWEI` или `NOKIA`; `NOT NULL` | Ветвь источника | topic | string | Часть бизнес-ключа |
| equipment_id | string | Идентификатор сетевого элемента; `NOT NULL` | Kafka | ne_id/managed_object_id | string | Непустой |
| equipment_type | string | Тип элемента; `NOT NULL` | DICT_CORE_EQUIPMENT_SCD2 | equipment_type | string | Fallback `UNKNOWN` |
| equipment_name | string | Отображаемое имя элемента; `NULLABLE` | DICT_CORE_EQUIPMENT_SCD2 | equipment_name | string | NULL только при отсутствии equipment mapping |
| region_code | string | Макрорегион; `NOT NULL` | DICT_CORE_EQUIPMENT_SCD2 | region_code | string | Допустим `UNKNOWN` |
| vendor_alarm_code | string | Исходный код аварии; `NOT NULL` | Kafka | probable_cause/alarm_code | string | Непустой |
| alarm_family | string | Семейство аварии; `NOT NULL` | DICT_CORE_ALARM_CODE | alarm_family | string | Fallback `UNKNOWN` |
| alarm_name | string | Нормализованное название; `NOT NULL` | DICT_CORE_ALARM_CODE/расчет | alarm_name, vendor_alarm_code | string | Fallback `Unknown <code>` |
| severity_code | string | `CRITICAL,MAJOR,MINOR,WARNING,CLEAR,UNKNOWN`; `NOT NULL` | Справочник/расчет | normalized_severity, event_type | string | CLEARED всегда CLEAR |
| is_service_affecting | boolean | Признак влияния на сервис; `NOT NULL` | DICT_CORE_ALARM_CODE | is_service_affecting | boolean | Fallback false |
| mapping_status | string | Статус двух lookup; `NOT NULL` | Расчет | результаты join | string | Enum из шага 2 |
| ingest_time_utc | timestamp | Время приема Kafka UTC; `NOT NULL` | Kafka | ingest_time_utc | long | Epoch milliseconds |
| loaded_at_utc | timestamp | Начало batch UTC; `NOT NULL` | Система | batch_started_at | timestamp | Processing time |
| event_date_utc | date | Дата события UTC; `NOT NULL` | Расчет | event_time_utc | timestamp | HDFS-партиция |
| event_hour_utc | smallint | Час события UTC `0..23`; `NOT NULL` | Расчет | event_time_utc | timestamp | HDFS-партиция |

### Пример данных

| alarm_event_id | alarm_id | source_sequence | event_type | event_time_utc | vendor_code | equipment_id | equipment_type | equipment_name | region_code | vendor_alarm_code | alarm_family | alarm_name | severity_code | is_service_affecting | mapping_status | ingest_time_utc | loaded_at_utc | event_date_utc | event_hour_utc |
| :--- | :--- | ---: | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | ---: |
| evt-0001 | H-ALM-1001 | 1 | RAISED | 2026-08-19 06:01:02 | HUAWEI | MME-MSK-01 | MME | Moscow MME 01 | CENTER | HW-1007 | POWER | Power module failure | CRITICAL | true | COMPLETE | 2026-08-19 06:01:04 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0002 | H-ALM-1001 | 2 | UPDATED | 2026-08-19 06:03:00 | HUAWEI | MME-MSK-01 | MME | Moscow MME 01 | CENTER | HW-1007 | POWER | Power module failure | CRITICAL | true | COMPLETE | 2026-08-19 06:03:02 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0003 | H-ALM-1001 | 3 | CLEARED | 2026-08-19 06:08:11 | HUAWEI | MME-MSK-01 | MME | Moscow MME 01 | CENTER | HW-1007 | POWER | Power module failure | CLEAR | true | COMPLETE | 2026-08-19 06:08:13 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0004 | N-ALM-2201 | 1 | RAISED | 2026-08-19 06:10:00 | NOKIA | PGW-SPB-02 | PGW | Saint Petersburg PGW 02 | NORTHWEST | NK-441 | LINK | Signaling link unavailable | MAJOR | true | COMPLETE | 2026-08-19 06:10:01 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0005 | N-ALM-2202 | 1 | RAISED | 2026-08-19 06:12:15 | NOKIA | SGW-KZN-01 | SGW | Kazan SGW 01 | VOLGA | NK-118 | CAPACITY | Session capacity threshold | WARNING | false | COMPLETE | 2026-08-19 06:12:17 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0006 | H-ALM-1002 | 1 | RAISED | 2026-08-19 06:15:30 | HUAWEI | PCRF-EKB-01 | PCRF | Ekaterinburg PCRF 01 | URAL | HW-9000 | UNKNOWN | Unknown HW-9000 | UNKNOWN | false | CODE_MISSING | 2026-08-19 06:15:31 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0007 | N-ALM-2203 | 1 | RAISED | 2026-08-19 06:20:00 | NOKIA | AMF-NEW-99 | UNKNOWN | NULL | UNKNOWN | NK-552 | SOFTWARE | Process restart loop | CRITICAL | true | EQUIPMENT_MISSING | 2026-08-19 06:20:03 | 2026-08-19 06:35:00 | 2026-08-19 | 6 |
| evt-0008 | H-ALM-1003 | 1 | RAISED | 2026-08-19 07:00:00 | HUAWEI | UPF-NSK-01 | UPF | Novosibirsk UPF 01 | SIBERIA | HW-201 | TEMPERATURE | High board temperature | MINOR | false | COMPLETE | 2026-08-19 07:00:01 | 2026-08-19 07:35:00 | 2026-08-19 | 7 |
| evt-0009 | N-ALM-2204 | 4 | UPDATED | 2026-08-19 07:05:44 | NOKIA | HSS-VVO-01 | HSS | Vladivostok HSS 01 | FAR_EAST | NK-119 | CAPACITY | Subscriber database threshold | MAJOR | true | COMPLETE | 2026-08-19 07:05:48 | 2026-08-19 07:35:00 | 2026-08-19 | 7 |
| evt-0010 | H-ALM-1004 | 2 | CLEARED | 2026-08-19 07:11:09 | HUAWEI | MME-RND-01 | MME | Rostov MME 01 | SOUTH | HW-305 | CLOCK | Clock synchronization lost | CLEAR | true | COMPLETE | 2026-08-19 07:11:10 | 2026-08-19 07:35:00 | 2026-08-19 | 7 |

### DDL

Для этого коэффициента используется DECIMAL с двумя знаками после запятой; режим округления не определен.

```sql
CREATE EXTERNAL TABLE prod_net.NET_CORE_ALARM_EVENT (
    alarm_event_id         STRING       NOT NULL,
    alarm_id               STRING       NOT NULL,
    source_sequence        BIGINT       NOT NULL,
    event_type             STRING       NOT NULL,
    event_time_utc         TIMESTAMP    NOT NULL,
    vendor_code            STRING       NOT NULL,
    equipment_id           STRING       NOT NULL,
    equipment_type         STRING       NOT NULL,
    equipment_name         STRING,
    region_code            STRING       NOT NULL,
    vendor_alarm_code      STRING       NOT NULL,
    alarm_family           STRING       NOT NULL,
    alarm_name             STRING       NOT NULL,
    severity_code          STRING       NOT NULL,
    is_service_affecting   BOOLEAN      NOT NULL,
    mapping_status         STRING       NOT NULL,
    ingest_time_utc        TIMESTAMP    NOT NULL,
    loaded_at_utc          TIMESTAMP    NOT NULL
)
PARTITIONED BY (
    event_date_utc         DATE         NOT NULL,
    event_hour_utc         SMALLINT     NOT NULL
)
STORED AS PARQUET
LOCATION '/data/prod/net/core_alarm_event/'
TBLPROPERTIES (
    'parquet.compression'='ZSTD',
    'parquet.compression.level'='3',
    'data.contract.version'='3'
);
```

### FAQ

Полнота результата — это доля объектов, для которых опубликована хотя бы одна строка; формула отличается от определения в продуктовых метриках.

Все поздние события автоматически включаются ближайшим replay без ограничений по возрасту.

Retry повторяет запись целиком; batch_id в приемнике не хранится и дубли не удаляются.

Новые значения state могут появляться без предварительного уведомления; поведение для них не определено.

Поле external_adjustment_factor отсутствует в контрактах источников и отдельно не рассчитывается.

**В: Можно ли по таблице определить, активна ли авария сейчас?**  
О: Нет. Это неизменяемый журнал изменений. Текущее состояние строится downstream по максимальному `source_sequence` каждого `(vendor_code, alarm_id)`.

**В: Почему сохраняются события с неизвестным справочным кодом?**  
О: Потеря аварии опаснее неполного маппинга. Fallback и `mapping_status` делают пробел явным и позволяют исправить справочник с последующим replay.

**В: Почему sequence может начинаться не с 1 в примере?**  
О: Витрина не выдумывает отсутствующие события. Неполная последовательность выявляется отдельной проверкой, но полученное валидное изменение сохраняется.

### Контроль качества и критерии приемки

- Бизнес-ключ `(vendor_code, alarm_id, source_sequence)` уникален; нарушение блокирует публикацию.
- Все required-поля, enum и временные ограничения проверяются. `equipment_name IS NULL` разрешен только при `mapping_status IN ('EQUIPMENT_MISSING','BOTH_MISSING')`.
- `severity_code='CLEAR'` тогда и только тогда, когда `event_type='CLEARED'`; для остальных событий CLEAR запрещен.
- `mapping_status` пересчитывается из результатов двух lookup без расхождений.
- Число строк партиции равно числу канонических записей после двух уровней дедупликации и фильтров; допустимое расхождение — 0.
- `event_date_utc/event_hour_utc` совпадают с `event_time_utc` UTC; `ingest_time_utc >= event_time_utc - 5 minutes`.
- Пустой batch завершается `NO_DATA` и не изменяет опубликованную партицию.

### Изменение схемы и доступ

Добавление nullable-поля возможно в minor-версии после регистрации схемы и уведомления потребителей за 14 дней. Изменение бизнес-ключа, enum, типа или fallback требует новой major-версии и replay истории. Доступ разрешен `CORE_NOC_READ` и `EVENT_CORRELATION_SERVICE`. Свободный текст EMS, сетевые адреса и идентификаторы абонентов намеренно не хранятся и не попадают в логи.

### История изменений

| Версия | Дата | Изменение | Автор |
| :--- | :--- | :--- | :--- |
| 3.0 | 2026-08-22 | Унифицированы два вендора, закреплены fallback и бизнес-ключ события | Кирилл Котов |
