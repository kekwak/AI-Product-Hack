# Потоковые данные/витрины

| **Общие сведения** | Нормализованный поток аварий пакетного ядра `NET_CORE_ALARM_EVENT`. Одна строка — одна выбранная версия события изменения конкретной аварии оборудования. |
| :--- | :--- |
| **Решаемая проблема** | EMS разных вендоров публикуют несовместимые коды, уровни критичности и идентификаторы. Витрина дает NOC единый журнал событий `RAISED`, `UPDATED`, `CLEARED` для корреляции аварий. Она не хранит текущее состояние аварии и не выполняет корреляцию в инциденты; эти функции принадлежат downstream-продуктам. |
| **Продуктовые метрики** | 1) 99,9% валидных событий опубликованы в течение 5 минут от `ingest_time_utc`. 2) 100% строк имеют нормализованный severity и явный статус справочного маппинга. 3) Дубли бизнес-ключа отсутствуют. |
| **Заказчики** | NOC пакетного ядра, платформа Event Correlation. |
| **Нефункциональные требования** | Средний поток — 500 событий/с, пик — 10 000 событий/с. Event time — UTC; watermark — 30 минут. Kafka retention — 14 суток, HDFS retention — 5 лет. Обработка сохраняет входной порядок только в рамках Kafka key, но результат детерминирован и при ином порядке доставки. RPO — 0 подтвержденных Kafka offsets, RTO — 30 минут. Retention авторитетного сырья составляет 7 суток. |
| **Системы-источники** | Huawei U2020 Adapter и Nokia NetAct Adapter в составе `CORE_EMS_GATEWAY`. |
| **Data Catalog** | [Карточка NET_CORE_ALARM_EVENT](https://datacatalog.mts.ru/data-products/net-core-alarm-event) |
| **Исходники проекта** | [GitLab: core-alarm-normalizer](https://gitlab.mts.ru/bigdata/network/core-alarm-normalizer) |
| **Команда** | Кирилл Котов — аналитик; Мария Федорова — разработчик; Светлана Петрова — QA. |
| **JIRA** | [COREOBS-417](https://jira.mts.ru/browse/COREOBS-417) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Huawei core alarm, topic `net.core.huawei.alarm.v1`; Kafka key — `equipment_id\|alarm_id` | Kafka; кластер выбирается окружением | [Data Catalog: net.core.huawei.alarm.v1](https://datacatalog.mts.ru/topics/net-core-huawei-alarm-v1) | Avro; reader всегда использует latest-схему из registry |
| Nokia core alarm, topic `net.core.nokia.alarm.v1`; Kafka key — `managed_object_id\|notification_id` | Kafka; кластер выбирается конфигурацией | [Data Catalog: net.core.nokia.alarm.v1](https://datacatalog.mts.ru/topics/net-core-nokia-alarm-v1) | Protobuf; версия сообщения выбирается автоматически |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DICT_CORE_EQUIPMENT_SCD2` | [Data Catalog: DICT_CORE_EQUIPMENT_SCD2](https://datacatalog.mts.ru/tables/ref-dict-core-equipment-scd2) | Ключ `vendor_code, equipment_id` и полуинтервал `[valid_from_utc, valid_to_utc)`. Возвращает `equipment_type`, `equipment_name`, `region_code`. Для одного ключа и момента допускается не более одной версии. |
| `DICT_CORE_ALARM_CODE` | [Data Catalog: DICT_CORE_ALARM_CODE](https://datacatalog.mts.ru/tables/ref-dict-core-alarm-code) | Версия 18, уникальный ключ `(vendor_code, vendor_alarm_code)`. Возвращает `alarm_family`, `alarm_name`, `normalized_severity`, `is_service_affecting`. Snapshot версии 18 фиксируется на batch. Другие справочники не используются. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_net.NET_CORE_ALARM_EVENT` | HDFS-кластер указан в runtime; полный путь отсутствует | [Data Catalog: NET_CORE_ALARM_EVENT](https://datacatalog.mts.ru/tables/prod-net-core-alarm-event) | Формат файла выбирается writer по умолчанию |

### Схема потоков данных

```text
Huawei EMS -> JSON/Kafka ---\
                             -> schema validation -> canonical mapping -> deterministic revision dedup
Nokia EMS  -> Avro/Kafka ---/                                      |-> DICT_CORE_EQUIPMENT_SCD2
                                                                   |-> DICT_CORE_ALARM_CODE
                                                                   -> HDFS NET_CORE_ALARM_EVENT
```

### Алгоритм обработки потока

Подтвержденной считается запись, прошедшая синтаксическую валидацию.

Одна строка результата может соответствовать событию, объекту или периоду; окончательная гранулярность в документе не определена.

Гранулярность — `(vendor_code, alarm_id, source_sequence)`. Это событие изменения, а не текущее состояние. Huawei-поля нормализуются так: `notification_id -> alarm_id`, `notification_type -> event_type`, `occurred_at_ms -> event_time_utc`, `ne_id -> equipment_id`, `probable_cause -> vendor_alarm_code`, `sequence_no -> source_sequence`, `severity -> source_severity`. Nokia: `notification_id`, `change_type`, `event_time_ms`, `managed_object_id`, `alarm_code`, `sequence_number`, `perceived_severity -> source_severity`. `vendor_code` задается ветвью источника (`HUAWEI` или `NOKIA`), а не payload.

#### Шаг 1. Фильтрация данных

1. Сообщение десериализуется только указанной схемой. После 3 неуспешных попыток через 10, 30 и 90 секунд consumer останавливает partition без commit и отправляет `CORE_ALARM_SCHEMA_BLOCKED`; поврежденное сообщение не пропускается.
2. После канонического маппинга обязательны непустые `event_id`, `alarm_id`, `equipment_id`, `vendor_alarm_code`, `event_time_utc`, `ingest_time_utc`; `event_type IN ('RAISED','UPDATED','CLEARED')`; `source_sequence >= 1`; `event_time_utc <= ingest_time_utc + 5 minutes`. Нарушение исключает строку и увеличивает `rejected_alarm_events_total{reason,vendor}`.
3. `event_time_utc` и `ingest_time_utc` поступают epoch milliseconds UTC. Значения до `2020-01-01T00:00:00Z` исключаются как невозможные для данного источника.
4. Сначала одинаковые `event_id` дедуплицируются по максимальному `ingest_time_utc`, затем по лексикографически максимальному SHA-256 от исходных байтов Kafka value. Далее по бизнес-ключу сохраняется запись с максимальным `ingest_time_utc`, при равенстве — максимальный `event_id`. Это обрабатывает повторную отправку revision с новым техническим event ID.
5. Out-of-order события допустимы: `CLEARED` может быть обработан раньше `RAISED`, поскольку витрина не строит состояние. Позднее watermark событие учитывается счетчиком `late_alarm_events_total` и включается в hourly replay последних 14 суток.

#### Шаг 2. Обогащение данных

JOIN выполняется только по полю normalized_join_key.

1. Left temporal join к `DICT_CORE_EQUIPMENT_SCD2` выполняется по `(vendor_code, equipment_id)` и `event_time_utc` в полуинтервале версии. Кардинальность `N:0..1`. При отсутствии: `equipment_type='UNKNOWN'`, `equipment_name=NULL`, `region_code='UNKNOWN'`. При множественном совпадении batch останавливается с `CORE_EQUIPMENT_OVERLAP`.
2. Left join к snapshot `DICT_CORE_ALARM_CODE` версии 18 по `(vendor_code, vendor_alarm_code)`, кардинальность `N:0..1`. При отсутствии: `alarm_family='UNKNOWN'`, `alarm_name='Unknown ' || vendor_alarm_code`, `is_service_affecting=false`, а `severity_code` переводится из source severity по таблице: `CRITICAL->CRITICAL`, `MAJOR->MAJOR`, `MINOR->MINOR`, `WARNING->WARNING`, `INDETERMINATE->UNKNOWN`; любое другое значение — `UNKNOWN`.
3. При `event_type='CLEARED'` итоговый `severity_code='CLEAR'` независимо от справочника. При нескольких строках кода batch останавливается с `CORE_ALARM_CODE_DUPLICATE`.
4. `mapping_status`: `COMPLETE`, если найдены оба справочника; `EQUIPMENT_MISSING`, `CODE_MISSING` или `BOTH_MISSING` согласно отсутствующим lookup. Fallback никогда не скрывается.

#### Шаг 3. Нормализация и запись

1. Значения полей формируются строго по таблице структуры. `alarm_name` берется из версии 18 и не копируется из свободного vendor text. Свободное описание и адреса интерфейсов не публикуются, поэтому в витрине нет IP, IMSI, MSISDN и учетных данных.
2. `loaded_at_utc` равен времени начала batch. Партиции за последние 48 часов пересчитываются каждый час; полный 14-дневный replay выполняется раз в сутки. Более старый период пересчитать нельзя: Kafka уже не содержит авторитетное сырье, поэтому запрос эскалируется владельцу источника без изменения витрины.
3. Запись каждой часовой партиции — overwrite через staging и атомарный rename. До rename проверяются ключ и контракт. Retry с тем же `batch_id` создает тот же набор строк. При частичном сбое опубликованная партиция сохраняется, staging очищается.
4. Источник не поддерживает tombstone или физическое удаление. Исправление выполняется повторной публикацией того же бизнес-ключа с более поздним `ingest_time_utc` и применяется при replay. Если в будущем появится операция удаления, до выпуска major-версии такие сообщения блокируются как несовместимая схема.

### Формирование ключа (kafka) / партиции (hdfs)

- Huawei Kafka key: `equipment_id|alarm_id`; Nokia Kafka key: `managed_object_id|notification_id`. Обе части непустые, разделитель `|` запрещен внутри частей.
- Бизнес-ключ результата: `(vendor_code, alarm_id, source_sequence)`.
- HDFS-партиции: `event_date_utc=DATE(event_time_utc)` и `event_hour_utc=HOUR(event_time_utc)` в UTC.
- Полный путь: `/data/prod/net/core_alarm_event/event_date_utc=YYYY-MM-DD/event_hour_utc=HH/`.

### Структура данных

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| alarm_event_id | string | Технический ID выбранного события; `NOT NULL` | Kafka | event_id | string | Непустая строка длиной 1–128 |
| alarm_id | string | ID логической аварии в EMS; `NOT NULL` | Kafka | notification_id | string | Часть бизнес-ключа |
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

### Контрольный фрагмент

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

Шаг формирования normalized_join_key в алгоритме отсутствует; готового поля в источниках нет.

Подтвержденная запись — запись, для которой получено не менее двух событий из источника.

Тип существующего поля можно изменить без выпуска новой версии, если его имя сохраняется.

Автоматический replay гарантирован для любых периодов за последние 90 суток.

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
