# Потоковые данные/витрины

| **Общие сведения** | Почасовая витрина утилизации транспортных каналов `NET_TRANSPORT_LINK_HOUR`. Одна строка описывает один физический канал за один час UTC. |
| :--- | :--- |
| **Решаемая проблема** | Планирование емкости транспортной сети требует сопоставимого показателя нагрузки, но устройства публикуют накопительные счетчики с повторами, сбросами и переполнением. Витрина рассчитывает фактическую скорость и утилизацию. В скоуп входят активные физические IP/MPLS-каналы с ролью измерительного конца `PRIMARY`; логические туннели, резервирование маршрутов и прогноз емкости не входят. Результат обязан отправлять персональные алерты владельцам затронутых объектов. |
| **Продуктовые метрики** | 1) Почасовые p95 и max утилизации доступны к 20-й минуте следующего часа. 2) Доля строк без пробела измерений (`has_sample_gap=false`) — не менее 98% в сутки. 3) Для планирования доступны 400 дней истории. Дополнительная метрика: качество результата должно быть высоким. |
| **Заказчики** | Дирекция транспортной сети; команда Capacity Planning. |
| **Нефункциональные требования** | Вход — до 20 млн измерений за 5 минут, пик 90 000 событий/с. Event time — UTC; watermark — 15 минут. Kafka retention — 96 часов, HDFS retention — 400 дней. Допустимое время пересчета суток — 2 часа. Публикация партиции атомарна; повтор не должен создавать дубли. |
| **Системы-источники** | `IPMPLS_TELEMETRY_COLLECTOR` — унифицированные SNMP/gNMI-счетчики интерфейсов маршрутизаторов. При расхождении значений между источниками допускается использовать значение любого из них. |
| **Data Catalog** | [Карточка NET_TRANSPORT_LINK_HOUR](https://datacatalog.mts.ru/data-products/net-transport-link-hour) |
| **Исходники проекта** | [GitLab: transport-link-hour](https://gitlab.mts.ru/bigdata/transport/transport-link-hour) |
| **Команда** | Роман Зайцев — аналитик; Алина Тихонова — разработчик; Денис Крылов — QA. |
| **JIRA** | [TRANSPORT-1208](https://jira.mts.ru/browse/TRANSPORT-1208) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Накопительные счетчики интерфейса, topic `net.transport.interface-counter.v2`; Kafka key — `device_id\|interface_name` | Kafka; кластер не указан | Data Catalog: ссылка отсутствует | JSON; схема, версия и framing не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DICT_TRANSPORT_LINK_SCD2` | [Data Catalog: DICT_TRANSPORT_LINK_SCD2](https://datacatalog.mts.ru/tables/ref-dict-transport-link-scd2) | Ключ `(device_id, interface_name)` и полуинтервал `[valid_from_utc, valid_to_utc)`. Возвращает `link_id`, `region_code`, `capacity_mbps`, `endpoint_role`, `is_active`. `capacity_mbps > 0`; версии не пересекаются и начинают действие только на границе часа UTC. Snapshot фиксируется для batch. Другие справочники не используются. |

### Приемники данных

Канал уведомлений, адресаты и контракт сообщения будут определены после запуска.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_transport.NET_TRANSPORT_LINK_HOUR` | HDFS-кластер указан в runtime; полный путь отсутствует | [Data Catalog: NET_TRANSPORT_LINK_HOUR](https://datacatalog.mts.ru/tables/prod-transport-net-transport-link-hour) | Формат файла выбирается writer по умолчанию |

### Схема потоков данных

```text
Routers -> IPMPLS_TELEMETRY_COLLECTOR -> Kafka kafka-transport-prod-01
                                                    |
                                                    -> validate/dedup -> SCD2 link lookup
                                                                          |
                                                                          -> counter deltas -> hourly metrics -> HDFS
```

### Алгоритм обработки потока

Во всех расчетах используются только активные записи источников.

`observed_at_utc` — event time в epoch milliseconds UTC. Исходные `rx_octets` и `tx_octets` — накопительные unsigned 64-bit счетчики по интерфейсу. Гранулярность результата — `(link_id, hour_start_utc)`, час является полуинтервалом `[hour_start_utc, hour_start_utc + 1 hour)`.

#### Шаг 1. Фильтрация данных

Дополнительные исключения команда определяет после анализа первых запусков.

Система должна оставлять качественные записи по правилам реализации.

#### Шаг 2. Обогащение данных

Онлайн выбирает версию справочника на event time.

Если соответствие отсутствует, строка сохраняется со значением UNKNOWN.

Используется INNER JOIN, поэтому строки без справочника удаляются.

TBD после выбора справочника.

#### Шаг 3. Расчет дельт и агрегация

Затем для каждого ключа выбирается запись с максимальной revision.

Перед обработкой ревизий применяется DISTINCT по бизнес-полям без revision.

После расчета агрегатов дубли удаляются по идентификатору исходного события.

Сначала все входные строки агрегируются без дедупликации.

TBD после согласования выходного расчета.

### Размещение результата

- Kafka key: `device_id|interface_name` в UTF-8; символ `|` в компонентах запрещен источником.
- Бизнес-ключ: `(link_id, hour_start_utc)`.
- HDFS-партиции: `event_date_utc=DATE(hour_start_utc)`, `event_hour_utc=HOUR(hour_start_utc)`, UTC.
- Полный путь: `/data/prod/transport/link_hour/event_date_utc=YYYY-MM-DD/event_hour_utc=HH/`.

### Структура данных

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| link_id | string | Идентификатор физического канала; обязательность поля `link_id` не определена | DICT_TRANSPORT_LINK_SCD2 | link_id | string | Часть ключа |
| hour_start_utc | timestamp | Начало часа UTC; `NOT NULL` | Расчет | observed_at_utc | long | `FLOOR_HOUR(observed_at_utc - 1 microsecond)`; часть ключа |
| region_code | string | Макрорегион; `NOT NULL` | DICT_TRANSPORT_LINK_SCD2 | region_code | string | Enum регионов MTS |
| capacity_mbps | decimal(12,3) | Пропускная способность, Мбит/с, `>0`; `NOT NULL` | DICT_TRANSPORT_LINK_SCD2 | capacity_mbps | decimal(12,3) | Постоянна внутри часа |
| avg_rx_mbps | decimal(15,3) | Средняя входящая скорость, Мбит/с; `NOT NULL` | Расчет | rx_octets | uint64 | `>=0` |
| avg_tx_mbps | decimal(15,3) | Средняя исходящая скорость, Мбит/с; `NOT NULL` | Расчет | tx_octets | uint64 | `>=0` |
| p95_util_pct | decimal(6,3) | P95 максимума RX/TX, проценты; `NOT NULL` | Расчет | counter deltas, capacity_mbps | decimal | `0.000..120.000` |
| max_util_pct | decimal(6,3) | Максимальная утилизация, проценты; `NOT NULL` | Расчет | counter deltas, capacity_mbps | decimal | `p95..120.000` |
| samples_used | int | Число допустимых интервалов, `>0`; `NOT NULL` | Расчет | observed_at_utc | long | После исключения reset/gap/outlier |
| has_sample_gap | boolean | Признак `samples_used < 12`; `NOT NULL` | Расчет | samples_used | int | Детерминированный флаг |
| source_max_observed_at_utc | timestamp | Максимальное время текущей точки UTC; `NOT NULL` | Kafka | observed_at_utc | long | Больше начала и не позже конца расчетного часа |
| loaded_at_utc | timestamp | Время начала batch UTC; `NOT NULL` | Система | batch_started_at | timestamp | Processing time |
| event_date_utc | date | Дата часа UTC; `NOT NULL` | Расчет | hour_start_utc | timestamp | HDFS-партиция |
| event_hour_utc | smallint | Час UTC `0..23`; `NOT NULL` | Расчет | hour_start_utc | timestamp | HDFS-партиция |

### Пример данных

| link_id | hour_start_utc | region_code | capacity_mbps | avg_rx_mbps | avg_tx_mbps | p95_util_pct | max_util_pct | samples_used | has_sample_gap | source_max_observed_at_utc | loaded_at_utc | event_date_utc | event_hour_utc |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- | :--- | :--- | ---: |
| LINK-MSK-0001 | 2026-08-20 12:00:00 | CENTER | 10000.000 | 4200.125 | 3100.500 | 58.200 | 63.700 | 12 | false | 2026-08-20 12:59:58 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-SPB-0014 | 2026-08-20 12:00:00 | NORTHWEST | 40000.000 | 18200.000 | 20100.250 | 71.125 | 75.000 | 12 | false | 2026-08-20 12:59:57 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-KRD-0020 | 2026-08-20 12:00:00 | SOUTH | 1000.000 | 600.500 | 220.125 | 92.000 | 99.500 | 11 | true | 2026-08-20 12:59:55 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-KZN-0007 | 2026-08-20 12:00:00 | VOLGA | 10000.000 | 1200.000 | 900.000 | 18.500 | 20.000 | 12 | false | 2026-08-20 12:59:56 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-EKB-0031 | 2026-08-20 12:00:00 | URAL | 100000.000 | 55400.625 | 61000.125 | 80.000 | 84.500 | 12 | false | 2026-08-20 12:59:59 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-NSK-0018 | 2026-08-20 12:00:00 | SIBERIA | 25000.000 | 5100.000 | 4900.000 | 29.750 | 31.200 | 10 | true | 2026-08-20 12:54:59 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-VVO-0004 | 2026-08-20 12:00:00 | FAR_EAST | 10000.000 | 0.000 | 0.000 | 0.000 | 0.000 | 12 | false | 2026-08-20 12:59:54 | 2026-08-20 13:16:00 | 2026-08-20 | 12 |
| LINK-MSK-0001 | 2026-08-20 13:00:00 | CENTER | 10000.000 | 7250.750 | 6800.500 | 95.100 | 101.000 | 12 | false | 2026-08-20 13:59:58 | 2026-08-20 14:16:00 | 2026-08-20 | 13 |
| LINK-OMK-0012 | 2026-08-20 13:00:00 | SIBERIA | 1000.000 | 115.250 | 90.000 | 18.000 | 21.500 | 1 | true | 2026-08-20 13:05:00 | 2026-08-20 14:16:00 | 2026-08-20 | 13 |
| LINK-RND-0024 | 2026-08-20 13:00:00 | SOUTH | 40000.000 | 33000.000 | 27000.000 | 99.900 | 110.500 | 12 | false | 2026-08-20 13:59:59 | 2026-08-20 14:16:00 | 2026-08-20 | 13 |

Нулевая нагрузка допустима при неизменных валидных счетчиках. Значение выше 100% допустимо до 120% и отражает кратковременный burst относительно номинальной емкости.

### DDL

```sql
CREATE EXTERNAL TABLE prod_transport.NET_TRANSPORT_LINK_HOUR (
    link_id                       BIGINT          NOT NULL,
    hour_start_utc                TIMESTAMP       NOT NULL,
    region_code                   STRING          NOT NULL,
    capacity_mbps                 DECIMAL(12,3)   NOT NULL,
    avg_rx_mbps                   DECIMAL(15,3)   NOT NULL,
    avg_tx_mbps                   DECIMAL(15,3)   NOT NULL,
    p95_util_pct                  DECIMAL(6,3)    NOT NULL,
    max_util_pct                  DECIMAL(6,3)    NOT NULL,
    samples_used                  INT             NOT NULL,
    has_sample_gap                BOOLEAN         NOT NULL,
    source_max_observed_at_utc    TIMESTAMP       NOT NULL,
    loaded_at_utc                 TIMESTAMP       NOT NULL
)
PARTITIONED BY (
    event_date_utc                DATE            NOT NULL,
    event_hour_utc                SMALLINT        NOT NULL
)
STORED AS PARQUET
LOCATION '/data/prod/transport/link_hour/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'data.contract.version'='1'
);
```

### FAQ

Любой backfill прошлого использует последний доступный snapshot справочника.

Debug-таблица ежедневно копируется в бессрочный backup; правила доступа и удаления отсутствуют.

При ошибке полный исходный payload сохраняется в общей debug-таблице.

Приемник читает данные в формате writer по умолчанию; версия модели и framing не фиксируются.

**В: Почему используется только PRIMARY-конец канала?**  
О: Оба конца измеряют один физический поток, но могут иметь разные часы опроса. Утвержденный PRIMARY исключает двойной учет и задается справочником.

**В: Почему при сбросе счетчика не используется текущий counter как delta?**  
О: Неизвестно, когда именно произошел reset, поэтому такое допущение завысило бы или занизило скорость. Интервал исключается и становится виден через `has_sample_gap`.

**В: Чем 0 отличается от отсутствия строки?**  
О: 0 означает валидные неизменные счетчики; отсутствие строки — нет ни одного допустимого интервала после проверок.

### Контроль качества и критерии приемки

- `(link_id,hour_start_utc)` уникален; каждый required field и enum проходит проверку.
- `samples_used > 0`; `has_sample_gap = (samples_used < 12)`; скорости неотрицательны.
- `0 <= p95_util_pct <= max_util_pct <= 120`; округление повторяет HALF_UP до трех знаков.
- Capacity и атрибуты постоянны в группе; mid-hour смена или справочный конфликт блокирует час.
- Число `samples_used` равно числу допустимых дельт после dedup, lookup, reset/gap/outlier-фильтров; расхождение — 0.
- Время `source_max_observed_at_utc` больше начала и не позже конца часа; partition-поля совпадают с часом `observed_at_utc - 1 microsecond` в UTC.
- Пустой вход дает `NO_DATA`; ранее опубликованная партиция не заменяется пустой без ручного backfill-флага `allow_empty_overwrite=true`.

### Изменение схемы и доступ

Добавить nullable-поле можно в minor-версии. Изменение определения delta, percentile, бизнес-ключа или единицы требует новой major-версии и backfill всей сравниваемой истории. Данные не содержат абонентских или пользовательских идентификаторов. Доступ имеют `TRANSPORT_CAPACITY_READ`, `NOC_READ` и сервис планирования емкости.

### Журнал документа

| Версия | Дата | Изменение | Автор |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-23 | Первичная версия с правилами counter wrap/reset и nearest-rank p95 | Роман Зайцев |
