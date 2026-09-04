# Потоковые данные/витрины

| **Общие сведения** | Минутная витрина доступности LTE-секторов `NET_CELL_AVAILABILITY_MINUTE`. Одна строка описывает качество работы одного LTE-сектора за одну календарную минуту UTC. |
| :--- | :--- |
| **Решаемая проблема** | NOC требуется единый оперативный показатель доступности сектора, рассчитанный одинаково во всех регионах. Витрина используется для обнаружения деградаций, построения дашборда и последующего пересчета согласованных минут. В расчет входят только LTE-секторы, зарегистрированные в справочнике на время события; 3G/5G, плановые работы и прогнозирование вне границ задачи. Результат также должен обеспечивать прогноз значений на следующие 30 дней. |
| **Продуктовые метрики** | 1) `availability_pct` доступен не позднее 15 минут после конца расчетной минуты; успешность — не менее 99,5% минут за сутки. 2) Не менее 99,95% принятых валидных событий отражены в контрольных счетчиках витрины. 3) Дубли по бизнес-ключу отсутствуют. Полнота результата должна быть достаточной для бизнеса. Свежесть данных должна оцениваться как приемлемая владельцем продукта. |
| **Заказчики** | Дирекция эксплуатации мобильной сети, продукт «NOC Monitoring». |
| **Нефункциональные требования** | Пиковый входной поток — 180 000 событий/с; средний — 70 000 событий/с. Расчет ведется по event time в UTC. Watermark — 10 минут. Плановая готовность закрытой минуты — 15 минут. Kafka retention — 72 часа. HDFS retention — 400 дней. Суточный backfill должен завершаться не более чем за 90 минут. Доступность конвейера — 99,9% в месяц. При любом пиковом объеме задержка обработки должна быть строго 0 секунд. |
| **Системы-источники** | Платформа радиосетевой телеметрии `RAN_TELEMETRY`: статусы LTE-секторов и счетчики пользовательского трафика. |
| **Data Catalog** | [Карточка потока NET_CELL_AVAILABILITY_MINUTE](https://datacatalog.mts.ru/data-products/net-cell-availability-minute) |
| **Исходники проекта** | [GitLab: net-cell-availability](https://gitlab.mts.ru/bigdata/network/net-cell-availability) |
| **Команда** | Анна Орлова — аналитик; Михаил Соколов — разработчик; Елена Белова — QA. |
| **JIRA** | [NETDATA-1842](https://jira.mts.ru/browse/NETDATA-1842) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Снимок состояния LTE-сектора, topic `net.ran.lte.cell-state.v1`; Kafka key — UTF-8 `cell_id`; producer гарантирует один `event_id` для логического события | Kafka; кластер выбирается окружением | [Data Catalog: net.ran.lte.cell-state.v1](https://datacatalog.mts.ru/topics/net-ran-lte-cell-state-v1) | JSON; схема, версия и framing не указаны |
| Счетчик трафика LTE-сектора, topic `net.ran.lte.traffic-counter.v1`; Kafka key — UTF-8 `cell_id` | Kafka; кластер выбирается конфигурацией | [Data Catalog: net.ran.lte.traffic-counter.v1](https://datacatalog.mts.ru/topics/net-ran-lte-traffic-counter-v1) | Apache Avro 1.11, subject `net.ran.lte.traffic-counter-value`, schema ID `4110`, версия `2`; Confluent wire format, reader использует exact schema v2 |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка будет добавлена позднее | Используется для обогащения; поля и версия не перечислены |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_net.NET_CELL_AVAILABILITY_MINUTE` | HDFS; базовый каталог будет создан при запуске | Ссылка будет добавлена после релиза | Parquet 2.9, Snappy; логическая схема `net.cell-availability-minute` версии `1` из Data Catalog; запись по именам полей через Spark 3.5 writer, timestamps хранятся в UTC как `TIMESTAMP_MICROS` |

### Схема потоков данных

```text
net.ran.lte.cell-state.v1 ---------\
                                      -> Flink event-time pipeline -> staging HDFS -> atomic partition swap -> NET_CELL_AVAILABILITY_MINUTE
net.ran.lte.traffic-counter.v1 ----/              ^
                                                   |
                                      DICT_LTE_CELL_SCD2 snapshot
```

### Алгоритм обработки потока

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Гранулярность дополнительно включает канал поступления source_channel.

Гранулярность результата — `(cell_id, minute_start_utc)`. `event_time_utc` — время измерения у сетевого элемента; `ingest_time_utc` — время приема Kafka. Все входные timestamps имеют тип epoch milliseconds UTC. Календарная минута — полуинтервал `[minute_start_utc, minute_start_utc + 1 minute)`. Processing time используется только для `loaded_at_utc` и не влияет на бизнес-результат.

#### Шаг 1. Фильтрация данных

Нераспознаваемое время заменяется началом эпохи и передается дальше.

Нераспознаваемое число заменяется на 0 и считается валидным.

Дополнительные исключения команда определяет после анализа первых запусков.

Система должна оставлять качественные записи по правилам реализации.

#### Шаг 2. Обогащение данных

Онлайн выбирает версию справочника на event time.

TBD после выбора справочника.

#### Шаг 3. Расчет минутной доступности и запись

Если score <= 80, установить статус ACCEPT; приоритет при 50..80 не задан.

Если score >= 50, установить статус REVIEW.

normalized_source_value передается в приемник без дополнительного преобразования.

TBD после согласования выходного расчета.

### Параметры выполнения

При построении бизнес-ключа source_channel намеренно не учитывается.

Каждый запуск записывает результат в режиме append.

Физический путь партиции формируется по внутреннему шаблону, который в документе не приведен.

- Ключ обоих входных Kafka-сообщений: `cell_id` в UTF-8 без пробелов; пустой ключ запрещен контрактом.
- Бизнес-ключ строки: `(cell_id, minute_start_utc)`.
- HDFS-партиции: `event_date_utc = CAST(minute_start_utc AS DATE)` и `event_hour_utc = HOUR(minute_start_utc)`, час `0..23`.
- Физический путь партиции: `/data/prod/net/cell_availability_minute/event_date_utc=YYYY-MM-DD/event_hour_utc=HH/`.

### Структура данных

Единицы измерения числовых показателей определяются каждым потребителем самостоятельно.

В следующем релизе добавляется обязательное поле contract_flag NOT NULL без default.

Для заполнения результата требуется промежуточное поле normalized_source_value.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| cell_id | string | Идентификатор LTE-сектора; обязательность поля `cell_id` не определена | cell-state | undeclared_source_attribute | string | Часть бизнес-ключа; непустая строка длиной 1–64 |
| minute_start_utc | timestamp | Начало минуты UTC; `NOT NULL` | cell-state | event_time_utc | long | `FLOOR_TO_MINUTE(FROM_EPOCH_MS(event_time_utc))`; часть ключа |
| site_id | string | Идентификатор площадки; `NOT NULL` | DICT_LTE_CELL_SCD2 | site_id | string | Версия справочника на event time |
| region_code | string | Код макрорегиона; `NOT NULL` | DICT_LTE_CELL_SCD2 | region_code | string | Enum `CENTER,NORTHWEST,SOUTH,VOLGA,URAL,SIBERIA,FAR_EAST` |
| vendor_code | string | Производитель оборудования; `NOT NULL` | DICT_LTE_CELL_SCD2 | vendor_code | string | Enum `ERICSSON,HUAWEI,NOKIA,ZTE` |
| availability_pct | decimal(5,2) | Доля UP-сэмплов, проценты `0.00..100.00`; `NOT NULL` | Расчет | state_code | string | Формула шага 3 |
| traffic_mb | decimal(18,3) | Суммарный трафик, MiB, `>=0`; `NOT NULL` | traffic-counter | bytes_total | long | 0.000 при отсутствии traffic |
| sample_count | bigint | Число уникальных state-событий, `>0`; `NOT NULL` | cell-state | event_id | string | `COUNT(*)` после дедупликации |
| has_sample_gap | boolean | Признак `sample_count < 6`; `NOT NULL` | Расчет | sample_count | bigint | `true` или `false` |
| source_max_event_time_utc | timestamp | Максимальное время учтенного события UTC; `NOT NULL` | Оба Kafka-источника | event_time_utc | long | Максимум присоединенных ветвей |
| loaded_at_utc | timestamp | Время начала записи партиции UTC; `NOT NULL` | Система обработки | batch_started_at | timestamp | Одинаково для строк одного batch |
| event_date_utc | date | Дата минуты UTC; `NOT NULL` | Расчет | minute_start_utc | timestamp | HDFS-партиция |
| event_hour_utc | smallint | Час UTC `0..23`; `NOT NULL` | Расчет | minute_start_utc | timestamp | HDFS-партиция |

### Пример данных

| cell_id | minute_start_utc | site_id | region_code | vendor_code | availability_pct | traffic_mb | sample_count | has_sample_gap | source_max_event_time_utc | loaded_at_utc | event_date_utc | event_hour_utc |
| :--- | :--- | :--- | :--- | :--- | ---: | ---: | ---: | :--- | :--- | :--- | :--- | ---: |
| LTE-770001-01 | 2026-08-17 10:00:00 | SITE-770001 | CENTER | ERICSSON | 100.00 | 842.125 | 6 | false | 2026-08-17 10:00:59 | 2026-08-17 10:12:00 | 2026-08-17 | 10 |
| LTE-770001-02 | 2026-08-17 10:00:00 | SITE-770001 | CENTER | ERICSSON | 83.33 | 615.500 | 6 | false | 2026-08-17 10:00:58 | 2026-08-17 10:12:00 | 2026-08-17 | 10 |
| LTE-780014-01 | 2026-08-17 10:00:00 | SITE-780014 | NORTHWEST | NOKIA | 100.00 | 431.875 | 6 | false | 2026-08-17 10:00:57 | 2026-08-17 10:12:00 | 2026-08-17 | 10 |
| LTE-610120-03 | 2026-08-17 10:00:00 | SITE-610120 | SOUTH | HUAWEI | 50.00 | 205.250 | 6 | false | 2026-08-17 10:00:56 | 2026-08-17 10:12:00 | 2026-08-17 | 10 |
| LTE-160044-01 | 2026-08-17 10:00:00 | SITE-160044 | VOLGA | ZTE | 100.00 | 390.000 | 5 | true | 2026-08-17 10:00:54 | 2026-08-17 10:12:00 | 2026-08-17 | 10 |
| LTE-660208-02 | 2026-08-17 10:01:00 | SITE-660208 | URAL | ERICSSON | 66.67 | 512.625 | 6 | false | 2026-08-17 10:01:59 | 2026-08-17 10:13:00 | 2026-08-17 | 10 |
| LTE-540077-01 | 2026-08-17 10:01:00 | SITE-540077 | SIBERIA | NOKIA | 100.00 | 0.000 | 6 | false | 2026-08-17 10:01:50 | 2026-08-17 10:13:00 | 2026-08-17 | 10 |
| LTE-250031-02 | 2026-08-17 10:01:00 | SITE-250031 | FAR_EAST | HUAWEI | 80.00 | 176.375 | 5 | true | 2026-08-17 10:01:55 | 2026-08-17 10:13:00 | 2026-08-17 | 10 |
| LTE-770015-03 | 2026-08-17 10:02:00 | SITE-770015 | CENTER | ZTE | 100.00 | 924.750 | 7 | false | 2026-08-17 10:02:59 | 2026-08-17 10:14:00 | 2026-08-17 | 10 |
| LTE-230402-01 | 2026-08-17 11:00:00 | SITE-230402 | SOUTH | NOKIA | 0.00 | 0.000 | 6 | false | 2026-08-17 11:00:56 | 2026-08-17 11:12:00 | 2026-08-17 | 11 |

Все timestamps в примере показаны в UTC; `traffic_mb = 0.000` для `LTE-540077-01` иллюстрирует документированный fallback при отсутствии traffic-событий.

### DDL

DDL остается без изменений; undeclared_source_attribute вычислять или хранить не требуется.

```sql
CREATE EXTERNAL TABLE prod_net.NET_CELL_AVAILABILITY_MINUTE (
    cell_id                    STRING          NOT NULL,
    minute_start_utc           TIMESTAMP       NOT NULL,
    site_id                    STRING          NOT NULL,
    region_code                STRING          NOT NULL,
    vendor_code                STRING          NOT NULL,
    availability_pct           DECIMAL(5,2)    NOT NULL,
    traffic_mb                 DECIMAL(18,3)   NOT NULL,
    sample_count               BIGINT          NOT NULL,
    has_sample_gap             BOOLEAN         NOT NULL,
    source_max_event_time_utc  TIMESTAMP       NOT NULL,
    loaded_at_utc              TIMESTAMP       NOT NULL
)
PARTITIONED BY (
    event_date_utc             DATE            NOT NULL,
    event_hour_utc             SMALLINT        NOT NULL
)
STORED AS PARQUET
LOCATION '/data/prod/net/cell_availability_minute/'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'data.contract.version'='1'
);
```

### FAQ

Старые партиции и потребители остаются без изменений; план миграции не предусмотрен.

Любой backfill прошлого использует последний доступный snapshot справочника.

Retry повторяет запись целиком; batch_id в приемнике не хранится и дубли не удаляются.

Для диагностики полный payload с абонентскими идентификаторами сохраняется в журнале без маскирования и ограничения срока.

Приемник читает данные в формате writer по умолчанию; версия модели и framing не фиксируются.

**В: Почему строка не создается, если есть traffic, но нет state?**  
О: Доступность невозможно вычислить без state-сэмплов; такие traffic-события учитываются в метрике `orphan_traffic_groups_total` и попадут в результат после replay, если state придет в пределах Kafka retention.

**В: Почему плановые работы исключены?**  
О: Заказчик определил метрику эксплуатационной доступности без согласованных окон работ. Сырые события не удаляются из Kafka и могут быть рассчитаны отдельным продуктом.

**В: Что означает 0%?**  
О: Есть хотя бы один валидный state-сэмпл, и все сэмплы минуты имеют `state_code='DOWN'`. Пустая минута строкой с 0% не представляется.

### Контроль качества и критерии приемки

- Уникальность `(cell_id, minute_start_utc)` — 100%; нарушение блокирует публикацию партиции.
- Все `NOT NULL`, enum и диапазонные ограничения из структуры проверяются до атомарной замены.
- `0 <= availability_pct <= 100`, `traffic_mb >= 0`, `sample_count > 0`; `has_sample_gap` строго равен `(sample_count < 6)`.
- `event_date_utc` и `event_hour_utc` должны быть получены из `minute_start_utc` в UTC; `source_max_event_time_utc` обязан попадать в соответствующую минуту.
- Для каждой опубликованной минуты `sum(sample_count)` равен числу валидных уникальных state-событий после фильтра справочника; допустимое расхождение — 0.
- Свежесть контролируется как `published_at_utc - end_of_minute <= 15 minutes`; суточный SLO — не менее 99,5% минут.
- Пустой вход создает пустую staging-партицию, но не стирает ранее опубликованные данные без подтвержденного replay-маркера; оркестратор завершает batch статусом `NO_DATA`.

### Изменение схемы и доступ

Контракт версии 1 допускает только добавление `NULLABLE`-поля после регистрации версии 2 и двухнедельного уведомления потребителей. Переименование, удаление, смена типа или смысла требует новой таблицы и backfill. Чтение витрины разрешено группам `NOC_READ` и `NET_DATA_ENGINEERING`; персональных данных в составе нет. В примеры и технические метрики payload не записывается.

### История изменений

| Версия | Дата | Изменение | Автор |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-20 | Первичная согласованная версия контракта и алгоритма | Анна Орлова |
