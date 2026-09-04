# Потоковые данные/витрины

| **Общие сведения** | Суточная витрина энергопотребления площадок мобильной сети `NET_SITE_ENERGY_DAY`. Одна строка описывает одну площадку за ее локальную календарную дату. |
| :--- | :--- |
| **Решаемая проблема** | Технический блок и финансы нуждаются в согласованном расчете потребленной энергии и оценочной стоимости по площадке. Витрина объединяет интервальные показания всех активных счетчиков, учитывает часовой пояс площадки и явно показывает неполноту. В скоуп входит электроэнергия сетевого оборудования; аренда, дизельное топливо, реактивная энергия и бухгалтерское закрытие счета вне скоупа. |
| **Продуктовые метрики** | 1) Предварительный итог доступен не позднее 3 часов после локального конца суток. 2) Финальный итог формируется через 48 часов. 3) Доля площадок с `completeness_pct=100.00` — не менее 97% в сутки. 4) Стоимость считается только при найденном тарифе и никогда не подменяется нулевой ставкой. Полнота результата должна быть достаточной для бизнеса. Свежесть данных должна оцениваться как приемлемая владельцем продукта. |
| **Заказчики** | Дирекция инфраструктуры сети; команда Energy Efficiency; управленческий учет. |
| **Нефункциональные требования** | До 8 млн интервалов в час, пик 8 000 событий/с. Интервал измерения — 15 минут. Kafka retention — 10 суток, HDFS retention — 7 лет. Event timestamps — UTC, бизнес-дата — локальная дата площадки по IANA TZDB `2026a`. Онлайн-watermark — 2 часа; плановый replay — 10 суток. Пересчет месяца — до 6 часов. Единственный ключ партиционирования — country_code, хотя основная часть событий приходится на одно значение этого поля. |
| **Системы-источники** | `ENERGY_METER_GATEWAY` — нормализованные интервальные показания коммерческих счетчиков площадок RAN. |
| **Data Catalog** | [Карточка NET_SITE_ENERGY_DAY](https://datacatalog.mts.ru/data-products/net-site-energy-day) |
| **Исходники проекта** | [GitLab: site-energy-day](https://gitlab.mts.ru/bigdata/infrastructure/site-energy-day) |
| **Команда** | Виктория Морозова — аналитик; Артем Егоров — разработчик; Наталья Виноградова — QA. |
| **JIRA** | [ENERGYDATA-566](https://jira.mts.ru/browse/ENERGYDATA-566) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| 15-минутное активное энергопотребление, topic `net.energy.meter-interval.v1`; Kafka key — `meter_id`; поддерживаются `UPSERT` и `DELETE` | Kafka, кластер `kafka-iot-prod-02` | [Data Catalog: net.energy.meter-interval.v1](https://datacatalog.mts.ru/topics/net-energy-meter-interval-v1) | JSON UTF-8; JSON Schema draft 2020-12, `$id=net.energy.meter-interval`, версия `3` по [схеме](https://schema-registry.mts.ru/json/net.energy.meter-interval/3); producer передает `schema_version=3`, consumer выполняет exact validation v3 |
| Архив тех же change records `raw_energy.METER_INTERVAL_HISTORY`, полный путь `/data/raw/energy/meter_interval_history/`; хранение 7 лет | HDFS/Iceberg, кластер `hdfs-prod-04` | [Data Catalog: METER_INTERVAL_HISTORY](https://datacatalog.mts.ru/tables/raw-energy-meter-interval-history) | Iceberg v2, Parquet ZSTD; схема `energy.meter-interval-history` версии `3`; backfill фиксирует Iceberg snapshot ID и читает те же поля и семантику revision, что Kafka-контракт v3 |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Корпоративный справочник | Ссылка отсутствует | Используется актуальная версия с необходимыми полями |
| `DICT_ELECTRICITY_TARIFF_DAY` | [Data Catalog: DICT_ELECTRICITY_TARIFF_DAY](https://datacatalog.mts.ru/tables/ref-dict-electricity-tariff-day) | Уникальный ключ `(tariff_zone, business_date_local)`, версия набора `2026.08`; поле `rate_rub_per_kwh DECIMAL(10,4) > 0`. Тариф действует на локальные сутки целиком. Другие справочники не используются. |

### Приемники данных

Одна строка приемника является суточным агрегатом объекта.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_energy.NET_SITE_ENERGY_DAY` | HDFS; кластер и базовый путь не указаны | [Data Catalog: NET_SITE_ENERGY_DAY](https://datacatalog.mts.ru/tables/prod-energy-net-site-energy-day) | Parquet 2.9, ZSTD level 3; логическая схема `energy.site-energy-day` версии `1`; Spark writer по именам полей, decimals без float-конвертации |

### Схема потоков данных

```text
Meters -> ENERGY_METER_GATEWAY -> Kafka kafka-iot-prod-02 -> validate/revision dedup
                                                                  |-> DICT_SITE_ENERGY_METER_SCD2 (base + lookup)
                                                                  |-> DICT_ELECTRICITY_TARIFF_DAY
                                                                  -> local-day aggregation -> HDFS NET_SITE_ENERGY_DAY
```

### Алгоритм обработки потока

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Каждое прошедшее фильтр событие записывается отдельной строкой без агрегации.

Одна UPSERT-запись содержит энергию в Wh за полуинтервал `[interval_start_utc, interval_end_utc)`. Оба timestamps — epoch milliseconds UTC. Локальная бизнес-дата определяется как `DATE(interval_start_utc AT TIME ZONE timezone_name)`. Поддерживаемые зоны: `Europe/Kaliningrad`, `Europe/Moscow`, `Europe/Samara`, `Asia/Yekaterinburg`, `Asia/Omsk`, `Asia/Novosibirsk`, `Asia/Krasnoyarsk`, `Asia/Irkutsk`, `Asia/Vladivostok`. Поддерживаемый диапазон бизнес-дат — `2019-01-01..2030-12-31`; по TZDB 2026a каждые такие сутки в перечисленных зонах имеют 24 часа.

#### Шаг 1. Фильтрация данных

Online-расчет и replay используют разные наборы фильтров из кода приложения; конкретные условия, границы и исключения в документации не перечислены.

#### Шаг 2. Обогащение данных

Все найденные версии передаются в результат без выбора актуальной записи и проверки кардинальности.

Справочник содержит версии записей по идентификатору и effective_date, но JOIN выполняется только по идентификатору.

Онлайн-обработка выбирает версию справочника, действовавшую на event time.

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. Формирование суточной строки

1. Базовый набор строится из всех активных meter-версий справочника на локальные сутки, поэтому площадка создается даже при полном отсутствии телеметрии. Благодаря ограничению смены в полночь каждый активный счетчик ожидается все сутки.
2. `active_meter_count = COUNT(DISTINCT meter_id)` базы; `expected_interval_count = 96 * active_meter_count`; `valid_interval_count` — число выбранных валидных UPSERT. Для отсутствующего интервала contribution равен нулю только в сумме энергии, но неполнота остается видна в счетчиках.
3. `energy_kwh = ROUND(SUM(energy_wh) / 1000, 3)`, при полном отсутствии интервалов — `0.000`. `completeness_pct = ROUND(100.00 * valid_interval_count / expected_interval_count, 2)`. Поскольку active meter count больше нуля, нулевого знаменателя нет. `has_interval_gap = valid_interval_count < expected_interval_count`.
4. При тарифе `cost_rub = ROUND(energy_kwh * tariff_rate_rub_per_kwh, 2)`; промежуточное умножение выполняется DECIMAL(28,7), округление HALF_UP. При отсутствии тарифа стоимость остается NULL, zero fallback запрещен.
5. `source_max_interval_end_utc` — максимум конца валидного интервала; при полном отсутствии — NULL. Предварительная версия публикуется через 3 часа после локального конца дня, финальная — через 48 часов. Ежедневно replay полностью перезаписывает последние 10 бизнес-дат.
6. Партиция пишется в staging и атомарно заменяется после контроля. Retry с тем же `batch_id` выполняет overwrite. При частичном сбое опубликованная партиция остается прежней. DELETE и исправленные revision отражаются при replay. Backfill читает архив и принимает явные `date_from`, `date_to`, `raw_snapshot_id`, `meter_snapshot_id`, `tariff_dataset_version`; все три версии сохраняются в metadata запуска, повтор с ними безопасен.

### Формирование ключа (kafka) / партиции (hdfs)

При повторном запуске партиция может дополняться или перезаписываться по выбору оператора.

Дополнительный salt или составной ключ для устранения перекоса нагрузки не предусмотрен.

Партиция создается в HDFS по внутренней конфигурации; полный физический путь и шаблон каталогов в документе не указаны.

### Структура данных

В следующем релизе добавляется обязательное поле contract_flag NOT NULL без default.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| site_id | string | Идентификатор площадки; `NOT NULL` | DICT_SITE_ENERGY_METER_SCD2 | site_id | string | Часть бизнес-ключа |
| timezone_name | string | IANA timezone из разрешенного списка; `NOT NULL` | DICT_SITE_ENERGY_METER_SCD2 | timezone_name | string | TZDB 2026a |
| region_code | string | Макрорегион; `NOT NULL` | DICT_SITE_ENERGY_METER_SCD2 | region_code | string | Enum регионов MTS |
| tariff_zone | string | Код тарифной зоны; `NOT NULL` | DICT_SITE_ENERGY_METER_SCD2 | tariff_zone | string | Ключ тарифа |
| active_meter_count | int | Активные счетчики площадки, `>0`; `NOT NULL` | Справочник/расчет | meter_id,is_active | string,boolean | Distinct meters базы |
| expected_interval_count | int | `96 * active_meter_count`; `NOT NULL` | Расчет | active_meter_count | int | `>0` |
| valid_interval_count | int | Валидные интервалы, `0..expected`; `NOT NULL` | Kafka | interval_start_utc | long | После dedup и фильтров |
| energy_kwh | decimal(18,3) | Активная энергия, кВт·ч, `>=0`; `NOT NULL` | Kafka/расчет | energy_wh | long | Сумма / 1000 |
| completeness_pct | decimal(5,2) | Полнота, проценты `0.00..100.00`; `NOT NULL` | Расчет | valid,expected counts | int | HALF_UP |
| has_interval_gap | boolean | Признак `valid < expected`; `NOT NULL` | Расчет | valid,expected counts | int | Детерминированный флаг |
| tariff_rate_rub_per_kwh | decimal(10,4) | Тариф, руб./кВт·ч; `NULLABLE` | DICT_ELECTRICITY_TARIFF_DAY | rate_rub_per_kwh | decimal(10,4) | NULL только при MISSING |
| cost_rub | decimal(20,2) | Оценочная стоимость, руб.; `NULLABLE` | Расчет | energy_kwh, rate | decimal | NULL только при MISSING |
| tariff_status | string | `COMPLETE` или `MISSING`; `NOT NULL` | Расчет | результат tariff join | string | Явный статус fallback |
| source_max_interval_end_utc | timestamp | Максимальный конец валидного интервала UTC; `NULLABLE` | Kafka | interval_end_utc | long | NULL при нуле интервалов |
| loaded_at_utc | timestamp | Начало batch UTC; `NOT NULL` | Система | batch_started_at | timestamp | Processing time |
| business_date_local | date | Локальная дата площадки; `NOT NULL` | Расчет | interval_start_utc, timezone_name | long,string | Часть ключа и HDFS-партиция |

### Пример данных

| site_id | timezone_name | region_code | tariff_zone | active_meter_count | expected_interval_count | valid_interval_count | energy_kwh | completeness_pct | has_interval_gap | tariff_rate_rub_per_kwh | cost_rub | tariff_status | source_max_interval_end_utc | loaded_at_utc | business_date_local |
| :--- | :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- | ---: | ---: | :--- | :--- | :--- |
| SITE-MSK-0001 | Europe/Moscow | CENTER | MOSENERGO-A | 2 | 192 | 192 | 1250.500 | 100.00 | false | 6.4200 | 8028.21 | COMPLETE | 2026-08-20 21:00:00 | 2026-08-20 23:30:00 | 2026-08-20 |
| SITE-SPB-0014 | Europe/Moscow | NORTHWEST | LENENERGO-B | 1 | 96 | 96 | 980.000 | 100.00 | false | 6.1800 | 6056.40 | COMPLETE | 2026-08-20 21:00:00 | 2026-08-20 23:30:00 | 2026-08-20 |
| SITE-SMR-0021 | Europe/Samara | VOLGA | SAMARA-A | 3 | 288 | 280 | 730.250 | 97.22 | true | 6.0100 | 4388.80 | COMPLETE | 2026-08-20 20:00:00 | 2026-08-20 22:30:00 | 2026-08-20 |
| SITE-EKB-0030 | Asia/Yekaterinburg | URAL | URAL-A | 2 | 192 | 190 | 1500.000 | 98.96 | true | 5.9500 | 8925.00 | COMPLETE | 2026-08-20 19:00:00 | 2026-08-20 21:30:00 | 2026-08-20 |
| SITE-OMS-0008 | Asia/Omsk | SIBERIA | OMSK-A | 1 | 96 | 80 | 420.500 | 83.33 | true | 6.2700 | 2636.54 | COMPLETE | 2026-08-20 18:00:00 | 2026-08-20 20:30:00 | 2026-08-20 |
| SITE-NSK-0042 | Asia/Novosibirsk | SIBERIA | SIBERIA-A | 4 | 384 | 384 | 2100.750 | 100.00 | false | 5.8300 | 12247.37 | COMPLETE | 2026-08-20 17:00:00 | 2026-08-20 19:30:00 | 2026-08-20 |
| SITE-VVO-0012 | Asia/Vladivostok | FAR_EAST | PRIMORYE-A | 2 | 192 | 192 | 660.000 | 100.00 | false | 5.7100 | 3768.60 | COMPLETE | 2026-08-20 14:00:00 | 2026-08-20 16:30:00 | 2026-08-20 |
| SITE-KRS-0028 | Asia/Krasnoyarsk | SIBERIA | KRASNOYARSK-A | 3 | 288 | 287 | 1800.125 | 99.65 | true | 6.4200 | 11556.80 | COMPLETE | 2026-08-20 17:00:00 | 2026-08-20 19:30:00 | 2026-08-20 |
| SITE-KGD-0003 | Europe/Kaliningrad | NORTHWEST | KALININGRAD-A | 1 | 96 | 96 | 300.000 | 100.00 | false | 6.1800 | 1854.00 | COMPLETE | 2026-08-20 22:00:00 | 2026-08-21 00:30:00 | 2026-08-20 |
| SITE-IRK-0017 | Asia/Irkutsk | SIBERIA | IRKUTSK-NEW | 2 | 192 | 192 | 875.250 | 100.00 | false | NULL | NULL | MISSING | 2026-08-20 16:00:00 | 2026-08-20 18:30:00 | 2026-08-20 |

UTC-время конца последнего интервала соответствует следующей локальной полуночи каждой зоны. Последняя строка показывает допустимое отсутствие тарифа без потери энергии.

### DDL

```sql
CREATE EXTERNAL TABLE prod_energy.NET_SITE_ENERGY_DAY (
    site_id                         BIGINT          NOT NULL,
    timezone_name                   STRING          NOT NULL,
    region_code                     STRING          NOT NULL,
    tariff_zone                     STRING          NOT NULL,
    active_meter_count              INT             NOT NULL,
    expected_interval_count         INT             NOT NULL,
    valid_interval_count            INT             NOT NULL,
    energy_kwh                      DECIMAL(18,3)   NOT NULL,
    completeness_pct                DECIMAL(5,2)    NOT NULL,
    has_interval_gap                BOOLEAN         NOT NULL,
    tariff_rate_rub_per_kwh         DECIMAL(10,4),
    cost_rub                        DECIMAL(20,2),
    tariff_status                   STRING          NOT NULL,
    source_max_interval_end_utc     TIMESTAMP,
    loaded_at_utc                   TIMESTAMP       NOT NULL
)
PARTITIONED BY (
    business_date_local             DATE            NOT NULL
)
STORED AS PARQUET
LOCATION '/data/prod/energy/site_energy_day/'
TBLPROPERTIES (
    'parquet.compression'='ZSTD',
    'parquet.compression.level'='3',
    'data.contract.version'='1'
);
```

### FAQ

Debug-таблица ежедневно копируется в бессрочный backup; правила доступа и удаления отсутствуют.

При ошибке полный исходный payload сохраняется в общей debug-таблице.

Старые партиции и потребители остаются без изменений; план миграции не предусмотрен.

При backfill любого прошлого периода используется последний snapshot справочника на дату запуска.

**В: Почему бизнес-дата не UTC?**  
О: Тариф и операционный день определены локальной датой площадки. UTC остается единственной шкалой входных событий, а IANA timezone делает преобразование воспроизводимым.

**В: Означает ли `energy_kwh=0`, что телеметрия полна?**  
О: Нет. Для этого существуют `valid_interval_count`, `completeness_pct` и `has_interval_gap`. Ноль с 96 интервалами — валидное отсутствие расхода; ноль с 0 интервалов — полная потеря телеметрии.

**В: Можно ли использовать `cost_rub` для бухгалтерского счета?**  
О: Нет. Это управленческая оценка по суточному тарифу без аренды, реактивной энергии и корректировок поставщика.

### Контроль качества и критерии приемки

- `(site_id,business_date_local)` уникален; все required-поля и допустимые timezone проверяются.
- `active_meter_count > 0`; `expected_interval_count = 96 * active_meter_count`; `0 <= valid_interval_count <= expected_interval_count`.
- `completeness_pct` точно повторяет формулу; `has_interval_gap = (valid < expected)`; энергия неотрицательна.
- При `tariff_status='COMPLETE'` ставка положительна и cost точно соответствует формуле. При `MISSING` оба поля обязаны быть NULL.
- Число valid intervals сверяется с выбранными логическими ключами после DELETE/dedup/filter; допустимое расхождение — 0.
- Каждый интервал полностью лежит в своей локальной дате; max end не позже следующей локальной полуночи.
- Даже при пустом входе создаются строки активных площадок с `valid_interval_count=0`, `energy_kwh=0.000`, `has_interval_gap=true`; поэтому отсутствие потока наблюдаемо.

### Изменение схемы и доступ

Добавление nullable-поля допускается в minor-версии. Смена TZDB, формулы стоимости, границ бизнес-дня, ключа или единицы энергии требует major-версии, impact assessment и пересчета истории. Результат не содержит персональных данных; meter ID в выход не публикуется. Доступ: `ENERGY_EFFICIENCY_READ`, `NETWORK_FINANCE_READ`, `ENERGY_DATA_ENGINEERING`.

### История изменений

| Версия | Дата | Изменение | Автор |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-24 | Первичная версия с локальными сутками, явной полнотой и nullable-тарифом | Виктория Морозова |
