# Потоковые данные/витрины

| **Общие сведения** | Ежемесячная витрина установленной базы устройств: число активных абонентов по домашнему региону, вендору, операционной системе и поддержке 5G. |
| :--- | :--- |
| **Решаемая проблема** | Команда развития 5G получает воспроизводимую оценку device base без двойного счёта абонента с несколькими устройствами. В скоупе — розничные абоненты с хотя бы одним валидным сетевым событием в месяце; M2M, сотрудники, тестовые SIM и абоненты без валидного IMEI не входят. |
| **Продуктовые метрики** | Доля 5G-capable устройств и распределение вендоров по регионам; 100% вошедших subscriber token представлены ровно в одной группе; отклонение контрольного числа абонентов от детального набора — 0; предварительная публикация M+3, финальная M+16 до 09:00 MSK. |
| **Заказчики** | Дирекция 5G, коммерческая аналитика, MTS Big Data. |
| **Нефункциональные требования** | 9–14 млрд событий в месяц; историческая загрузка с 2024-01-01; хранение агрегатов бессрочно; расчёт в UTC, публикация по расписанию Europe/Moscow; один месяц пересчитывается целиком; RTO 8 часов, одновременная публикация двух версий месяца запрещена. При любом пиковом объеме задержка обработки должна быть строго 0 секунд. |
| **Системы-источники** | `NETWORK_EVENT_DDS` — нормализованные события активности с pseudonymous subscriber token и IMEI. При расхождении значений между источниками допускается использовать значение любого из них. |
| **Data Catalog** | [Продукт Device Base Monthly](https://datacatalog.corp.mts.ru/products/device-base-monthly) |
| **Исходники проекта** | [GitLab: commercial/device-base-monthly](https://gitlab.corp.mts.ru/bigdata/commercial/device-base-monthly) |
| **Команда** | Светлана Осина — аналитик; Михаил Рябов — разработчик; Галина Юдина — QA; Андрей Тарасов — Product Owner. |
| **JIRA** | [DEVBASE-3156 — Ежемесячная база устройств](https://jira.corp.mts.ru/browse/DEVBASE-3156) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `DDS_NET.TABLE_SUBSCRIBER_DEVICE_EVENT`, полный путь `/warehouse/dds/net/subscriber_device_event/` | HDFS/Iceberg, кластер `hadoop-dwh-prod-01` | Data Catalog: ссылка отсутствует | JSON; схема и версия не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Неуказанный справочник | Ссылка на справочник отсутствует | Используется для обогащения; поля и версия не перечислены |
| `DDS_CRM.TABLE_SUBSCRIBER_PROFILE_SCD` | [Data Catalog: TABLE_SUBSCRIBER_PROFILE_SCD](https://datacatalog.corp.mts.ru/tables/DDS_CRM/TABLE_SUBSCRIBER_PROFILE_SCD) | Iceberg v2/Parquet `ZSTD`, кластер `hadoop-dwh-prod-01`, путь `/warehouse/dds/crm/subscriber_profile_scd/`, schema 8.3; Spark фиксирует snapshot ID. SCD2-профиль token: `home_region_code`, `segment_code`, `is_employee`, `is_test`; интервал `[valid_from_utc, valid_to_utc)`. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_COMM.TABLE_DEVICE_BASE_MONTHLY` | Greenplum, кластер `gp-analytics-prod-02`, БД `dwh`, schema `cdm_comm` | [Data Catalog: TABLE_DEVICE_BASE_MONTHLY](https://datacatalog.corp.mts.ru/tables/CDM_COMM/TABLE_DEVICE_BASE_MONTHLY) | Реляционная модель версии 1.0; загрузка Spark Greenplum Connector 2.1 через GPFDIST во временную таблицу в PostgreSQL binary row format, затем транзакционный `ALTER TABLE EXCHANGE PARTITION`; чтение — PostgreSQL protocol по DDL-модели. |

### Схема потоков данных

Iceberg `TABLE_SUBSCRIBER_DEVICE_EVENT` → месячная фильтрация и дедупликация → выбор последнего устройства subscriber token → temporal JOIN профиля и TAC → агрегация → GPFDIST staging → атомарный exchange месячной партиции `TABLE_DEVICE_BASE_MONTHLY`.

Одна строка результата относится к `(FIELD_MONTH, FIELD_HOME_REGION_CODE, FIELD_DEVICE_VENDOR, FIELD_OS_FAMILY, FIELD_5G_CAPABILITY)`. Этот набор является полным уникальным бизнес-ключом.

### Алгоритм обработки потока

Фильтрацию и обогащение можно выполнять в любом порядке по усмотрению реализации.

Во всех расчетах используются только активные записи источников.

#### Шаг 1. Фильтрация данных

Дополнительно учитываются события только за последние 7 дней.

В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.

#### Шаг 2. Обогащение данных

Описание этого этапа будет согласовано после начала разработки.

#### Шаг 3. <Наименование шага 3>

- `FIELD_MONTH` — первое число месяца M (`DATE`) в UTC.
- Нормализовать `vendor_name` и `os_family` только функцией `trim`; регистр и spelling задаёт справочник. Пустая после trim строка является дефектом справочника.
- Преобразовать boolean `is_5g_capable`: `true → 'YES'`, `false → 'NO'`; fallback указан выше.
- Сгруппировать по полному бизнес-ключу и вычислить `FIELD_SUBSCRIBERS_CNT = COUNT(*)`. После выбора одной строки на token это равно `COUNT(DISTINCT subscriber_token)`; обе величины сверяются перед публикацией.
- `FIELD_PROC_TS` — UTC timestamp начала успешной транзакции публикации; одинаков для всего месяца.

#### Шаг 4. Регламент, исправления и запись

Предварительный расчёт запускается третьего числа M+1 в 03:00 Europe/Moscow по одному зафиксированному Iceberg snapshot. Финальный — шестнадцатого числа в 03:00 Europe/Moscow. Событие с `ingest_ts` ровно до snapshot входит, после snapshot — нет. Источник хранит 400 дней.

Поздние события и исправления до финального запуска включаются полной заменой M. Изменение после финализации создаёт `post_final_change_cnt`; автоматический backfill разрешён для последних 12 месяцев и всегда перечитывает месяц целиком по новому snapshot. Более глубокий backfill требует заявки владельца.

Перед staging процедура `CDM_COMM.ensure_device_base_partition(M)` под advisory lock создаёт отдельную месячную партицию `[M, M+1)`, если её ещё нет; M обязан быть не раньше `2024-01-01` и не позже текущего UTC-месяца. Затем данные записываются в `CDM_COMM.TABLE_DEVICE_BASE_MONTHLY_STG_<run_id>`, проходят проверки, и в одной Greenplum-транзакции выполняется exchange партиции M. При ошибке транзакция откатывается, опубликованная версия остаётся прежней. Retry использует новый staging и полностью заменяет месяц, поэтому идемпотентен. Staging удаляется только после успешного exchange или через 7 дней сервисной очисткой.

### Формирование ключа (kafka) / партиции (hdfs)

- Kafka не используется: Kafka-ключ — `не применимо`.
- HDFS-приёмник отсутствует: HDFS-партиция — `не применимо`. Источник физически партиционирован `months(event_date)`; это используется только для pruning.
- Greenplum: range partition по `FIELD_MONTH`, одна партиция на календарный месяц; `DISTRIBUTED BY (FIELD_HOME_REGION_CODE, FIELD_DEVICE_VENDOR)`.

### Структура данных

Единицы измерения числовых показателей определяются каждым потребителем самостоятельно.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_MONTH | DATE | Первое число месяца UTC; `NOT NULL` | Параметр DAG | `month_start` | DATE | Всегда day=1 |
| FIELD_HOME_REGION_CODE | VARCHAR(16) | Домашний регион или `UNKNOWN`; `NOT NULL` | `TABLE_SUBSCRIBER_PROFILE_SCD` | `home_region_code` | STRING | Temporal JOIN на последнее событие |
| FIELD_DEVICE_VENDOR | VARCHAR(128) | Вендор либо `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `vendor_name` | STRING | `trim`, fallback |
| FIELD_OS_FAMILY | VARCHAR(64) | Семейство ОС либо `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `os_family` | STRING | `trim`, fallback |
| FIELD_5G_CAPABILITY | VARCHAR(7) | `YES`, `NO`, `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `is_5g_capable` | BOOLEAN | Явный mapping |
| FIELD_SUBSCRIBERS_CNT | BIGINT | Число уникальных активных tokens, >0; `NOT NULL` | `TABLE_SUBSCRIBER_DEVICE_EVENT` | `subscriber_token` | STRING | После выбора последнего устройства |
| FIELD_PROC_TS | TIMESTAMP | Время публикации UTC; `NOT NULL` | Airflow | `run_start_ts` | TIMESTAMP | Одинаково для партиции M |

### Пример данных

| FIELD_MONTH | FIELD_HOME_REGION_CODE | FIELD_DEVICE_VENDOR | FIELD_OS_FAMILY | FIELD_5G_CAPABILITY | FIELD_SUBSCRIBERS_CNT | FIELD_PROC_TS |
| :--- | :--- | :--- | :--- | :--- | ---: | :--- |
| 2026-07-01 | MOS | Apple | iOS | YES | 2845100 | 2026-08-16 00:05:44 |
| 2026-07-01 | MOS | Apple | iOS | NO | 711200 | 2026-08-16 00:05:44 |
| 2026-07-01 | MOS | Samsung | Android | YES | 2110940 | 2026-08-16 00:05:44 |
| 2026-07-01 | SPE | Xiaomi | Android | YES | 804100 | 2026-08-16 00:05:44 |
| 2026-07-01 | KZN | Samsung | Android | NO | 315880 | 2026-08-16 00:05:44 |
| 2026-07-01 | EKB | Huawei | HarmonyOS | NO | 197310 | 2026-08-16 00:05:44 |
| 2026-07-01 | NSK | Realme | Android | YES | 120450 | 2026-08-16 00:05:44 |
| 2026-07-01 | SAM | Nokia | Android | NO | 40120 | 2026-08-16 00:05:44 |
| 2026-07-01 | UFA | UNKNOWN | UNKNOWN | UNKNOWN | 1830 | 2026-08-16 00:05:44 |
| 2026-07-01 | UNKNOWN | ZTE | Android | YES | 615 | 2026-08-16 00:05:44 |

### DDL

```sql
CREATE TABLE CDM_COMM.TABLE_DEVICE_BASE_MONTHLY (
    FIELD_MONTH DATE NOT NULL,
    FIELD_HOME_REGION_CODE VARCHAR(16) NOT NULL,
    FIELD_DEVICE_VENDOR VARCHAR(128) NOT NULL,
    FIELD_OS_FAMILY VARCHAR(64) NOT NULL,
    FIELD_5G_CAPABILITY VARCHAR(7) NOT NULL,
    FIELD_SUBSCRIBERS_CNT BIGINT NOT NULL,
    FIELD_PROC_TS TIMESTAMP NOT NULL
)
WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=5)
DISTRIBUTED BY (FIELD_HOME_REGION_CODE, FIELD_DEVICE_VENDOR)
PARTITION BY RANGE (FIELD_MONTH)
(START (DATE '2024-01-01') INCLUSIVE END (DATE '2031-01-01') EXCLUSIVE EVERY (INTERVAL '1 month'));
```

До exchange проверяются: уникальность ключа; `EXTRACT(DAY FROM FIELD_MONTH)=1`; enum capability; положительный count; отсутствие NULL; сумма `FIELD_SUBSCRIBERS_CNT` равна числу допущенных уникальных tokens; ни один token не относится к двум группам. Выход не содержит token или IMEI.

### FAQ

Тип существующего поля можно изменить без выпуска новой версии, если его имя сохраняется.

**Какое устройство считается устройством месяца?** Устройство из последнего валидного сетевого события абонента в UTC-месяце; tie-break полностью определён в шаге 2.

**Что означает `UNKNOWN` для 5G?** TAC отсутствует в историческом срезе справочника. Это не означает `NO` и не включается в знаменатель продуктовой доли 5G; доля рассчитывается как `YES / (YES + NO)`.

**Почему сотрудник исключён?** Это явная граница коммерческой установленной базы; M2M, employee и test анализируются в отдельных продуктах.

**Как защищаются данные?** Исходные token и IMEI доступны только service account `device-base-etl`; они не попадают в staging-логи и результат. Агрегат разрешён ролям `commercial_device_read` и `network_strategy_read`. Группы с малым count не раскрывают PII, но выгрузка наружу контура запрещена политикой продукта.

**Что произойдёт при изменении TAC?** Обычный расчёт использует историческую версию на последнее событие. Исправление прошлого требует полного backfill месяца. Изменение ключа, формулы или enum публикуется как major-версия отдельной таблицы; добавление nullable-атрибута — как minor после уведомления потребителей.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-18 | Светлана Осина | Первичная версия с выбором последнего устройства, SCD2-срезами и атомарным exchange. |
