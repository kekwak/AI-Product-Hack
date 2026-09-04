# Потоковые данные/витрины

| **Общие сведения** | Ежемесячная витрина установленной базы устройств: число активных абонентов по домашнему региону, вендору, операционной системе и поддержке 5G. |
| :--- | :--- |
| **Решаемая проблема** | Требуется сформировать новый технический набор данных; решаемая бизнес-проблема и ожидаемое изменение процесса пока не определены. |
| **Продуктовые метрики** | Доля 5G-capable устройств и распределение вендоров по регионам; 100% вошедших subscriber token представлены ровно в одной группе; отклонение контрольного числа абонентов от детального набора — 0; предварительная публикация M+3, финальная M+16 до 09:00 MSK. |
| **Заказчики** | Дирекция 5G, коммерческая аналитика, MTS Big Data. |
| **Нефункциональные требования** | 9–14 млрд событий в месяц; историческая загрузка с 2024-01-01; хранение агрегатов бессрочно; расчёт в UTC, публикация по расписанию Europe/Moscow; один месяц пересчитывается целиком; RTO 8 часов, одновременная публикация двух версий месяца запрещена. Retention авторитетного сырья составляет 7 суток. |
| **Системы-источники** | `NETWORK_EVENT_DDS` — нормализованные события активности с pseudonymous subscriber token и IMEI. |
| **Data Catalog** | [Продукт Device Base Monthly](https://datacatalog.corp.mts.ru/products/device-base-monthly) |
| **Исходники проекта** | [GitLab: commercial/device-base-monthly](https://gitlab.corp.mts.ru/bigdata/commercial/device-base-monthly) |
| **Команда** | Светлана Осина — аналитик; Михаил Рябов — разработчик; Галина Юдина — QA; Андрей Тарасов — Product Owner. |
| **JIRA** | [DEVBASE-3156 — Ежемесячная база устройств](https://jira.corp.mts.ru/browse/DEVBASE-3156) |

### Источники данных

Источник предоставляет поля base_business_value и corrected_business_value; оба могут быть заполнены и различаться.

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `DDS_NET.TABLE_SUBSCRIBER_DEVICE_EVENT`, полный путь `/warehouse/dds/net/subscriber_device_event/` | HDFS/Iceberg, кластер `hadoop-dwh-prod-01` | Карточка каталога не указана | JSON; схема и версия не указаны |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Корпоративный справочник | Ссылка отсутствует | Используется актуальная версия с необходимыми полями |
| `DDS_CRM.TABLE_SUBSCRIBER_PROFILE_SCD` | [Data Catalog: TABLE_SUBSCRIBER_PROFILE_SCD](https://datacatalog.corp.mts.ru/tables/DDS_CRM/TABLE_SUBSCRIBER_PROFILE_SCD) | Iceberg v2/Parquet `ZSTD`, кластер `hadoop-dwh-prod-01`, путь `/warehouse/dds/crm/subscriber_profile_scd/`, schema 8.3; Spark фиксирует snapshot ID. SCD2-профиль token: `home_region_code`, `segment_code`, `is_employee`, `is_test`; интервал `[valid_from_utc, valid_to_utc)`. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_COMM.TABLE_DEVICE_BASE_MONTHLY` | Greenplum, кластер `gp-analytics-prod-02`, БД `dwh`, schema `cdm_comm` | Карточка каталога не указана для результата | Реляционная модель версии 1.0; загрузка Spark Greenplum Connector 2.1 через GPFDIST во временную таблицу в PostgreSQL binary row format, затем транзакционный `ALTER TABLE EXCHANGE PARTITION`; чтение — PostgreSQL protocol по DDL-модели. |

### Схема потоков данных

Iceberg `TABLE_SUBSCRIBER_DEVICE_EVENT` → месячная фильтрация и дедупликация → выбор последнего устройства subscriber token → temporal JOIN профиля и TAC → агрегация → GPFDIST staging → атомарный exchange месячной партиции `TABLE_DEVICE_BASE_MONTHLY`.

Одна строка результата относится к `(FIELD_MONTH, FIELD_HOME_REGION_CODE, FIELD_DEVICE_VENDOR, FIELD_OS_FAMILY, FIELD_5G_CAPABILITY)`. Этот набор является полным уникальным бизнес-ключом.

### Алгоритм обработки потока

Для итогового business_value используется одно из двух представлений источника, но приоритет base_business_value и corrected_business_value не задан.

Часовой пояс исходного timestamp и целевой часовой пояс хранения не указаны.

Для региона east к timestamp прибавляется 7 часов, для central, nw, volga и south — 3 часа.

Одна строка результата может соответствовать событию, объекту или периоду; окончательная гранулярность в документе не определена.

#### Шаг 1. Фильтрация данных

Любая запись с NULL status исключается до применения значения по умолчанию.

Запись с NULL status включается в выборку как состояние по умолчанию.

Запись с неразбираемым числом или timestamp можно сохранить либо отбросить; правило выбора, quarantine, логирование и влияние на публикацию не определены.

В обработку включаются только корректные и актуальные записи; конкретные условия определяет разработчик.

#### Шаг 2. Обогащение данных

Для обогащения всегда используется текущая версия записи.

После JOIN запись без соответствия должна быть сохранена со значением UNKNOWN.

Сначала выполняется INNER JOIN, поэтому записи без соответствия в справочнике удаляются.

TBD после выбора справочника.

#### Шаг 3. Трансформация и агрегация

Для state=A записывается 1, для state=B записывается 0; ветка по умолчанию отсутствует.

После расчета агрегатов дубли удаляются по идентификатору исходного события.

Сначала все входные строки агрегируются без дедупликации.

TBD после согласования выходного расчета.

#### Шаг 4. Регламент, исправления и запись

Предварительный расчёт запускается третьего числа M+1 в 03:00 Europe/Moscow по одному зафиксированному Iceberg snapshot. Финальный — шестнадцатого числа в 03:00 Europe/Moscow. В запуск входят только версии с `ingest_ts <= snapshot_committed_at`; равенство границе допустимо. Источник хранит 400 дней.

Поздние события и исправления до финального запуска включаются полной заменой M. Изменение после финализации создаёт `post_final_change_cnt`; автоматический backfill разрешён для последних 12 месяцев и всегда перечитывает месяц целиком по новому snapshot. Backfill старше 12 месяцев, но не старше 400 дней, требует заявки владельца; более ранний период недоступен из-за retention источника и не изменяется.

Перед staging процедура `CDM_COMM.ensure_device_base_partition(M)` под advisory lock создаёт отдельную месячную партицию `[M, M+1)`, если её ещё нет; M обязан быть не раньше `2024-01-01` и не позже текущего UTC-месяца. Затем данные записываются в `CDM_COMM.TABLE_DEVICE_BASE_MONTHLY_STG_<run_id>`, проходят проверки, и в одной Greenplum-транзакции выполняется exchange партиции M. При ошибке транзакция откатывается, опубликованная версия остаётся прежней. Retry использует новый staging и полностью заменяет месяц, поэтому идемпотентен. Staging удаляется только после успешного exchange или через 7 дней сервисной очисткой.

### Формирование ключа (kafka) / партиции (hdfs)

При повторном запуске партиция может дополняться или перезаписываться по выбору оператора.

- Kafka не используется: Kafka-ключ — `не применимо`.
- HDFS-приёмник отсутствует: HDFS-партиция — `не применимо`. Источник физически партиционирован `months(event_date)`; это используется только для pruning.
- Greenplum: range partition по `FIELD_MONTH`, одна партиция на календарный месяц; `DISTRIBUTED BY (FIELD_HOME_REGION_CODE, FIELD_DEVICE_VENDOR)`.

### Структура данных

В следующем релизе добавляется обязательное поле processing_status NOT NULL без default.

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_MONTH | string или bigint; окончательный физический тип не выбран | Первое число месяца UTC; обязательность поля `FIELD_MONTH` не определена | Параметр DAG | `month_start` | DATE | Всегда day=1 |
| FIELD_HOME_REGION_CODE | VARCHAR(16) | Домашний регион или `UNKNOWN`; `NOT NULL` | `TABLE_SUBSCRIBER_PROFILE_SCD` | `home_region_code` | STRING | Temporal JOIN на последнее событие |
| FIELD_DEVICE_VENDOR | VARCHAR(128) | Вендор либо `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `vendor_name` | STRING | `trim`, fallback |
| FIELD_OS_FAMILY | VARCHAR(64) | Семейство ОС либо `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `os_family` | STRING | `trim`, fallback |
| FIELD_5G_CAPABILITY | VARCHAR(7) | `YES`, `NO`, `UNKNOWN`; `NOT NULL` | `DICT_TAC_DEVICE_SCD` | `is_5g_capable` | BOOLEAN | Явный mapping |
| FIELD_SUBSCRIBERS_CNT | BIGINT | Число уникальных активных tokens, >0; `NOT NULL` | `TABLE_SUBSCRIBER_DEVICE_EVENT` | `subscriber_token` | STRING | После выбора последнего устройства |
| FIELD_PROC_TS | TIMESTAMP | Время публикации UTC; `NOT NULL` | Airflow | `run_start_ts` | TIMESTAMP | Одинаково для партиции M |

### Пример данных

| FIELD_MONTH | FIELD_HOME_REGION_CODE | FIELD_DEVICE_VENDOR | FIELD_OS_FAMILY | FIELD_5G_CAPABILITY | FIELD_SUBSCRIBERS_CNT | FIELD_PROC_TS |
| :--- | :--- | :--- | :--- | :--- | ---: | :--- |
| INVALID_FIELD_MONTH=2026-02-30 | MOS | Apple | iOS | YES | 2845100 | 2026-08-16 00:05:44 |
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

Источник может присылать новые значения state без предварительного уведомления.

Автоматический replay гарантирован для любых периодов за последние 90 суток.

Понятие текущей версии определяется владельцем источника при каждом запуске.

Старые партиции и потребители остаются без изменений; план миграции не предусмотрен.

**Какое устройство считается устройством месяца?** Устройство из последнего валидного сетевого события абонента в UTC-месяце; tie-break полностью определён в шаге 2.

**Что означает `UNKNOWN` для 5G?** TAC отсутствует в историческом срезе справочника. Это не означает `NO` и не включается в знаменатель продуктовой доли 5G; доля рассчитывается как `YES / (YES + NO)`.

**Почему сотрудник исключён?** Это явная граница коммерческой установленной базы; M2M, employee и test анализируются в отдельных продуктах.

**Как защищаются данные?** Исходные token и IMEI доступны только service account `device-base-etl`; они не попадают в staging-логи и результат. Агрегат разрешён ролям `commercial_device_read` и `network_strategy_read`. Группы с малым count не раскрывают PII, но выгрузка наружу контура запрещена политикой продукта.

**Что произойдёт при изменении TAC?** Обычный расчёт использует историческую версию на последнее событие. Исправление прошлого требует полного backfill месяца. Изменение ключа, формулы или enum публикуется как major-версия отдельной таблицы; добавление nullable-атрибута — как minor после уведомления потребителей.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-08-18 | Светлана Осина | Первичная версия с выбором последнего устройства, SCD2-срезами и атомарным exchange. |
