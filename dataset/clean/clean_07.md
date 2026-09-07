# Потоковые данные/витрины

| **Общие сведения** | Суточная витрина потребления услуг международного роуминга. Одна строка — агрегат за календарные сутки UTC по домашнему региону, стране пребывания и типу услуги. |
| :--- | :--- |
| **Решаемая проблема** | Финансы и роуминговый продукт получают один согласованный набор количества сессий, уникальных абонентов, трафика и начислений. В скоупе — финальные тарифицируемые записи международного роуминга; национальный роуминг, незавершённые и отменённые записи исключены. |
| **Продуктовые метрики** | 100% принятых финальных CDR попадают ровно в одну группу; сумма начислений витрины точно равна сумме `charge_rub` принятых CDR с точностью 4 знака; предварительная версия D доступна D+1 до 06:00 UTC, финальная — D+8 до 06:00 UTC. |
| **Заказчики** | Блок международного роуминга, финансовая аналитика MTS Big Data. |
| **Нефункциональные требования** | 120–800 млн CDR/сутки, до 1,5 ТБ/сутки; хранение результата 5 лет; расчёт Spark на `hadoop-billing-prod-02`; RPO 24 часа, RTO 4 часа; повторный запуск не должен создавать дубли. |
| **Системы-источники** | `BILLING_ROAM`, нормализующий TAP/RAP и онлайн-billing CDR в единую RAW-таблицу. |
| **Data Catalog** | [Продукт Roaming Daily Usage](https://datacatalog.corp.mts.ru/products/roaming-daily-usage) |
| **Исходники проекта** | [GitLab: finance/roaming-daily-usage](https://gitlab.corp.mts.ru/bigdata/finance/roaming-daily-usage) |
| **Команда** | Павел Миронов — аналитик; Ольга Титова — разработчик; Алексей Громов — QA; Елена Соколова — владелец продукта. |
| **JIRA** | [ROAMDATA-2084 — Суточные агрегаты роуминга](https://jira.corp.mts.ru/browse/ROAMDATA-2084) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| `RAW_BILLING.TABLE_ROAMING_CDR`, полный путь `/warehouse/raw/billing/roaming_cdr/`; raw-строки и snapshot history хранятся 5 лет | HDFS/Iceberg, кластер `hadoop-billing-prod-02` | [Data Catalog: TABLE_ROAMING_CDR](https://datacatalog.corp.mts.ru/tables/RAW_BILLING/TABLE_ROAMING_CDR) | Apache Iceberg v2, Parquet `ZSTD`; схема Hive Metastore `RAW_BILLING.TABLE_ROAMING_CDR` версии 3.2; десериализация Spark Iceberg reader по snapshot ID. Для расчёта фиксируется один snapshot на запуск. |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DWH_REF.DICT_MCC_COUNTRY_SCD` | [Data Catalog: DICT_MCC_COUNTRY_SCD](https://datacatalog.corp.mts.ru/tables/DWH_REF/DICT_MCC_COUNTRY_SCD) | Greenplum `gp-ref-prod-01`, реляционная модель 2.1; Spark JDBC читает repeatable-read snapshot. Исторический MCC → ISO alpha-2; ключ версии `(mcc, valid_from_utc)`, интервал `[valid_from_utc, valid_to_utc)`, открытая версия имеет `valid_to_utc IS NULL`. Snapshot ID сохраняется с run metadata. |

### Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| `CDM_ROAM.TABLE_ROAMING_USAGE_DAILY` | HDFS/Iceberg, кластер `hadoop-billing-prod-02`, полный путь `/warehouse/cdm/roam/roaming_usage_daily/` | [Data Catalog: TABLE_ROAMING_USAGE_DAILY](https://datacatalog.corp.mts.ru/tables/CDM_ROAM/TABLE_ROAMING_USAGE_DAILY) | Apache Iceberg v2, Parquet `ZSTD`; целевая модель версии 1.0 в Hive Metastore; сериализация Spark DataFrame writer v2 с атомарным `overwritePartitions`, чтение Iceberg reader по snapshot metadata. |

### Схема потоков данных

`BILLING_ROAM` → Iceberg RAW `TABLE_ROAMING_CDR` → фиксация snapshot → фильтрация и дедупликация → temporal JOIN `DICT_MCC_COUNTRY_SCD` → суточная агрегация → Iceberg CDM `TABLE_ROAMING_USAGE_DAILY`.

Гранулярность и бизнес-ключ результата: `(FIELD_BIZ_DATE, FIELD_HOME_REGION_CODE, FIELD_VISITED_COUNTRY_CODE, FIELD_SERVICE_TYPE)`. Каждая принятая версия CDR входит ровно в одну строку результата.

### Алгоритм обработки потока

#### Шаг 1. Фильтрация данных

Расчёт дня D читает change records источника по `session_start_ts` в полуоткрытом UTC-интервале `[D 00:00:00, D+1 00:00:00)`. Контракт источника требует полный business payload, исходный `session_start_ts`, непустые `cdr_id`, `source_update_ts`, `source_file_name`, `source_row_number` и `operation IN ('INSERT','UPDATE','DELETE')` для любой операции, включая `DELETE`.

Сначала дедуплицировать change records по `cdr_id`: сохранить строку с максимальным `source_update_ts`, затем с максимальным `source_file_name`, затем с максимальным `source_row_number`. Winning-операция `DELETE` удаляет CDR из расчёта. Равенство всех полей сортировки при различающемся payload считается ошибкой источника и блокирует публикацию. Затем для winning `INSERT`/`UPDATE` применить все условия:

```sql
record_status = 'FINAL'
AND roaming_scope = 'INTERNATIONAL'
AND subscriber_token RLIKE '^[0-9a-f]{64}$'
AND home_region_code RLIKE '^[A-Z0-9]{2,8}$'
AND visited_mcc BETWEEN 200 AND 799
AND service_type IN ('VOICE', 'SMS', 'DATA')
AND session_end_ts >= session_start_ts
AND uplink_bytes >= 0
AND downlink_bytes >= 0
AND charge_rub >= 0
```

Все timestamps источника — `TIMESTAMP` UTC с точностью миллисекунда. Для `VOICE` и `SMS` оба поля bytes обязаны быть 0; нарушение исключается. Некорректные строки не входят в результат и учитываются в `invalid_cdr_cnt` по взаимоисключающей первой причине из порядка условий выше. Пустой доступный вход создаёт пустую партицию D и успешный контроль; ошибка чтения источника останавливает запуск до записи.

#### Шаг 2. Обогащение данных

Выполнить `LEFT JOIN` по `cdr.visited_mcc = dict.mcc` и `session_start_ts >= valid_from_utc AND (session_start_ts < valid_to_utc OR valid_to_utc IS NULL)`.

- Кардинальность — many-to-zero-or-one. Несколько совпадений для одного CDR блокируют публикацию D.
- При одном совпадении взять `country_code`.
- При отсутствии совпадения установить `FIELD_VISITED_COUNTRY_CODE = 'ZZ'` и увеличить `unknown_mcc_cnt`.
- Пустой или недоступный справочник блокирует расчёт; fallback `ZZ` применяется только к отсутствующему конкретному MCC, а не к недоступности всего справочника.

Другие справочники и НСИ не используются. `FIELD_HOME_REGION_CODE` является атрибутом авторитетного billing snapshot в CDR и не переопределяется.

#### Шаг 3. Трансформация и агрегация

1. `FIELD_BIZ_DATE = CAST(session_start_ts AS DATE)` в UTC.
2. `FIELD_SERVICE_TYPE = service_type` без изменения регистра после валидации enum.
3. Сгруппировать по полному бизнес-ключу.
4. `FIELD_SESSIONS_CNT = COUNT(*)`; `FIELD_USERS_CNT = COUNT(DISTINCT subscriber_token)`.
5. Сначала просуммировать `uplink_bytes + downlink_bytes` как `DECIMAL(38,0)`, затем единожды рассчитать `FIELD_TRAFFIC_MB = ROUND(sum_bytes / 1048576, 3)` по правилу half-up. Для `VOICE` и `SMS` результат равен `0.000`.
6. `FIELD_CHARGE_RUB = SUM(CAST(charge_rub AS DECIMAL(20,4)))` с целевым типом `DECIMAL(24,4)`, без округления. Отрицательные начисления не входят в этот продукт и публикуются в витрине корректировок.
7. `FIELD_PROC_TS` — UTC-время коммита запуска, одинаковое для всех строк записываемой партиции.

#### Шаг 4. Регламент, поздние данные и запись

- Предварительный запуск D выполняется D+1 в 05:00 UTC по snapshot, созданному не раньше D+1 04:50 UTC. Финальный запуск — D+8 в 05:00 UTC.
- CDR, появившиеся до финального cutoff D+8 05:00 UTC, включаются при следующей полной замене D. Запись ровно на cutoff входит, если её `source_update_ts <= D+8 05:00:00.000 UTC`.
- Более поздняя вставка, исправление или tombstone создаёт контроль `post_final_change_cnt` и задачу полного пересчёта D. Автоматический backfill разрешён за последние 90 дней; период от 91 дня до 5 лет пересчитывается по заявке финансового контролёра и явному snapshot ID. Более старый период недоступен из-за raw-retention и эскалируется владельцу источника без изменения результата.
- Запись — атомарный Iceberg `overwritePartitions` только для D. До commit проверяются уникальность ключа, неотрицательность метрик и сверка с контрольными суммами источника. При сбое snapshot не публикуется. Retry с тем же snapshot ID детерминирован; retry с новым snapshot пересчитывает D целиком.

### Формирование ключа (kafka) / партиции (hdfs)

- Kafka не используется: Kafka-ключ — `не применимо`.
- HDFS/Iceberg partition transform: `days(FIELD_BIZ_DATE)`. Полный путь: `/warehouse/cdm/roam/roaming_usage_daily/`.
- Сортировка файлов внутри партиции: `(FIELD_HOME_REGION_CODE, FIELD_VISITED_COUNTRY_CODE, FIELD_SERVICE_TYPE)`; она не меняет бизнес-смысл ключа.

### Структура данных

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| FIELD_BIZ_DATE | DATE | Дата начала CDR в UTC; `NOT NULL` | `TABLE_ROAMING_CDR` | `session_start_ts` | TIMESTAMP | UTC date |
| FIELD_HOME_REGION_CODE | STRING | Домашний регион, 2–8 символов; `NOT NULL` | `TABLE_ROAMING_CDR` | `home_region_code` | STRING | Валидируется regex |
| FIELD_VISITED_COUNTRY_CODE | CHAR(2) | ISO alpha-2 или `ZZ`; `NOT NULL` | `DICT_MCC_COUNTRY_SCD` | `country_code` | CHAR(2) | Temporal JOIN, fallback `ZZ` |
| FIELD_SERVICE_TYPE | STRING | `VOICE`, `SMS` или `DATA`; `NOT NULL` | `TABLE_ROAMING_CDR` | `service_type` | STRING | Без преобразования |
| FIELD_SESSIONS_CNT | BIGINT | Число финальных уникальных CDR, >= 1; `NOT NULL` | `TABLE_ROAMING_CDR` | `cdr_id` | STRING | `COUNT(*)` после дедупликации |
| FIELD_USERS_CNT | BIGINT | Число уникальных токенов, >= 1; `NOT NULL` | `TABLE_ROAMING_CDR` | `subscriber_token` | STRING | `COUNT(DISTINCT ...)` |
| FIELD_TRAFFIC_MB | DECIMAL(20,3) | Трафик MiB, >= 0; `NOT NULL` | `TABLE_ROAMING_CDR` | `uplink_bytes`, `downlink_bytes` | BIGINT | Делитель 1 048 576, half-up |
| FIELD_CHARGE_RUB | DECIMAL(24,4) | Точная сумма начислений в RUB, >= 0; `NOT NULL` | `TABLE_ROAMING_CDR` | `charge_rub` | DECIMAL(20,4) | Сумма без округления |
| FIELD_PROC_TS | TIMESTAMP | Время коммита UTC; `NOT NULL` | Spark | `processing_ts` | TIMESTAMP | Одинаково в пределах запуска D |

### Пример данных

| FIELD_BIZ_DATE | FIELD_HOME_REGION_CODE | FIELD_VISITED_COUNTRY_CODE | FIELD_SERVICE_TYPE | FIELD_SESSIONS_CNT | FIELD_USERS_CNT | FIELD_TRAFFIC_MB | FIELD_CHARGE_RUB | FIELD_PROC_TS |
| :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| 2026-07-12 | MOW | TR | DATA | 18420 | 9912 | 84520.375 | 1245012.4400 | 2026-07-20 05:42:10 |
| 2026-07-12 | MOW | TR | VOICE | 6430 | 4218 | 0.000 | 912300.1000 | 2026-07-20 05:42:10 |
| 2026-07-12 | MOW | TR | SMS | 3921 | 2804 | 0.000 | 78420.0000 | 2026-07-20 05:42:10 |
| 2026-07-12 | SPE | FI | DATA | 7210 | 4360 | 26990.125 | 318774.9200 | 2026-07-20 05:42:10 |
| 2026-07-12 | SPE | FI | VOICE | 1990 | 1311 | 0.000 | 287640.5500 | 2026-07-20 05:42:10 |
| 2026-07-12 | KZN | AE | DATA | 4067 | 2881 | 19044.750 | 441908.2000 | 2026-07-20 05:42:10 |
| 2026-07-12 | EKB | KZ | DATA | 8840 | 5110 | 32108.003 | 285110.0900 | 2026-07-20 05:42:10 |
| 2026-07-12 | NSK | TH | SMS | 1455 | 1102 | 0.000 | 29100.0000 | 2026-07-20 05:42:10 |
| 2026-07-12 | SAM | DE | VOICE | 874 | 602 | 0.000 | 130512.8000 | 2026-07-20 05:42:10 |
| 2026-07-12 | UFA | ZZ | DATA | 12 | 9 | 42.500 | 1190.4000 | 2026-07-20 05:42:10 |

### DDL

```sql
CREATE TABLE IF NOT EXISTS CDM_ROAM.TABLE_ROAMING_USAGE_DAILY (
    FIELD_BIZ_DATE DATE NOT NULL,
    FIELD_HOME_REGION_CODE STRING NOT NULL,
    FIELD_VISITED_COUNTRY_CODE CHAR(2) NOT NULL,
    FIELD_SERVICE_TYPE STRING NOT NULL,
    FIELD_SESSIONS_CNT BIGINT NOT NULL,
    FIELD_USERS_CNT BIGINT NOT NULL,
    FIELD_TRAFFIC_MB DECIMAL(20,3) NOT NULL,
    FIELD_CHARGE_RUB DECIMAL(24,4) NOT NULL,
    FIELD_PROC_TS TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(FIELD_BIZ_DATE))
LOCATION '/warehouse/cdm/roam/roaming_usage_daily/'
TBLPROPERTIES (
  'format-version' = '2',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'zstd'
);
```

Проверки перед commit: ключ уникален; `FIELD_USERS_CNT <= FIELD_SESSIONS_CNT`; все счётчики положительны; денежные и трафиковые поля неотрицательны; для `VOICE`/`SMS` трафик равен нулю; сумма сессий совпадает с числом принятых CDR, а сумма charge — с контрольной суммой источника точно до четырёх знаков.

### FAQ

**К какому дню относится сессия через полночь?** К UTC-дате `session_start_ts`; одна CDR не делится между днями.

**Почему неизвестная страна не отбрасывается?** Код `ZZ` сохраняет финансовую полноту. Его доля контролируется отдельно, после исправления справочника день пересчитывается.

**Что означает MiB?** В поле с суффиксом `_MB` для совместимости продукта используется 1 MiB = 1 048 576 bytes; округление выполняется после суммы bytes.

**Какие данные чувствительные?** Выход агрегирован и не содержит идентификаторов абонента. `subscriber_token` применяется только внутри расчёта уникальных пользователей, не пишется в результат и логи. Чтение результата разрешено группам `roam_finance_read` и `roam_product_read`; аудит хранится 1 год.

**Как меняется контракт?** Только добавление nullable-поля допускается в minor-версии. Любое изменение типа, ключа, формулы или смысла требует major-версии, параллельной публикации и подтверждения финансового заказчика.

### История изменений

| Версия | Дата | Автор | Изменение |
| :--- | :--- | :--- | :--- |
| 1.0 | 2026-07-21 | Павел Миронов | Утверждены формулы, temporal JOIN, два cutoff, атомарная запись и контрольные сверки. |
