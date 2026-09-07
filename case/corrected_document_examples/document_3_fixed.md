Потоковые данные/витрины

Общие сведения
Агрегат по устройствам абонентов: ежемесячная сводка по вендорам устройств и регионам пользователей.

Решаемая проблема
Получение ежемесячной аналитики по распределению абонентов и их устройств (по вендорам) в разрезе регионов для MVNO-бизнеса.

Продуктовые метрики
- Покрытие региональной аналитикой: 100% абонентов
- Точность определения вендора: > 95%

Заказчики
- MVNO Unit X

Нефункциональные требования
- Способ загрузки: Инкремент (ежемесячно, 1 раз в месяц).
- Глубина данных: с 01.05.2023 (обязательно загрузить исторические данные).
- Обновление: Только полная перезагрузка месяца (без upsert).
- Часовой пояс: UTC.

Системы-источники
IUM — IN-платформа (сбор событий интернет-трафика, звонков и SMS).

Data Catalog
- http://datacatalog.corp/cdm_nets_agg_devices

Исходники проекта
- http://gitlab.corp/cdm_agg_devices

Команда
- USER_C - аналитик
- USER_D - разработчик
(PO: USER_A, TechPM: USER_B, QA: USER_E)

JIRA
PROJECT-XX125 — Витрина агрегата по устройствам. http://jira.corp/PROJECT-XX125

Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| SCHEMA_RAW.TABLE_IUM_RAW_PS (интернет-трафик) | HDFS/Hive | [Data Catalog: RAW_PS](http://datacatalog.corp/raw_ps) | ORC |
| SCHEMA_RAW.TABLE_IUM_RAW_MS (звонки и SMS) | HDFS/Hive | [Data Catalog: RAW_MS](http://datacatalog.corp/raw_ms) | ORC |

Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| SCHEMA_ADDS.TABLE_BS_REF | [Data Catalog: BS_REF](http://datacatalog.corp/bs_ref) | Справочник базовых станций |
| SCHEMA_DIC.TABLE_REGION_REF | [Data Catalog: REGION_REF](http://datacatalog.corp/region_ref) | Справочник регионов |
| SCHEMA_DIC.TABLE_DEVICE_REF | [Data Catalog: DEVICE_REF](http://datacatalog.corp/device_ref) | Справочник устройств |

Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| SCHEMA_CDM_NETS.TABLE_AGG_DEVICES | HDFS (`hadoop-prod-01`, путь: `/data/cdm/nets/agg_devices/`) | [DC: AGG_DEVICES](http://datacatalog.corp/agg_devices) | ORC |

Схема потоков данных
IUM (IN-платформа) → RAW (`TABLE_IUM_RAW_PS`, `TABLE_IUM_RAW_MS`) → Обогащение НСИ (`BS_REF`, `REGION_REF`, `DEVICE_REF`) → CDM (`TABLE_AGG_DEVICES`)

Алгоритм обработки потока

Шаг 1. Фильтрация данных
Берутся все записи уникальных `FIELD_IMSI` за отчетный календарный месяц. Типовой фильтр: партиция события попадает в `FIELD_BIZ_DATE` (месяц расчета).

Шаг 2. Обогащение данных
- **Определение вендора:** Для каждого `IMSI` берется последний за месяц `IMEI`. Извлекается `tac = substring(imei, 1, 8)`. Присоединяется `FIELD_VENDOR_NAME` по `tac` из `TABLE_DEVICE_REF`.
- **Определение региона:** Находится последняя запись с ненулевыми `LAC` и `CELL_ID`. Сопоставление с `TABLE_BS_REF`:
  - Приоритет `(lac, cell_id)`
  - Если `lac = 0` → джойн только по `cell_id` (4G)
  - Если `cell = 0` → джойн только по `lac`
- Присоединение `FIELD_REGION_NAME` из `TABLE_REGION_REF`.

Шаг 3. Трансформация (Агрегация)
Убираются дубли: на выходе — одна `region_code` на абонента.
Происходит группировка по `FIELD_REGION_NAME` и `FIELD_VENDOR_NAME`.
Расчет целевой метрики: `FIELD_USERS_CNT = count(distinct FIELD_IMSI)`.

Формирование ключа (kafka) / партиции (hdfs)
- Ключ Kafka: Не применимо.
- Партиция HDFS: `FIELD_BIZ_DATE` (1-е число месяца, за который формируется агрегат).

Структура данных (TABLE_AGG_DEVICES)

| Приемники: Атрибут | Приемники: Тип данных | Описание атрибута | Источники: Источник | Источники: Атрибут | Источники: Тип данных | Комментарий |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| FIELD_REGION_NAME | string | Название региона (NOT NULL) | TABLE_REGION_REF | region_name | string | Определяется по lac/cell |
| FIELD_VENDOR_NAME | string | Наименование вендора устройства (NOT NULL) | TABLE_DEVICE_REF | vendor_name | string | tac -> vendor_name |
| FIELD_USERS_CNT | bigint | Уникальное кол-во абонентов (NOT NULL) | IUM_RAW | imsi | string | count(distinct FIELD_IMSI) |
| FIELD_PROC_TS | timestamp | Дата/время создания агрегата (NOT NULL) | Airflow | execution_date | timestamp | Метка обработки DAG |
| FIELD_BIZ_DATE | date | Бизнес-дата (NOT NULL) | IUM_RAW | biz_date | date | Партиция (1-е число месяца) |

Пример данных (TABLE_AGG_DEVICES)

| FIELD_REGION_NAME | FIELD_VENDOR_NAME | FIELD_USERS_CNT | FIELD_PROC_TS | FIELD_BIZ_DATE |
| :--- | :--- | :--- | :--- | :--- |
| Центр | Apple | 154200 | 2026-08-02 03:00:00 | 2026-07-01 |
| Центр | Samsung | 120500 | 2026-08-02 03:00:00 | 2026-07-01 |
| Центр | Xiaomi | 98000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Сибирь | Xiaomi | 85000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Сибирь | Samsung | 72100 | 2026-08-02 03:00:00 | 2026-07-01 |
| Сибирь | Apple | 45000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Урал | Xiaomi | 64000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Урал | Samsung | 58000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Урал | Apple | 39000 | 2026-08-02 03:00:00 | 2026-07-01 |
| Юг | Realme | 21000 | 2026-08-02 03:00:00 | 2026-07-01 |

DDL

```sql
CREATE TABLE IF NOT EXISTS SCHEMA_CDM_NETS.TABLE_AGG_DEVICES (
    FIELD_REGION_NAME STRING,
    FIELD_VENDOR_NAME STRING,
    FIELD_USERS_CNT BIGINT,
    FIELD_PROC_TS TIMESTAMP
)
PARTITIONED BY (FIELD_BIZ_DATE DATE)
STORED AS ORC
LOCATION '/data/cdm/nets/agg_devices/';
```

FAQ
В: Что происходит, если у абонента за месяц было несколько устройств?
О: Для определения вендора берется только последний активный IMEI по времени (за расчетный месяц).

В: Как обрабатывается 4G-трафик при определении региона?
О: При `lac = 0` (что характерно для 4G) соединение со справочником `TABLE_BS_REF` происходит только по `cell_id`.

История изменений
- 1.0 — Первичное создание документации, приведение к шаблону, добавление маппинга и DDL.
