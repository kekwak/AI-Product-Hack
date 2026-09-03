Потоковые данные/витрины

Общие сведения

Модуль обеспечивает получение сведений о текущем и историческом местоположении абонентских устройств в реальном времени. Обработка основана на сигнальном трафике сетей радиодоступа (3G/4G), поступающем от платформы PROVIDER_GEO.

Решаемая проблема

Замена устаревшей технологии геолокации абонентов по данным радиоинтерфейса. Цель — создание отечественного решения с потоковой обработкой и интеграцией в Data Lake.

Продуктовые метрики
- Задержка: < 1 мин
- Пропускная способность: до 100 000 событий/сек
- Точность локации: 3G — 50–500 м, 4G — 10–100 м

Заказчики
- BigData Unit X

Нефункциональные требования
- Инкрементная загрузка по часовым партициям
- Географическое разбиение по регионам
- Стриминг в реальном времени (задержка ≈ 0 сек)
- Хранение: Kafka — 24 ч, RAW-слой — 30 дней

Системы-источники
PROVIDER_GEO — мультивендорная платформа агрегации событий 3G/4G и MDT.

Data Catalog
- http://datacatalog.corp/provider_geo

Исходники проекта
- http://gitlab.corp/project

Команда
- USER_B - аналитик
- USER_C - разработчик
(USER_A - Product Owner)

JIRA

PROJECT-XX123 — Описание потоковой обработки геоданных. http://jira.corp/PROJECT-XX123

Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Топики 3G (TOPIC_GEO3G_CENTRAL, EAST, NW, VOLGA, SIB, SOUTH, URAL) | Kafka (кластер `kafka-prod-01`) | [Data Catalog: Kafka 3G](http://datacatalog.corp/kafka3g) | JSON |
| Топики 4G (TOPIC_GEO4G_CENTRAL, EAST, NW, VOLGA, SIB, SOUTH, URAL) | Kafka (кластер `kafka-prod-01`) | [Data Catalog: Kafka 4G](http://datacatalog.corp/kafka4g) | JSON |

Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Справочник базовых станций RNC/eNodeB (DICT_BS_REF) | [Data Catalog: DICT_BS](http://datacatalog.corp/dict_bs) | Нормативно-справочная информация для маппинга CELL_ID |

Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| TABLE_GEO_RAW_3G (Данные 3G) | HDFS (путь: `/data/raw/geo/3g/`) | [DC: RAW_3G](http://datacatalog.corp/raw3g) | ORC |
| TABLE_GEO_RAW_4G (Данные 4G) | HDFS (путь: `/data/raw/geo/4g/`) | [DC: RAW_4G](http://datacatalog.corp/raw4g) | ORC |
| TABLE_GEO_DDS_DETAILED | Greenplum | [DC: DDS](http://datacatalog.corp/dds) | - |
| TABLE_GEO_ADDS_AGG | Greenplum | [DC: ADDS](http://datacatalog.corp/adds) | - |
| TABLE_GEO_CDM_INTERVALS | Greenplum | [DC: CDM](http://datacatalog.corp/cdm) | - |

Схема потоков данных
PROVIDER_GEO → Kafka (`kafka-prod-01`) → Apache Flink → RAW (`/data/raw/`) → DDS → ADDS → CDM

Алгоритм обработки потока

Шаг 1. Фильтрация данных
Исключаются записи с пустыми координатами или техническими ошибками (типовой фильтр): `FIELD_LAT IS NOT NULL AND FIELD_LON IS NOT NULL`.

Шаг 2. Обогащение данных
Обогащение CELL_ID данными из справочника DICT_BS_REF (RNC/eNodeB). В остальном — не применимо (для RAW-слоя).

Шаг 3. Трансформация
Обработка хэндоверов, расчет геоинтервалов (применяется при загрузке в DDS/CDM). На этапе RAW — не применимо.

Формирование ключа (kafka) / партиции (hdfs)
- Ключ Kafka: `FIELD_IMSI`
- Партиция HDFS: `FIELD_DATE_EVENT`, `FIELD_HOUR`

Структура данных (на примере TABLE_GEO_RAW_3G)

| Приемники: Атрибут | Приемники: Тип данных | Описание атрибута | Источники: Источник | Источники: Атрибут | Источники: Тип данных | Комментарий |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| FIELD_REGION | string | Регион (NOT NULL) | Kafka | region | string | Из названия топика |
| FIELD_DATE_EVENT | date | Дата события (NOT NULL) | Kafka | date_event | string | Формат YYYY-MM-DD |
| FIELD_HOUR | int | Час события (партиция) (NOT NULL) | Kafka | hour | int | - |
| FIELD_IMSI | string | IMSI (NOT NULL) | Kafka | imsi | string | - |
| FIELD_MSISDN | string | MSISDN (NULLABLE) | Kafka | msisdn | string | - |
| FIELD_IMEI | string | IMEI (NULLABLE) | Kafka | imei | string | - |
| FIELD_LAT | double | Широта геопозиции (NOT NULL) | Kafka | lat | double | - |
| FIELD_LON | double | Долгота геопозиции (NOT NULL) | Kafka | lon | double | - |
| FIELD_RADIUS | int | Радиус точности (метры) (NULLABLE) | Kafka | radius | int | - |
| FIELD_CELL_START | long | ID начальной БС (NULLABLE) | Kafka | cell_start | long | - |
| FIELD_CELL_END | long | ID конечной БС (NULLABLE) | Kafka | cell_end | long | - |
| FIELD_TIME_START | long | Время начала события (NOT NULL) | Kafka | time_start | long | Unix-время |
| FIELD_TIME_END | long | Время окончания события (NULLABLE) | Kafka | time_end | long | - |
| FIELD_LOC_ACCURACY | int | Точность локации (NULLABLE) | Kafka | loc_accuracy | int | - |
| FIELD_VENDOR | string | Вендор БС (NULLABLE) | Kafka | vendor | string | - |

Пример данных (TABLE_GEO_RAW_3G)

| FIELD_REGION | FIELD_DATE_EVENT | FIELD_HOUR | FIELD_IMSI | FIELD_MSISDN | FIELD_LAT | FIELD_LON | FIELD_RADIUS | FIELD_TIME_START |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Сибирь | 2026-09-03 | 14 | 250011234567890 | 79991234567 | 55.0308 | 82.9204 | 100 | 1718000000 |
| Центр | 2026-09-03 | 14 | 250011234567891 | 79991234568 | 55.7558 | 37.6173 | 50 | 1718000010 |
| Урал | 2026-09-03 | 14 | 250011234567892 | 79991234569 | 56.8389 | 60.6057 | 150 | 1718000020 |
| Юг | 2026-09-03 | 14 | 250011234567893 | 79991234570 | 45.0393 | 38.9800 | 70 | 1718000030 |
| Сибирь | 2026-09-03 | 14 | 250011234567894 | 79991234571 | 55.0310 | 82.9210 | 100 | 1718000040 |
| Центр | 2026-09-03 | 14 | 250011234567895 | 79991234572 | 55.7560 | 37.6180 | 50 | 1718000050 |
| Дальний Восток| 2026-09-03 | 14 | 250011234567896 | 79991234573 | 43.1155 | 131.8855| 200 | 1718000060 |
| Поволжье | 2026-09-03 | 14 | 250011234567897 | 79991234574 | 55.7963 | 49.1088 | 80 | 1718000070 |
| Северо-Запад | 2026-09-03 | 14 | 250011234567898 | 79991234575 | 59.9343 | 30.3351 | 60 | 1718000080 |
| Урал | 2026-09-03 | 14 | 250011234567899 | 79991234576 | 56.8400 | 60.6100 | 150 | 1718000090 |


DDL

```sql
CREATE TABLE IF NOT EXISTS TABLE_GEO_RAW_3G (
    FIELD_REGION STRING,
    FIELD_IMSI STRING,
    FIELD_MSISDN STRING,
    FIELD_IMEI STRING,
    FIELD_LAT DOUBLE,
    FIELD_LON DOUBLE,
    FIELD_RADIUS INT,
    FIELD_CELL_START BIGINT,
    FIELD_CELL_END BIGINT,
    FIELD_TIME_START BIGINT,
    FIELD_TIME_END BIGINT,
    FIELD_LOC_ACCURACY INT,
    FIELD_VENDOR STRING
)
PARTITIONED BY (FIELD_DATE_EVENT DATE, FIELD_HOUR INT)
STORED AS ORC
LOCATION '/data/raw/geo/3g/';
```

FAQ
В: Что означает FIELD_LOC_ACCURACY?
О: Указывает на погрешность геолокации в метрах (зависит от типа сети, для 3G в среднем 50-500м).

История изменений
- 1.0 — Первичное создание документации, добавление структур слоев RAW и маппинга источников.
