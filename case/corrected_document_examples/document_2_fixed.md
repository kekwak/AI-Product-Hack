Потоковые данные/витрины

Общие сведения
Платформа обслуживает prepaid-абонентов. Файлы, поступающие с системы, содержат телеком-события абонентов, заведённых на платформе. 

Решаемая проблема
Унификация и потоковая обработка биллинговых CDR-данных prepaid-абонентов для интеграции в корпоративное хранилище.

Продуктовые метрики
- Доступность данных: 99.9%
- Задержка доставки: не более 5 минут

Заказчики
- BigData

Нефункциональные требования
- Объем данных: ~500 ГБ/сутки
- Задержка потоковой обработки: не более 5 минут

Системы-источники
PI_System — система сбора CDR-файлов с сетевых элементов подвижной связи.

Data Catalog
- http://datacatalog.corp/mscp_cdr

Исходники проекта
- http://gitlab.corp/mscp_cdr_project

Команда
- USER_B - аналитик
- USER_C - разработчик
(USER_A - Product Owner)

JIRA
PROJECT-XX124 — Обработка потока MSCP CDR. http://jira.corp/PROJECT-XX124

Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Топики Kafka (TOPIC_MSCP_CDR_CENTRAL, EAST, SIB, NW, VOLGA, SOUTH) | Kafka (кластер `kafka-prod-02`) | [Data Catalog: MSCP Kafka](http://datacatalog.corp/kafka_mscp) | CSV (сжатые gzip) |

Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| Не применимо | Не применимо | Нормативно-справочная информация (НСИ) на данном этапе не используется |

Приемники данных

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| TABLE_MSCP_RAW_KFK (необработанные CDR из Kafka) | HDFS (путь: `/data/raw/mscp/kfk/`) | [DC: MSCP_RAW_KFK](http://datacatalog.corp/mscp_raw_kfk) | ORC |
| TABLE_MSCP_RAW_SFTP (необработанные CDR из SFTP) | HDFS (путь: `/data/raw/mscp/sftp/`) | [DC: MSCP_RAW_SFTP](http://datacatalog.corp/mscp_raw_sftp) | CSV |

Схема потоков данных
PI_System → Kafka (`kafka-prod-02`, топики по регионам) → Apache Flink → HDFS (`/data/raw/mscp/`)

Алгоритм обработки потока

Шаг 1. Фильтрация данных
Учитываются только события, произошедшие не ранее 7 дней назад и не позже 2 часов вперёд от текущего времени. 
*Применяемый фильтр по полю:* `FIELD_CALL_START_TIME`.

Шаг 2. Трансформация данных
1. Очистка числовых строк от лидирующих нулей: из целевых полей удаляются лидирующие нули до первого значимого символа.
2. Коррекция часового пояса для временных меток:
   - Для региона east (и sib): +7 часов.
   - Для регионов central, nw, volga, south: +3 часа.

Шаг 3. Обогащение данных
Не применимо (обогащение справочниками не производится).

Формирование ключа (kafka) / партиции (hdfs)
- Ключ Kafka: `FIELD_IMSI`
- Партиции HDFS: `FIELD_BIZ_DATE`, `FIELD_HOUR`

Структура данных (TABLE_MSCP_RAW_KFK)

| Приемники: Атрибут | Приемники: Тип данных | Описание атрибута | Источники: Источник | Источники: Атрибут | Источники: Тип данных | Комментарий |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| FIELD_REGION | string | Название региона (NOT NULL) | Kafka | region | string | region sib = east |
| FIELD_BIZ_DATE | date | Бизнес-дата события (NOT NULL) | Kafka | biz_date | string | Партиция |
| FIELD_HOUR | int | Часовая партиция (NOT NULL) | Kafka | hour | int | Партиция |
| FIELD_IMSI | bigint | IMSI (NOT NULL) | Kafka | imsi | bigint | - |
| FIELD_CALLING_NUMBER| string | Номер вызывающего абонента (NULLABLE)| Kafka | calling_num | string | Очистка от нулей |
| FIELD_CALLED_NUMBER | string | Номер вызываемого абонента (NULLABLE) | Kafka | called_num | string | Очистка от нулей |
| FIELD_CALL_START_TIME| timestamp | Время начала вызова (NOT NULL) | Kafka | call_start | string | Применяется фильтр 7д/2ч |
| FIELD_CALL_END_TIME | timestamp | Время окончания вызова (NULLABLE) | Kafka | call_end | string | - |
| FIELD_CALL_DURATION | int | Длительность вызова в сек (NULLABLE) | Kafka | duration | int | - |
| FIELD_IS_ROAMING | string | Признак роуминга (NULLABLE) | Kafka | is_roaming | string | true/false |
| FIELD_OL_SERVICE_TYPE| tinyint | Тип сервиса (NOT NULL) | Kafka | service_type| int | - |
| FIELD_TIME_ZONE_SHIFT| string | Сдвиг часового пояса (NULLABLE) | Kafka | tz_shift | string | - |
| FIELD_TIMEZONE_CALC | int | Вычисленный сдвиг в минутах (NULLABLE)| Kafka | tz_calc | int | Корректируется алгоритмом |
| FIELD_BALANCE | double | Текущий баланс абонента (NULLABLE) | Kafka | balance | double | - |
| FIELD_BALANCE_EXPIRE| timestamp | Срок истечения баланса (NULLABLE) | Kafka | bal_expire | string | - |

Пример данных (TABLE_MSCP_RAW_KFK)

| FIELD_REGION | FIELD_BIZ_DATE | FIELD_HOUR | FIELD_IMSI | FIELD_CALLING_NUMBER | FIELD_CALLED_NUMBER | FIELD_CALL_START_TIME | FIELD_CALL_DURATION | FIELD_OL_SERVICE_TYPE | FIELD_BALANCE |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| central | 2026-09-03 | 14 | 250012345678901 | 79991234567 | 79997654321 | 2026-09-03 14:01:00 | 120 | 1 | 150.50 |
| east | 2026-09-03 | 14 | 250012345678902 | 79991234568 | 79997654322 | 2026-09-03 14:05:00 | 45 | 1 | 42.00 |
| sib | 2026-09-03 | 14 | 250012345678903 | 79991234569 | 79997654323 | 2026-09-03 14:10:00 | 0 | 2 | 300.00 |
| nw | 2026-09-03 | 14 | 250012345678904 | 79991234570 | 79997654324 | 2026-09-03 14:15:30 | 60 | 1 | 15.20 |
| volga | 2026-09-03 | 14 | 250012345678905 | 79991234571 | 79997654325 | 2026-09-03 14:20:00 | 15 | 1 | 550.10 |
| south | 2026-09-03 | 14 | 250012345678906 | 79991234572 | 79997654326 | 2026-09-03 14:25:45 | 300 | 1 | 12.00 |
| central | 2026-09-03 | 14 | 250012345678907 | 79991234573 | 79997654327 | 2026-09-03 14:30:00 | 0 | 3 | 99.99 |
| east | 2026-09-03 | 14 | 250012345678908 | 79991234574 | 79997654328 | 2026-09-03 14:40:10 | 10 | 1 | 5.50 |
| sib | 2026-09-03 | 14 | 250012345678909 | 79991234575 | 79997654329 | 2026-09-03 14:45:00 | 90 | 1 | 210.00 |
| nw | 2026-09-03 | 14 | 250012345678910 | 79991234576 | 79997654330 | 2026-09-03 14:55:00 | 0 | 2 | 45.00 |

DDL

```sql
CREATE TABLE IF NOT EXISTS TABLE_MSCP_RAW_KFK (
    FIELD_REGION STRING,
    FIELD_IMSI BIGINT,
    FIELD_CALLING_NUMBER STRING,
    FIELD_CALLED_NUMBER STRING,
    FIELD_CALL_START_TIME TIMESTAMP,
    FIELD_CALL_END_TIME TIMESTAMP,
    FIELD_CALL_DURATION INT,
    FIELD_IS_ROAMING STRING,
    FIELD_OL_SERVICE_TYPE TINYINT,
    FIELD_TIME_ZONE_SHIFT STRING,
    FIELD_TIMEZONE_CALC INT,
    FIELD_BALANCE DOUBLE,
    FIELD_BALANCE_EXPIRE TIMESTAMP
)
PARTITIONED BY (FIELD_BIZ_DATE DATE, FIELD_HOUR INT)
STORED AS ORC
LOCATION '/data/raw/mscp/kfk/';
```

FAQ
Тип события (голос, SMS, данные):
Определяется по FIELD_OL_SERVICE_TYPE (см. выше).

Определение технологии связи (2G/3G/4G/5G):
- Для data-трафика — по полю radioAccessTechnology.
- Для голоса и SMS — косвенно: по наличию/отсутствию полей геолокации (например, CELL_ID, LAC).

История изменений
- 1.0 — Первичное создание документации, приведение к единому шаблону.
