# Потоковые данные/витрины

| **Общие сведения** | Пятиминутная витрина качества голосовых вызовов VoLTE `NET_VOLTE_QUALITY_5M`. Одна строка содержит итоговые показатели вызовов одного макрорегиона за окно UTC. |
| :--- | :--- |
| **Решаемая проблема** | Команды эксплуатации и Voice Product используют разные трактовки успешного соединения и аварийного обрыва. Поток создает единый проверяемый расчет для оперативного дашборда. В границы входят завершенные VoLTE-вызовы физических абонентов; тестовые вызовы, VoWiFi, CSFB, незавершенные сессии и прогнозы не входят. Пользователь должен видеть полную историю каждого изменения результата. |
| **Продуктовые метрики** | 1) ASR: доля успешно установленных вызовов. 2) Drop Rate: доля аварийных завершений среди установленных. 3) Средний MOS установленных вызовов. Публикация окна — не позднее 30 минут после его конца; целевой SLO — 99,7% окон в месяц. |
| **Заказчики** | Центр управления голосовой сетью; продукт «Voice Quality». |
| **Нефункциональные требования** | Средний поток — 25 000 событий/с, пик — 80 000 событий/с. Event time и все календарные границы — UTC. Watermark — 20 минут. Kafka retention — 7 суток, HDFS retention — 25 месяцев. Повторный расчет суток — до 45 минут. Конвейер должен обрабатывать повторную доставку без изменения результата. |
| **Системы-источники** | `VOICE_CDR_GATEWAY` — нормализованные финальные записи IMS/VoLTE о завершенных вызовах. |
| **Data Catalog** | [Карточка NET_VOLTE_QUALITY_5M](https://datacatalog.mts.ru/data-products/net-volte-quality-5m) |
| **Исходники проекта** | [GitLab: volte-quality-5m](https://gitlab.mts.ru/bigdata/voice/volte-quality-5m) |
| **Команда** | Ирина Лебедева — аналитик; Павел Новиков — разработчик; Олег Власов — QA. |
| **JIRA** | [VOICE-932](https://jira.mts.ru/browse/VOICE-932) |

### Источники данных

| Описание источника | Тип источника | Ссылка на источник | Сериализация |
| :--- | :--- | :--- | :--- |
| Финальная запись вызова, topic `voice.volte.call-final.v2`; Kafka key — `call_id`; операции `UPSERT` и `DELETE` | Kafka; кластер не указан | [Data Catalog: voice.volte.call-final.v2](https://datacatalog.mts.ru/topics/voice-volte-call-final-v2) | Protocol Buffers 3, message `mts.voice.volte.CallFinal`, файл `call_final.proto`, schema registry subject `voice.volte.call-final-value`, версия `7`; Confluent Protobuf framing, десериализация exact writer schema v7 |

### Источники обогащения данных

| Описание источника | Ссылка | Описание |
| :--- | :--- | :--- |
| `DICT_CELL_REGION_SCD2` | [Data Catalog: DICT_CELL_REGION_SCD2](https://datacatalog.mts.ru/tables/ref-dict-cell-region-scd2) | Сопоставляет `serving_cell_id` с `region_code` по полуинтервалу `[valid_from_utc, valid_to_utc)`. Ограничение исключает пересекающиеся версии одного cell. Используется snapshot на начало batch. |
| `DICT_VOLTE_RELEASE_CAUSE` | [Data Catalog: DICT_VOLTE_RELEASE_CAUSE](https://datacatalog.mts.ru/tables/ref-dict-volte-release-cause) | Версия 12; уникальный ключ `release_cause_code`, значение `release_class IN ('NORMAL','UNEXPECTED','REJECTED')`. Полный snapshot фиксируется на начало batch. Другие справочники не используются. |

### Приемники данных

В приемнике сохраняется только последняя опубликованная версия без журнала изменений.

| Описание данных | Кластер | Ссылка на Каталог | Сериализация |
| :--- | :--- | :--- | :--- |
| Hive-таблица `prod_voice.NET_VOLTE_QUALITY_5M` | HDFS-кластер `hdfs-prod-02`, полный путь `/data/prod/voice/volte_quality_5m/` | [Data Catalog: NET_VOLTE_QUALITY_5M](https://datacatalog.mts.ru/tables/prod-voice-net-volte-quality-5m) | ORC 1.9, ZLIB; логическая схема `voice.volte-quality-5m` версии `2`; Spark writer сопоставляет поля по имени, timestamps записывает в UTC |

### Схема потоков данных

```text
VOICE_CDR_GATEWAY -> Kafka kafka-voice-prod-02 -> Flink -> dedup/revisions
                                                         |-> DICT_CELL_REGION_SCD2
                                                         |-> DICT_VOLTE_RELEASE_CAUSE
                                                         -> 5-minute aggregation -> HDFS NET_VOLTE_QUALITY_5M
```

### Алгоритм обработки потока

Одна входная запись описывает финальное состояние логического вызова `call_id`. `call_end_time_utc` является event time и передается epoch milliseconds UTC. Окно — полуинтервал `[window_start_utc, window_start_utc + 5 minutes)`, в который попадает `call_end_time_utc`. Гранулярность результата — `(region_code, window_start_utc)`.

#### Шаг 1. Фильтрация данных

Некорректную запись разрешается либо исключить, либо сохранить без изменений.

1. Неуспешная десериализация или неизвестная версия схемы вызывает 3 retry через 15, 45 и 120 секунд; после них Kafka partition останавливается до вмешательства, offset не фиксируется, создается алерт `VOLTE_SCHEMA_BLOCKED`.
2. Для `UPSERT` обязательны: непустые `event_id`, `call_id`, `serving_cell_id`; `revision >= 1`; `access_type='VOLTE'`; `subscriber_type='MASS'`; `call_start_time_utc <= call_end_time_utc`; вычисленная длительность `FLOOR((call_end_time_utc-call_start_time_utc)/1000)` находится в `0..14400` секунд; `setup_result IN ('SUCCESS','FAILED')`; непустой `release_cause_code`. `mos_avg`, если задан, должен быть `1.00..5.00`. Нарушившая запись исключается и учитывается в `rejected_calls_total{reason}`.
3. `is_test_call=true`, `access_type!='VOLTE'` и `subscriber_type!='MASS'` исключаются как вне границ продукта и считаются в отдельных метриках.
4. Для `DELETE` обязательны `call_id`, `revision`, `event_id`, `call_end_time_utc`; `call_end_time_utc` обязан совпасть с удаляемой UPSERT-версией в семидневном состоянии, иначе событие блокируется с `DELETE_WINDOW_CONFLICT`. Остальные бизнес-поля не используются. DELETE удаляет логический вызов при replay, если его revision не меньше выбранного UPSERT. Если UPSERT отсутствует во всем семидневном состоянии, DELETE не меняет результат и учитывается в `orphan_delete_total`. Удаление старше семи суток не может быть применено: архивного источника в скоупе продукта нет, отклонение эскалируется владельцу источника.
5. Сначала дубли `event_id` схлопываются по максимальному `ingest_time_utc`, затем по лексикографически максимальному SHA-256 от исходных байтов Kafka value. Для одного `call_id` выбирается запись с максимальным `revision`, при равенстве — максимальный `ingest_time_utc`, затем `event_id`. Если выбрана DELETE, вызов не участвует в результате. Правило одинаково для online и replay.
6. Запись с `call_end_time_utc > ingest_time_utc + 5 minutes` исключается; равенство границе допустимо. События за watermark не меняют закрытое окно онлайн, считаются в `late_calls_total` и учитываются плановым replay последних 7 суток.

#### Шаг 2. Обогащение данных

1. Выполняется left temporal join к `DICT_CELL_REGION_SCD2` по `serving_cell_id` и времени окончания вызова. Кардинальность `N:0..1`. При отсутствии строки применяется `region_code='UNKNOWN'` и `unknown_cell_flag=true`. При более чем одной строке batch останавливается с `CELL_REGION_OVERLAP`.
2. Выполняется left join к `DICT_VOLTE_RELEASE_CAUSE` по `release_cause_code`, кардинальность `N:0..1`. При отсутствии используется `release_class='UNKNOWN'`, `unknown_cause_flag=true`; неизвестная причина не считается аварийным обрывом. Множественное совпадение останавливает batch с `RELEASE_CAUSE_DUPLICATE`.
3. Снимки обоих справочников и их `snapshot_id` сохраняются в метаданных batch. При backfill используется snapshot, действовавший на `call_end_time_utc` для SCD2, и версия справочника причин, указанная в параметрах replay; по умолчанию — версия 12.

#### Шаг 3. Агрегация и публикация

Для каждой группы вычисляются:

- `calls_total = COUNT(*)`;
- `calls_connected = COUNT_IF(setup_result='SUCCESS')`;
- `calls_dropped = COUNT_IF(setup_result='SUCCESS' AND release_class='UNEXPECTED')`;
- `asr_pct = ROUND(100.00 * calls_connected / calls_total, 2)`; пустые группы не создаются;
- `drop_rate_pct = ROUND(100.00 * calls_dropped / calls_connected, 2)`, а при `calls_connected=0` — `0.00`;
- `mos_avg = ROUND(AVG(mos_avg), 2)` только по установленным вызовам с ненулевым MOS; если таких нет — `NULL`;
- `low_mos_calls = COUNT_IF(setup_result='SUCCESS' AND mos_avg IS NOT NULL AND mos_avg < 3.50)`;
- `connected_duration_sec = SUM(duration_sec)` только по установленным вызовам;
- `unknown_cell_calls` и `unknown_cause_calls` — суммы соответствующих флагов.

Окно публикуется после watermark. Оркестратор раз в час пересчитывает из compacted состояния только часовые партиции последних 7 суток, затронутые новыми revision, DELETE или late-событиями; раз в сутки выполняется контрольный полный replay семидневного периода. Каждая часовая партиция пишется во staging и атомарно заменяется только после проверок. Повтор с тем же `batch_id` выполняет overwrite и дает тот же результат. При сбое опубликованная версия сохраняется; незавершенный staging очищается.

### Формирование ключа (kafka) / партиции (hdfs)

- Kafka key: непустой `call_id` в UTF-8; все revisions вызова попадают в один partition.
- Бизнес-ключ: `(region_code, window_start_utc)`.
- HDFS-партиции: `event_date_utc=DATE(window_start_utc)` и `event_hour_utc=HOUR(window_start_utc)` в UTC.
- Полный путь: `/data/prod/voice/volte_quality_5m/event_date_utc=YYYY-MM-DD/event_hour_utc=HH/`.

### Структура данных

| Приемники | | | Источники | | | |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Атрибут** | **Тип данных** | **Описание атрибута** | **Источник** | **Атрибут** | **Тип данных** | **Комментарий** |
| window_start_utc | timestamp | Начало пятиминутного окна UTC; `NOT NULL` | Kafka | call_end_time_utc | long | `FLOOR_5_MINUTES(FROM_EPOCH_MS(...))`; часть ключа |
| region_code | string | Макрорегион; `NOT NULL` | DICT_CELL_REGION_SCD2/расчет | region_code | string | Справочник либо `UNKNOWN`; часть ключа |
| calls_total | bigint | Число выбранных вызовов, `>0`; `NOT NULL` | Расчет | call_id | string | Количество после revision/dedup |
| calls_connected | bigint | Установленные вызовы, `0..calls_total`; `NOT NULL` | Kafka | setup_result | string | `SUCCESS` |
| calls_dropped | bigint | Аварийные завершения, `0..calls_connected`; `NOT NULL` | Kafka + справочник причин | setup_result, release_cause_code | string | Формула шага 3 |
| asr_pct | decimal(5,2) | ASR, проценты `0.00..100.00`; `NOT NULL` | Расчет | calls_connected, calls_total | bigint | Округление HALF_UP до 2 знаков |
| drop_rate_pct | decimal(5,2) | Drop Rate, проценты `0.00..100.00`; `NOT NULL` | Расчет | calls_dropped, calls_connected | bigint | 0.00 при нуле connected |
| mos_avg | decimal(3,2) | Средний MOS `1.00..5.00`; `NULLABLE` | Kafka | mos_avg | decimal(3,2) | NULL, если нет установленного вызова с MOS |
| low_mos_calls | bigint | Установленные вызовы с MOS < 3.50; `NOT NULL` | Kafka | mos_avg | decimal(3,2) | `0..calls_connected` |
| connected_duration_sec | bigint | Суммарная длительность установленных вызовов, секунды; `NOT NULL` | Расчет | call_start_time_utc, call_end_time_utc | long | Сумма `FLOOR((end-start)/1000)`, `>=0` |
| unknown_cell_calls | bigint | Вызовы без версии cell-справочника; `NOT NULL` | Расчет | serving_cell_id | string | `0..calls_total` |
| unknown_cause_calls | bigint | Вызовы с неизвестной причиной; `NOT NULL` | Расчет | release_cause_code | string | `0..calls_total` |
| source_max_end_time_utc | timestamp | Максимальное окончание учтенного вызова UTC; `NOT NULL` | Kafka | call_end_time_utc | long | Внутри окна |
| loaded_at_utc | timestamp | Время начала публикации batch UTC; `NOT NULL` | Система | batch_started_at | timestamp | Processing time |
| event_date_utc | date | Дата окна UTC; `NOT NULL` | Расчет | window_start_utc | timestamp | HDFS-партиция |
| event_hour_utc | smallint | Час окна UTC `0..23`; `NOT NULL` | Расчет | window_start_utc | timestamp | HDFS-партиция |

### Пример данных

| window_start_utc | region_code | calls_total | calls_connected | calls_dropped | asr_pct | drop_rate_pct | mos_avg | low_mos_calls | connected_duration_sec | unknown_cell_calls | unknown_cause_calls | source_max_end_time_utc | loaded_at_utc | event_date_utc | event_hour_utc |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- | :--- | :--- | ---: |
| 2026-08-18 09:00:00 | CENTER | 1000 | 950 | 19 | 95.00 | 2.00 | 4.12 | 44 | 142500 | 0 | 2 | 2026-08-18 09:04:59 | 2026-08-18 09:26:00 | 2026-08-18 | 9 |
| 2026-08-18 09:00:00 | NORTHWEST | 800 | 720 | 18 | 90.00 | 2.50 | 3.98 | 61 | 108000 | 1 | 0 | 2026-08-18 09:04:58 | 2026-08-18 09:26:00 | 2026-08-18 | 9 |
| 2026-08-18 09:00:00 | SOUTH | 500 | 400 | 20 | 80.00 | 5.00 | 3.44 | 95 | 58000 | 0 | 4 | 2026-08-18 09:04:57 | 2026-08-18 09:26:00 | 2026-08-18 | 9 |
| 2026-08-18 09:00:00 | VOLGA | 750 | 600 | 12 | 80.00 | 2.00 | 4.01 | 37 | 91200 | 0 | 0 | 2026-08-18 09:04:56 | 2026-08-18 09:26:00 | 2026-08-18 | 9 |
| 2026-08-18 09:00:00 | UNKNOWN | 20 | 10 | 0 | 50.00 | 0.00 | 3.50 | 4 | 800 | 20 | 1 | 2026-08-18 09:04:55 | 2026-08-18 09:26:00 | 2026-08-18 | 9 |
| 2026-08-18 09:05:00 | URAL | 600 | 570 | 0 | 95.00 | 0.00 | 4.25 | 15 | 85500 | 0 | 0 | 2026-08-18 09:09:59 | 2026-08-18 09:31:00 | 2026-08-18 | 9 |
| 2026-08-18 09:05:00 | SIBERIA | 400 | 300 | 15 | 75.00 | 5.00 | 3.20 | 88 | 42000 | 0 | 3 | 2026-08-18 09:09:58 | 2026-08-18 09:31:00 | 2026-08-18 | 9 |
| 2026-08-18 09:05:00 | FAR_EAST | 250 | 200 | 10 | 80.00 | 5.00 | 3.75 | 32 | 31000 | 0 | 0 | 2026-08-18 09:09:57 | 2026-08-18 09:31:00 | 2026-08-18 | 9 |
| 2026-08-18 09:05:00 | CENTER | 10 | 0 | 0 | 0.00 | 0.00 | NULL | 0 | 0 | 0 | 10 | 2026-08-18 09:09:56 | 2026-08-18 09:31:00 | 2026-08-18 | 9 |
| 2026-08-18 10:00:00 | NORTHWEST | 1250 | 1000 | 10 | 80.00 | 1.00 | 4.30 | 25 | 160000 | 0 | 0 | 2026-08-18 10:04:59 | 2026-08-18 10:26:00 | 2026-08-18 | 10 |

`NULL` в девятой строке является допустимым и ожидаемым: в группе нет ни одного установленного вызова с MOS.

### DDL

```sql
CREATE EXTERNAL TABLE prod_voice.NET_VOLTE_QUALITY_5M (
    window_start_utc           TIMESTAMP       NOT NULL,
    region_code                STRING          NOT NULL,
    calls_total                BIGINT          NOT NULL,
    calls_connected            BIGINT          NOT NULL,
    calls_dropped              BIGINT          NOT NULL,
    asr_pct                    DECIMAL(5,2)    NOT NULL,
    drop_rate_pct              DECIMAL(5,2)    NOT NULL,
    mos_avg                    DECIMAL(3,2),
    low_mos_calls              BIGINT          NOT NULL,
    connected_duration_sec     BIGINT          NOT NULL,
    unknown_cell_calls         BIGINT          NOT NULL,
    unknown_cause_calls        BIGINT          NOT NULL,
    source_max_end_time_utc    TIMESTAMP       NOT NULL,
    loaded_at_utc              TIMESTAMP       NOT NULL
)
PARTITIONED BY (
    event_date_utc             DATE            NOT NULL,
    event_hour_utc             SMALLINT        NOT NULL
)
STORED AS ORC
LOCATION '/data/prod/voice/volte_quality_5m/'
TBLPROPERTIES (
    'orc.compress'='ZLIB',
    'data.contract.version'='2'
);
```

### FAQ

Порог срабатывания, период сравнения и блокировка публикации определяются вручную после проверки.

После загрузки рассчитываются число дублей и расхождение с источником.

**В: Почему Drop Rate равен 0, когда нет установленных вызовов?**  
О: Это договоренный нейтральный результат для нулевого знаменателя; число connected всегда доступно рядом и позволяет отличить его от реально хорошего окна.

**В: Считается ли неизвестная причина обрывом?**  
О: Нет. Она входит в `unknown_cause_calls`, чтобы качество справочника было видно и не искажало аварийные завершения.

**В: Могут ли исправления изменить прошлое окно?**  
О: Да, в пределах 7 суток через hourly replay. Более старое исправление не применяется: авторитетного архива в скоупе продукта нет, случай эскалируется владельцу источника.

### Контроль качества и критерии приемки

- Ключ `(region_code, window_start_utc)` уникален; нарушение блокирует партицию.
- `calls_total > 0`; `0 <= calls_dropped <= calls_connected <= calls_total`; счетчики unknown и low MOS лежат в `0..calls_total` и `0..calls_connected` соответственно.
- ASR и Drop Rate пересчитываются из счетчиков; допустимое расхождение после HALF_UP — 0.00 процентного пункта.
- `mos_avg` либо NULL по описанному правилу, либо находится в `1.00..5.00`.
- Сумма `calls_total` по окну равна числу выбранных UPSERT после DELETE, фильтров и дедупликации; допустимое расхождение — 0.
- Время источника попадает в окно, partition date/hour совпадают с `window_start_utc` UTC.
- Batch с пустым входом имеет статус `NO_DATA` и не заменяет опубликованную непустую партицию.

### Изменение схемы и доступ

Версия 2 обратно совместима только при добавлении nullable-поля. Изменение формулы показателя, типа, enum или nullability требует новой major-версии продукта и полного пересчета сравниваемого периода. В результате нет MSISDN, IMSI и номера вызываемой стороны; `call_id` используется только внутри расчета и не публикуется. Чтение разрешено группам `VOICE_NOC_READ` и `VOICE_ANALYTICS`.

### История изменений

| Версия | Дата | Изменение | Автор |
| :--- | :--- | :--- | :--- |
| 2.0 | 2026-08-21 | Зафиксированы revision/DELETE, temporal join и правила нулевого знаменателя | Ирина Лебедева |
