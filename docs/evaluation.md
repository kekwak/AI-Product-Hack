# LLM-судья для матчинга ошибок

CLI `evaluate-predictions` сопоставляет предсказания модели с ground truth из `dataset/corrupted/*.json` и считает строгие `TP`, `FP`, `FN`, precision, recall и F1.

## Формат предсказаний

В каталоге predictions нужен одноименный JSON для каждого кейса: `case_001.json`, …, `case_050.json`. Допустим непосредственно массив или объект с полем `predictions`:

```json
[
  {
    "error_type_id": "D06",
    "evidence_quote": "Kafka; кластер не указан",
    "title": "Не указан Kafka-кластер",
    "problem": "Источник нельзя однозначно идентифицировать без имени кластера."
  }
]
```

Отсутствующий файл считается пустым предсказанием: все эталонные ошибки кейса станут `FN`.

## Правило матчинга

Код до вызова LLM применяет жесткие ограничения:

- `error_type_id` должен совпасть;
- `evidence_quote` предсказания должна встречаться в документе ровно один раз;
- matching строго one-to-one: одно предсказание не закрывает несколько ошибок, дубликаты становятся `FP`.

В LLM передаются только оставшиеся кандидаты одинакового типа. Судья сопоставляет место и корневую причину по `title`, `problem`, цитате и небольшому контексту. Он возвращает только пары индексов; `TP/FP/FN` считает код.

## Запуск

Сначала проверить вход без сетевых вызовов:

```bash
uv run evaluate-predictions \
  --predictions path/to/predictions \
  --dry-run
```

Полная оценка:

```bash
export OPENROUTER_API_KEY='<key>'
# Необязательно: по умолчанию используется z-ai/glm-5.3-flash.
export OPENROUTER_MODEL='<model-slug-with-structured-outputs>'

uv run evaluate-predictions \
  --predictions path/to/predictions \
  --output artifacts/evaluation/run_001.json \
  --workers 4
```

Для калибровки удобно начать с `--case case_001` или `--limit 3`. Ответы судьи кэшируются рядом с отчетом; `--no-cache` принудительно вызывает модель заново.

Модель должна поддерживать structured outputs. Скрипт использует [`ChatOpenRouter`](https://docs.langchain.com/oss/python/integrations/chat/openrouter) из `langchain-openrouter`, Pydantic-схему и `provider.require_parameters=true`.

Во внешний API отправляются четыре поля находки и небольшой контекст вокруг цитаты, а не полный документ. Для чувствительных ТЗ нужна разрешенная корпоративная конфигурация OpenRouter либо замена клиента на локальный совместимый endpoint.

## Результат

Отчет содержит:

- micro `TP/FP/FN`, precision, recall, F1 и macro-метрики по документам;
- exact accuracy по документам;
- метрики по каждому `error_type_id`;
- число API-вызовов, токены и стоимость, возвращенную OpenRouter;
- для каждого кейса: подтвержденные пары, все `FP`, все `FN` и технический audit вызова OpenRouter.

## Локальные тесты

```bash
uv run python -m unittest discover -s tests -v
```
