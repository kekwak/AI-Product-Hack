# AI Product Hack — проверка технических заданий

Проект ищет ошибки в документации потоков и витрин данных, а затем измеряет качество поиска на синтетическом датасете.

Пайплайн состоит из трех частей:

1. `dataset-generate` создает чистые и поврежденные документы с ground truth.
2. `infer-documents` проверяет Markdown-документы моделью через OpenRouter и сохраняет найденные ошибки.
3. `evaluate-predictions` сопоставляет предсказания с ground truth и считает `TP`, `FP`, `FN`, precision, recall и F1.
4. `optimize-prompt` улучшает инструктивную часть промпта с помощью GEPA.

## Быстрый старт

Нужны Python 3.12+, [uv](https://docs.astral.sh/uv/) и ключ OpenRouter.

```bash
uv sync
export OPENROUTER_API_KEY='<ваш_ключ>'
```

Запустить инференс на первых 10 документах:

```bash
uv run infer-documents \
  --limit 10 \
  --concurrency 8 \
  --output artifacts/predictions/minimax_m3_first10
```

Оценить эти же документы:

```bash
uv run evaluate-predictions \
  --predictions artifacts/predictions/minimax_m3_first10 \
  --limit 10 \
  --workers 8 \
  --output artifacts/evaluation/minimax_m3_first10.json
```

## Структура проекта

## Веб-приложение

Django-интерфейс находится в `backend/`. Он принимает `.md`/`.markdown` до 2 МБ,
отправляет текст в OpenRouter, показывает найденные ошибки с подсветкой цитат и
сохраняет историю проверок. Пользователь выбирает модель из разрешённого
администратором списка. Django передаёт в inference только содержимое документа
и выбранную модель. Промпт, JSON-схема и полный набор проверок находятся в
`src/run_inference.py`; пользовательские лимиты и отдельные режимы pipeline не
используются.

Доступные пользователю модели управляются отдельным справочником «Модели
OpenRouter» в админке. Для каждой модели задаются понятное название, точный slug
OpenRouter, доступность и порядок в списке. На форме загрузки отображаются только
включённые модели; выбранный slug валидируется сервером перед API-вызовом.

```bash
uv sync
cp backend/.env.example backend/.env  # затем задайте OPENROUTER_API_KEY
uv run python backend/manage.py migrate
uv run python backend/manage.py createsuperuser
uv run python backend/manage.py runserver
```

Откройте `http://127.0.0.1:8000/`, админку — на `/admin/`.
Для production задайте случайный `DJANGO_SECRET_KEY`, `DJANGO_DEBUG=0`, корректный
`DJANGO_ALLOWED_HOSTS` и запускайте приложение через production WSGI/ASGI-сервер.

Проверки без обращения к API:

```bash
uv run python backend/manage.py check
uv run python backend/manage.py test reviewapp
```

## Запуск в Docker

Создайте файл с настройками и обязательно замените API-ключ и секрет Django:

```bash
cp .env.docker.example .env
docker compose up --build -d
```

После запуска приложение доступно на `http://localhost:8000/`, админка — на
`http://localhost:8000/admin/`. Миграции и сборка статических файлов выполняются
автоматически. SQLite хранится в Docker-volume `app_data` и не пропадает при
пересоздании контейнера.

Создание администратора:

```bash
docker compose exec web python backend/manage.py createsuperuser
```

Просмотр логов и остановка:

```bash
docker compose logs -f web
docker compose down
```

Команда `docker compose down -v` дополнительно удаляет базу данных, поэтому
используйте флаг `-v` только если сохранённая история и учётные записи больше не
нужны. Для публичного сервера укажите домен в `DJANGO_ALLOWED_HOSTS` и адрес с
протоколом `https://` в `DJANGO_CSRF_TRUSTED_ORIGINS`.

```text
.
├── case/                       # исходные материалы и критерии кейсодателя
├── dataset/
│   ├── clean/                  # 10 чистых документов
│   ├── corrupted/              # 50 пар: case_XXX.md + ground truth JSON
│   ├── train/                  # symlink-ссылки: 6 clean + 30 cases
│   ├── val/                    # symlink-ссылки: 2 clean + 10 cases
│   ├── test/                   # symlink-ссылки: 2 clean + 10 cases
│   ├── error_catalog.json      # каталог вариантов ошибок
│   └── manifest.json           # распределение ошибок по кейсам
├── src/
│   ├── generate_dataset.py     # генератор датасета
│   ├── run_inference.py        # LangGraph-инференсер
│   ├── evaluate_predictions.py # LLM-судья и метрики
│   ├── optimize_prompt.py       # GEPA-оптимизация промпта
│   └── paths.py                # пути проекта
├── tests/                      # тесты датасета, инференса и судьи
├── artifacts/                  # локальные предсказания и отчеты
└── examples/                   # примеры файлов предсказаний
```

Подробное описание синтетического набора находится в [dataset/README.md](dataset/README.md).

## Какие ошибки ищет модель

Всего используется 28 классов:

- `D01–D08` — обязательные доменные требования;
- `T01` — несоответствие обязательному шаблону;
- `U01–U19` — логика, полнота, согласованность и проверяемость.

Полный inference-промпт вместе с шаблоном и описанием всех ошибок явно записан в `FULL_PROMPT` файла `src/run_inference.py`. Во время запуска критерии из других файлов не подгружаются.

## Оптимизация промпта

GEPA изменяет только инструкции модели. Встроенные определения D01–D08, T01 и U01–U19 остаются неизменными. Одна итерация равна одной предложенной мутации промпта.

```bash
uv run optimize-prompt \
  --iterations 10 \
  --concurrency 4 \
  --output artifacts/gepa/luna_run_01
```

Используется `openai/gpt-5.6-luna:nitro` без параметра temperature для инференса, LLM-судьи и рефлексии. Датасет делится по исходным clean-документам: 01–06 для train, 07–08 для validation, 09–10 для test. Целевая метрика — macro F2: recall имеет в четыре раза больший вес, чем precision. F1 также сохраняется в отчете. Лучшие инструкции сохраняются в `best_prompt.md`, история validation и итоговые test-метрики — в `metrics.json`.

Запустить обычный инференс с найденным промптом:

```bash
uv run infer-documents \
  --model openai/gpt-5.6-luna:nitro \
  --no-temperature \
  --prompt-file artifacts/gepa/luna_run_01/best_prompt.md \
  --output artifacts/predictions/luna_gepa
```

## Инференс

По умолчанию используется модель `minimax/minimax-m3`. На каждый документ выполняется один LangGraph-вызов со structured output. Документы обрабатываются асинхронно; одновременно выполняется до восьми запросов.

Весь датасет:

```bash
uv run infer-documents \
  --output artifacts/predictions/minimax_m3 \
  --concurrency 8
```

Один кейс:

```bash
uv run infer-documents \
  --case case_001 \
  --output artifacts/predictions/minimax_m3
```

Несколько кейсов:

```bash
uv run infer-documents \
  --case case_001 \
  --case case_002 \
  --output artifacts/predictions/minimax_m3
```

Параметры:

- `--input` — каталог входных `.md`-документов;
- `--output` — каталог для JSON-предсказаний;
- `--limit N` — взять первые `N` документов;
- `--case case_XXX` — выбрать конкретный кейс, флаг можно повторять;
- `--concurrency N` — число параллельных запросов, по умолчанию `8`;
- `--temperature N` — температура модели, по умолчанию `0.0`;
- `--no-temperature` — совсем не отправлять параметр `temperature` модели;
- `--reasoning-effort` — бюджет reasoning, по умолчанию `high`;
- `--no-reasoning` — отключить reasoning и оставить весь output-бюджет финальному JSON;
- `--retries N` — число повторов после первой попытки; по умолчанию `2`, то есть не более 3 попыток всего;
- `--model MODEL` — модель OpenRouter.

Модель также можно задать переменной окружения:

```bash
export INFERENCE_MODEL='minimax/minimax-m3'
```

Каждый результат сохраняется как `case_XXX.json`:

```json
[
  {
    "error_type_id": "D02",
    "evidence_quote": "Data Catalog: ссылка отсутствует",
    "title": "Нет ссылки на Data Catalog",
    "problem": "Источник нельзя однозначно идентифицировать."
  }
]
```

## Оценка предсказаний

Судья получает предсказания и ground truth одного документа и возвращает пары совпавших ошибок. Совпадение определяется по месту и корневой проблеме; одинаковый `error_type_id` не обязателен. Если модель предложила пересекающиеся пары, код выбирает максимальное one-to-one сопоставление, чтобы не раздувать `TP`.

Цитата используется как подсказка. Отсутствующая или неуникальная цитата не становится автоматическим `FP`. Отсутствующий файл предсказаний считается пустым: все ошибки соответствующего кейса становятся `FN`.

Один кейс:

```bash
uv run evaluate-predictions \
  --predictions artifacts/predictions/minimax_m3 \
  --case case_001 \
  --output artifacts/evaluation/case_001.json
```

Весь датасет:

```bash
uv run evaluate-predictions \
  --predictions artifacts/predictions/minimax_m3 \
  --workers 8 \
  --output artifacts/evaluation/minimax_m3.json
```

Параметры:

- `--ground-truth` — каталог с ground truth, по умолчанию `dataset/corrupted`;
- `--case` и `--limit` — выбор кейсов;
- `--workers` — число параллельных запросов;
- `--model` — модель OpenRouter для judge;
- `--temperature N` — температура judge, по умолчанию `0.0`;
- `--no-temperature` — совсем не отправлять параметр `temperature` judge-модели;
- `--timeout` — таймаут одного запроса в секундах;
- `--retries` — число контролируемых повторов каждого неуспешного запроса;
- `--no-cache` — не использовать сохраненные ответы судьи.

Модель judge можно задать отдельно:

```bash
export OPENROUTER_MODEL='minimax/minimax-m3'
```

Итоговый отчет содержит общие и покейсовые метрики, результаты по классам ошибок, найденные пары, `FP`, `FN`, токены и стоимость вызовов. Сетевой сбой одного кейса не останавливает остальные: после исчерпания повторов он попадает в `failed_cases`, а частичный отчет сохраняется.

## Weights & Biases

W&B включается только флагом `--wandb-project`. Перед первым запуском выполните `wandb login` или задайте `WANDB_API_KEY`.

Инференс и оценку одного эксперимента связывайте одинаковым `--wandb-group`:

```bash
uv run infer-documents \
  --input dataset/val \
  --model google/gemma-4-31b-it:nitro \
  --output artifacts/predictions/gemma_4_31b_nitro_val \
  --wandb-project ai-product-hack \
  --wandb-group gemma-4-val \
  --wandb-run-name gemma-4-val-inference

uv run evaluate-predictions \
  --ground-truth dataset/val \
  --predictions artifacts/predictions/gemma_4_31b_nitro_val \
  --model openai/gpt-5.6-luna:nitro \
  --no-temperature \
  --output artifacts/evaluation/gemma_4_31b_nitro_val.json \
  --wandb-project ai-product-hack \
  --wandb-group gemma-4-val \
  --wandb-run-name gemma-4-val-evaluation
```

Inference-run логирует конфигурацию, число обработанных документов и artifact с предсказаниями. Evaluation-run логирует micro/macro-метрики, таблицу по типам ошибок и artifact с полным JSON-отчетом.

## Генерация датасета

Датасет уже находится в репозитории. Для полного воспроизведения:

```bash
uv run dataset-generate
```

Команда заново формирует содержимое `dataset/clean`, `dataset/corrupted`, каталог ошибок и manifest. Не запускайте ее поверх ручных изменений датасета, которые нужно сохранить.

## Тесты

```bash
uv run python -m unittest discover -s tests -v
```

Тесты проверяют структуру и воспроизводимость датасета, формат inference-ответа, LangGraph-пайплайн, matching и расчет метрик.

Справка по любой команде:

```bash
uv run infer-documents --help
uv run evaluate-predictions --help
```
