# AI-Product-Hack

- [Синтетический датасет](dataset/README.md): 10 чистых ТЗ и 50 размеченных кейсов.
- [LLM-судья](docs/evaluation.md): matching предсказаний через OpenRouter и расчет TP/FP/FN, precision, recall и F1.

Код проекта находится непосредственно в `src/`, тесты — в `tests/`, примеры входа — в `examples/`.

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
├── src/
│   ├── generate_dataset.py
│   ├── evaluate_predictions.py
│   └── paths.py
├── dataset/
│   ├── clean/            # 10 эталонов
│   ├── corrupted/        # 50 MD + 50 JSON ground truth
│   ├── error_catalog.json
│   └── manifest.json
├── tests/
│   ├── test_dataset.py
│   └── test_judge.py
├── examples/predictions/
├── docs/
└── case/                 # исходные материалы кейсодателя
```
