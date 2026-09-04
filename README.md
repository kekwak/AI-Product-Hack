# AI-Product-Hack

- [Синтетический датасет](dataset/README.md): 10 чистых ТЗ и 50 размеченных кейсов.
- [LLM-судья](docs/evaluation.md): matching предсказаний через OpenRouter и расчет TP/FP/FN, precision, recall и F1.

Код проекта находится непосредственно в `src/`, тесты — в `tests/`, примеры входа — в `examples/`.

## Веб-приложение

Django-интерфейс находится в `backend/`. Он принимает `.md`/`.markdown` до 2 МБ,
отправляет текст в OpenRouter, показывает найденные ошибки с подсветкой цитат и
сохраняет историю проверок. Модель, лимит результатов и доступность групп D/T/U
и лимит токенов ответа настраиваются в `/admin/`; если это разрешено администратором, модель и фильтры
можно выбрать и на форме загрузки.

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
