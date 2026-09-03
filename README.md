# AI-Product-Hack

- [Синтетический датасет](dataset/README.md): 10 чистых ТЗ и 50 размеченных кейсов.
- [LLM-судья](docs/evaluation.md): matching предсказаний через OpenRouter и расчет TP/FP/FN, precision, recall и F1.

Код проекта находится непосредственно в `src/`, тесты — в `tests/`, примеры входа — в `examples/`.

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
