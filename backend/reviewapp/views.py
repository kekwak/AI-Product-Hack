from pathlib import Path
import re
from secrets import choice

from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from run_inference import MODEL

from .models import OpenRouterModel, Review, ReviewFinding
from .rendering import highlight_groups, highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, run_review

MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
ALLOWED_SUFFIXES = {".md", ".markdown"}
CHECK_ERROR_MESSAGES = (
    "Упс, что-то пошло не так. Попробуйте ещё раз — со второго раза магия обычно послушнее",
    "Кажется, нейросеть споткнулась о собственные токены. Давайте повторим проверку",
    "Наш цифровой ревьюер взял незапланированный кофе-брейк. Попробуйте ещё раз через минуту",
    "Документ оказался достойным соперником. Перезапустим проверку и возьмём реванш",
    "Проверка ушла не по плану, зато ошибка уже записана. Можно спокойно попробовать снова",
    "Где-то потерялся один очень важный нолик. Повторите попытку — мы уже ищем его",
    "Сервер сказал «ой». Мы сохранили подробности, а вы можете запустить проверку ещё раз",
    "Токены немного запутались в проводах. Ещё одна попытка должна всё распутать",
)


def _finding_sort_key(item):
    error_id = str(item.get("error_type_id", "")).upper()
    match = re.fullmatch(r"([DTU])(\d+)", error_id)
    if match:
        family, number = match.groups()
        return {"D": 0, "T": 1, "U": 2}[family], int(number), error_id
    return 3, 0, error_id


def _upload_context(error=""):
    models = list(OpenRouterModel.objects.filter(enabled=True))
    return {"error": error, "available_models": models, "default_model": MODEL}


@require_http_methods(["GET", "POST"])
def upload(request):
    if request.method == "GET":
        return render(request, "reviewapp/upload.html", _upload_context())

    uploaded = request.FILES.get("document")
    if not uploaded:
        return render(request, "reviewapp/upload.html", _upload_context("Для проверки добавьте файлы."))
    if Path(uploaded.name).suffix.lower() not in ALLOWED_SUFFIXES:
        return render(request, "reviewapp/upload.html", _upload_context("Поддерживаются только файлы .md и .markdown."))
    if uploaded.size > MAX_DOCUMENT_BYTES:
        return render(request, "reviewapp/upload.html", _upload_context("Файл превышает лимит 2 МБ."))
    try:
        document = uploaded.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return render(request, "reviewapp/upload.html", _upload_context("Файл должен быть в кодировке UTF-8."))
    if not document.strip():
        return render(request, "reviewapp/upload.html", _upload_context("Файл пуст."))

    enabled = ["D", "T", "U", "OTHER"]
    requested_model = request.POST.get("model", "").strip()
    available = OpenRouterModel.objects.filter(enabled=True, slug=requested_model).first()
    if not available:
        return render(request, "reviewapp/upload.html", _upload_context("Выберите доступную модель OpenRouter."))
    model = available.slug
    review = Review.objects.create(document_name=Path(uploaded.name).name[:255], model=model)
    try:
        findings = run_review(
            document,
            model,
            max_tokens=available.max_tokens,
            reasoning_effort=available.reasoning_effort,
            no_reasoning=available.no_reasoning,
            temperature=available.temperature,
            no_temperature=available.no_temperature,
            provider=available.provider,
        )
    except ReviewError as exc:
        review.error = str(exc)
        review.save(update_fields=["error"])
        return render(request, "reviewapp/upload.html", _upload_context(choice(CHECK_ERROR_MESSAGES)))
    findings = sorted(findings, key=_finding_sort_key)
    review.findings = findings
    review.save(update_fields=["findings"])
    ReviewFinding.objects.bulk_create([
        ReviewFinding(
            review=review,
            error_type_id=str(item.get("error_type_id", "O")),
            result=item,
        )
        for item in findings
    ])
    display_findings = [
        {
            **item,
            "family": (
                item.get("error_type_id", "")[:1]
                if item.get("error_type_id", "")[:1] in {"D", "T", "U"}
                else "OTHER"
            ),
        }
        for item in findings
    ]
    finding_highlights = [
        {
            "index": index,
            "family": item["family"],
            "terms": highlight_terms(item["evidence_quote"]),
            "groups": highlight_groups(item["evidence_quote"]),
        }
        for index, item in enumerate(display_findings)
    ]
    family_counts = {family: sum(item["family"] == family for item in display_findings) for family in enabled}
    primary_count = family_counts["D"] + family_counts["T"]
    criteria_count = sum({"D": 8, "T": 1, "U": 19, "OTHER": 1}[family] for family in enabled)
    return render(request, "reviewapp/report.html", {
        "document_name": review.document_name, "model": model,
        "predictions": display_findings, "highlighted_document": highlighted_source(document, display_findings),
        "rendered_document": rendered_markdown(document, display_findings),
        "finding_highlights": finding_highlights,
        "family_counts": family_counts,
        "primary_count": primary_count,
        "criteria_count": criteria_count,
    })
