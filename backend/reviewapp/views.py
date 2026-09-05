from pathlib import Path

from django.shortcuts import render
from django.views.decorators.http import require_http_methods
from run_inference import MODEL

from .models import OpenRouterModel, Review, ReviewFinding
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, run_review

MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
ALLOWED_SUFFIXES = {".md", ".markdown"}


def _upload_context(error=""):
    models = list(OpenRouterModel.objects.filter(enabled=True))
    return {"error": error, "available_models": models, "default_model": MODEL}


@require_http_methods(["GET", "POST"])
def upload(request):
    if request.method == "GET":
        return render(request, "reviewapp/upload.html", _upload_context())

    uploaded = request.FILES.get("document")
    if not uploaded:
        return render(request, "reviewapp/upload.html", _upload_context("Выберите Markdown-файл."))
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
        return render(request, "reviewapp/upload.html", _upload_context(f"Не удалось выполнить проверку: {exc}"))
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
        {"index": index, "family": item["family"], "terms": highlight_terms(item["evidence_quote"])}
        for index, item in enumerate(display_findings)
    ]
    family_counts = {family: sum(item["family"] == family for item in display_findings) for family in enabled}
    criteria_count = sum({"D": 8, "T": 1, "U": 19, "OTHER": 1}[family] for family in enabled)
    return render(request, "reviewapp/report.html", {
        "document_name": review.document_name, "model": model,
        "predictions": display_findings, "highlighted_document": highlighted_source(document, display_findings),
        "rendered_document": rendered_markdown(document, display_findings),
        "finding_highlights": finding_highlights,
        "family_counts": family_counts,
        "criteria_count": criteria_count,
    })
