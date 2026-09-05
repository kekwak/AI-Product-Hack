from pathlib import Path

from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from .models import OpenRouterModel, Review, ReviewSettings
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, run_review

MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
ALLOWED_SUFFIXES = {".md", ".markdown"}


def _upload_context(config, error=""):
    models = list(OpenRouterModel.objects.filter(enabled=True))
    return {"config": config, "error": error, "available_models": models}


@require_http_methods(["GET", "POST"])
def upload(request):
    config = ReviewSettings.load()
    if request.method == "GET":
        return render(request, "reviewapp/upload.html", _upload_context(config))

    uploaded = request.FILES.get("document")
    if not uploaded:
        return render(request, "reviewapp/upload.html", _upload_context(config, "Выберите Markdown-файл."))
    if Path(uploaded.name).suffix.lower() not in ALLOWED_SUFFIXES:
        return render(request, "reviewapp/upload.html", _upload_context(config, "Поддерживаются только файлы .md и .markdown."))
    if uploaded.size > MAX_DOCUMENT_BYTES:
        return render(request, "reviewapp/upload.html", _upload_context(config, "Файл превышает лимит 2 МБ."))
    try:
        document = uploaded.read().decode("utf-8-sig")
    except UnicodeDecodeError:
        return render(request, "reviewapp/upload.html", _upload_context(config, "Файл должен быть в кодировке UTF-8."))
    if not document.strip():
        return render(request, "reviewapp/upload.html", _upload_context(config, "Файл пуст."))

    enabled = [family for family in ("D", "T", "U") if getattr(config, f"enabled_{family.lower()}")]
    if not enabled:
        return render(request, "reviewapp/upload.html", _upload_context(config, "Администратор отключил все группы проверок."))
    if config.allow_client_model:
        requested_model = request.POST.get("model", "").strip()
        available = OpenRouterModel.objects.filter(enabled=True, slug=requested_model).first()
        if not available:
            return render(request, "reviewapp/upload.html", _upload_context(config, "Выберите доступную модель OpenRouter."))
        model = available.slug
    else:
        model = config.model
    review = Review.objects.create(
        document_name=Path(uploaded.name).name[:255], model=model,
        pipeline_mode=config.pipeline_mode, filters=enabled,
    )
    try:
        findings = run_review(
            document,
            model,
            enabled,
            config.max_findings,
            config.max_output_tokens,
            config.reasoning_effort,
            config.pipeline_mode,
        )
    except ReviewError as exc:
        review.error = str(exc)
        review.save(update_fields=["error"])
        return render(request, "reviewapp/upload.html", _upload_context(config, f"Не удалось выполнить проверку: {exc}"))
    review.findings = findings
    review.save(update_fields=["findings"])
    finding_highlights = [
        {"index": index, "family": item["family"], "terms": highlight_terms(item["evidence_quote"])}
        for index, item in enumerate(findings)
    ]
    family_counts = {family: sum(item["family"] == family for item in findings) for family in enabled}
    criteria_count = sum({"D": 8, "T": 1, "U": 19}[family] for family in enabled)
    return render(request, "reviewapp/report.html", {
        "document_name": review.document_name, "model": model, "filters": enabled,
        "pipeline_mode": config.pipeline_mode,
        "predictions": findings, "highlighted_document": highlighted_source(document, findings),
        "rendered_document": rendered_markdown(document, findings),
        "finding_highlights": finding_highlights,
        "family_counts": family_counts,
        "criteria_count": criteria_count,
    })
