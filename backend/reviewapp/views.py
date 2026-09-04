from pathlib import Path

from django.shortcuts import render
from django.views.decorators.http import require_http_methods

from .models import Review, ReviewSettings
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, run_review

MAX_DOCUMENT_BYTES = 2 * 1024 * 1024
ALLOWED_SUFFIXES = {".md", ".markdown"}


def _upload_context(config, error=""):
    return {"config": config, "error": error}


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
    model = request.POST.get("model", "").strip() if config.allow_client_model else config.model
    model = model or config.model
    review = Review.objects.create(document_name=Path(uploaded.name).name[:255], model=model, filters=enabled)
    try:
        findings = run_review(
            document,
            model,
            enabled,
            config.max_findings,
            config.max_output_tokens,
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
    return render(request, "reviewapp/report.html", {
        "document_name": review.document_name, "model": model, "filters": enabled,
        "predictions": findings, "highlighted_document": highlighted_source(document, findings),
        "rendered_document": rendered_markdown(document, findings),
        "finding_highlights": finding_highlights,
    })
