from concurrent.futures import ThreadPoolExecutor
import logging
import os
from pathlib import Path
import re
from secrets import choice

from django.db import close_old_connections
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.csrf import csrf_exempt
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
logger = logging.getLogger(__name__)
_REVIEW_EXECUTOR = ThreadPoolExecutor(
    max_workers=max(1, int(os.getenv("REVIEW_WORKERS", "2"))),
    thread_name_prefix="document-review",
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


class DocumentUploadError(ValueError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def _read_document(uploaded):
    if not uploaded:
        raise DocumentUploadError("Для проверки добавьте файлы.")
    if Path(uploaded.name).suffix.lower() not in ALLOWED_SUFFIXES:
        raise DocumentUploadError("Поддерживаются только файлы .md и .markdown.")
    if uploaded.size > MAX_DOCUMENT_BYTES:
        raise DocumentUploadError("Файл превышает лимит 2 МБ.", 413)
    try:
        document = uploaded.read().decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise DocumentUploadError("Файл должен быть в кодировке UTF-8.") from exc
    if not document.strip():
        raise DocumentUploadError("Файл пуст.")
    return document


def _model_for_slug(slug, *, use_default=False):
    models = OpenRouterModel.objects.filter(enabled=True)
    if slug:
        return models.filter(slug=slug).first()
    if use_default:
        return models.filter(slug=MODEL).first() or models.first()
    return None


def _execute_review(review, available):
    review.status = Review.Status.RUNNING
    review.save(update_fields=["status"])
    try:
        findings = run_review(
            review.document,
            available.slug,
            max_tokens=available.max_tokens,
            reasoning_effort=available.reasoning_effort,
            no_reasoning=available.no_reasoning,
            temperature=available.temperature,
            no_temperature=available.no_temperature,
            provider=available.provider,
        )
    except Exception as exc:
        if isinstance(exc, ReviewError):
            logger.warning("Review %s failed: %s", review.public_id, exc)
        else:
            logger.exception("Unexpected error while processing review %s", review.public_id)
        review.error = str(exc)
        review.status = Review.Status.FAILED
        review.save(update_fields=["error", "status"])
        return None

    findings = sorted(findings, key=_finding_sort_key)
    review.findings = findings
    review.status = Review.Status.COMPLETED
    review.save(update_fields=["findings", "status"])
    ReviewFinding.objects.bulk_create([
        ReviewFinding(
            review=review,
            error_type_id=str(item.get("error_type_id", "O")),
            result=item,
        )
        for item in findings
    ])
    return findings


def _run_and_save_review(document, document_name, available):
    review = Review.objects.create(
        document_name=Path(document_name).name[:255],
        document=document,
        model=available.slug,
        status=Review.Status.PENDING,
    )
    return review, _execute_review(review, available)


def _process_review(review_id):
    close_old_connections()
    try:
        review = Review.objects.get(pk=review_id)
        available = OpenRouterModel.objects.get(slug=review.model)
        _execute_review(review, available)
    except Exception as exc:
        logger.exception("Could not start background review %s", review_id)
        Review.objects.filter(pk=review_id).update(
            status=Review.Status.FAILED,
            error=str(exc),
        )
    finally:
        close_old_connections()


def _submit_review_job(review_id):
    return _REVIEW_EXECUTOR.submit(_process_review, review_id)


def _report_context(review):
    enabled = ["D", "T", "U", "OTHER"]
    display_findings = [
        {
            **item,
            "family": (
                item.get("error_type_id", "")[:1]
                if item.get("error_type_id", "")[:1] in {"D", "T", "U"}
                else "OTHER"
            ),
        }
        for item in review.findings
    ]
    finding_highlights = [
        {
            "index": index,
            "family": item["family"],
            "terms": highlight_terms(item.get("evidence_quote", "")),
            "groups": highlight_groups(item.get("evidence_quote", "")),
        }
        for index, item in enumerate(display_findings)
    ]
    family_counts = {
        family: sum(item["family"] == family for item in display_findings)
        for family in enabled
    }
    return {
        "report_uuid": review.public_id,
        "document_name": review.document_name,
        "model": review.model,
        "predictions": display_findings,
        "highlighted_document": highlighted_source(review.document, display_findings),
        "rendered_document": rendered_markdown(review.document, display_findings),
        "finding_highlights": finding_highlights,
        "family_counts": family_counts,
        "primary_count": family_counts["D"] + family_counts["T"],
        "criteria_count": sum({"D": 8, "T": 1, "U": 19, "OTHER": 1}[family] for family in enabled),
    }


def _report_urls(request, review):
    page_path = reverse("reviewapp:report_detail", kwargs={"report_id": review.public_id})
    api_path = reverse("reviewapp:report_api_detail", kwargs={"report_id": review.public_id})
    return request.build_absolute_uri(page_path), request.build_absolute_uri(api_path)


def _report_payload(request, review):
    page_url, api_url = _report_urls(request, review)
    payload = {
        "id": str(review.public_id),
        "status": review.status,
        "document_name": review.document_name,
        "model": review.model,
        "created_at": review.created_at.isoformat(),
        "findings": review.findings,
        "findings_count": len(review.findings),
        "report_url": page_url,
        "api_url": api_url,
    }
    if review.status == Review.Status.FAILED:
        payload["message"] = choice(CHECK_ERROR_MESSAGES)
    return payload


@require_http_methods(["GET", "POST"])
def upload(request):
    if request.method == "GET":
        return render(request, "reviewapp/upload.html", _upload_context())

    try:
        uploaded = request.FILES.get("document")
        document = _read_document(uploaded)
    except DocumentUploadError as exc:
        return render(request, "reviewapp/upload.html", _upload_context(str(exc)))

    requested_model = request.POST.get("model", "").strip()
    available = _model_for_slug(requested_model)
    if not available:
        return render(request, "reviewapp/upload.html", _upload_context("Выберите доступную модель OpenRouter."))
    review, findings = _run_and_save_review(document, uploaded.name, available)
    if findings is None:
        return render(request, "reviewapp/upload.html", _upload_context(choice(CHECK_ERROR_MESSAGES)))
    response = render(request, "reviewapp/report.html", _report_context(review))
    response["X-Report-URL"] = reverse(
        "reviewapp:report_detail", kwargs={"report_id": review.public_id}
    )
    return response


@require_http_methods(["GET"])
def report_detail(request, report_id):
    review = get_object_or_404(Review, public_id=report_id)
    if review.status == Review.Status.FAILED:
        return render(request, "reviewapp/report_error.html", {
            "report_uuid": review.public_id,
            "document_name": review.document_name,
            "message": "Этот отчёт не удалось сформировать. Исходная ошибка сохранена для диагностики.",
        })
    if review.status in {Review.Status.PENDING, Review.Status.RUNNING}:
        return render(request, "reviewapp/report_pending.html", {
            "report_uuid": review.public_id,
            "document_name": review.document_name,
            "api_url": reverse("reviewapp:report_api_detail", kwargs={"report_id": review.public_id}),
        })
    return render(request, "reviewapp/report.html", _report_context(review))


@csrf_exempt
@require_http_methods(["POST"])
def reports_api(request):
    try:
        uploaded = request.FILES.get("document")
        document = _read_document(uploaded)
    except DocumentUploadError as exc:
        return JsonResponse({"error": str(exc)}, status=exc.status)

    available = _model_for_slug(request.POST.get("model", "").strip(), use_default=True)
    if not available:
        return JsonResponse({"error": "Нет доступной модели OpenRouter."}, status=400)

    review = Review.objects.create(
        document_name=Path(uploaded.name).name[:255],
        document=document,
        model=available.slug,
        status=Review.Status.PENDING,
    )
    try:
        _submit_review_job(review.pk)
    except Exception as exc:
        logger.exception("Could not enqueue review %s", review.public_id)
        review.status = Review.Status.FAILED
        review.error = str(exc)
        review.save(update_fields=["status", "error"])
    payload = _report_payload(request, review)
    response = JsonResponse(payload, status=202 if review.status == Review.Status.PENDING else 503)
    response["Cache-Control"] = "no-store"
    return response


@require_http_methods(["GET"])
def report_api_detail(request, report_id):
    try:
        review = Review.objects.get(public_id=report_id)
    except Review.DoesNotExist:
        return JsonResponse({"error": "Отчёт не найден."}, status=404)
    response = JsonResponse(_report_payload(request, review))
    response["Cache-Control"] = "no-store"
    return response
