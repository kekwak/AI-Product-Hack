import base64

from django.http import HttpResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST

from .checker import build_annotated_markdown, check_document
from .forms import MarkdownUploadForm
from .models import HeadingRule


def upload_view(request):
    report = None
    filename = None
    content_b64 = None

    if request.method == "POST":
        form = MarkdownUploadForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded = form.cleaned_data["file"]
            filename = uploaded.name
            raw = uploaded.read()
            try:
                text = raw.decode("utf-8")
            except UnicodeDecodeError:
                text = raw.decode("utf-8", errors="replace")

            rules = HeadingRule.objects.filter(is_active=True)
            report = check_document(text, rules)
            content_b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
    else:
        form = MarkdownUploadForm()

    return render(
        request,
        "docchecker/upload.html",
        {
            "form": form,
            "report": report,
            "filename": filename,
            "content_b64": content_b64,
        },
    )


@require_POST
def download_view(request):
    content_b64 = request.POST.get("content_b64")
    filename = request.POST.get("filename") or "document.md"
    if not content_b64:
        return HttpResponseBadRequest("Нет содержимого файла для формирования отчёта.")

    try:
        text = base64.b64decode(content_b64).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return HttpResponseBadRequest("Не удалось прочитать содержимое файла.")

    rules = HeadingRule.objects.filter(is_active=True)
    report = check_document(text, rules)
    annotated = build_annotated_markdown(text, report)

    base_name = filename.rsplit(".", 1)[0]
    download_name = f"{base_name}_checked.md"

    response = HttpResponse(annotated, content_type="text/markdown; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="{download_name}"'
    return response
