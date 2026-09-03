from django.shortcuts import render

from .checker import check_document
from .forms import MarkdownUploadForm
from .models import HeadingRule


def upload_view(request):
    report = None
    filename = None

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
    else:
        form = MarkdownUploadForm()

    return render(
        request,
        "docchecker/upload.html",
        {
            "form": form,
            "report": report,
            "filename": filename,
        },
    )
