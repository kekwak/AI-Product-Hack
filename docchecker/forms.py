from django import forms


class MarkdownUploadForm(forms.Form):
    file = forms.FileField(
        label="Markdown-файл (.md)",
        help_text="Выберите файл документации в формате .md",
    )

    def clean_file(self):
        uploaded = self.cleaned_data["file"]
        if not uploaded.name.lower().endswith(".md"):
            raise forms.ValidationError("Файл должен иметь расширение .md")
        if uploaded.size > 2 * 1024 * 1024:
            raise forms.ValidationError("Файл слишком большой (максимум 2 МБ)")
        return uploaded
