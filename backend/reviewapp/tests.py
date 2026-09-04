from io import BytesIO
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from .models import Review, ReviewSettings
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, run_review


class UploadTests(TestCase):
    def test_get_creates_settings_and_renders_form(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(ReviewSettings.objects.exists())

    def test_rejects_non_markdown(self):
        response = self.client.post("/", {"document": SimpleUploadedFile("x.txt", b"hello")})
        self.assertContains(response, "только файлы .md")

    @patch("reviewapp.views.run_review")
    def test_review_flow(self, run_review):
        run_review.return_value = [{"error_type_id": "D01", "family": "D", "title": "Ошибка", "problem": "Описание", "evidence_quote": "фрагмент"}]
        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", "# ТЗ\nфрагмент".encode()),
            "filters": ["D", "T", "U"], "model": "test/model",
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "D01")
        self.assertContains(response, 'data-family="D"')
        self.assertContains(response, 'data-family="T"')
        self.assertContains(response, 'data-family="U"')
        self.assertNotContains(response, 'name="filters"')
        self.assertEqual(run_review.call_args.args[2], ["D", "T", "U"])
        self.assertEqual(Review.objects.get().findings[0]["error_type_id"], "D01")


class MarkdownRenderingTests(TestCase):
    def test_renders_table_and_removes_unsafe_html(self):
        document = "# Заголовок\n\n| Поле | Тип |\n|---|---|\n| id | int |\n\n<script>alert(1)</script>"
        result = rendered_markdown(document, [])
        self.assertIn("<table>", result)
        self.assertIn("<th>Поле</th>", result)
        self.assertNotIn("<script", result)

    def test_builds_highlight_terms_for_markdown_table(self):
        terms = highlight_terms("| Поле | Тип |\n|---|---|\n| user_id | bigint |")
        self.assertEqual(terms, ["Поле", "Тип", "user_id", "bigint"])

    def test_source_marks_include_family(self):
        result = highlighted_source("ошибка здесь", [{
            "error_type_id": "D01", "family": "D", "evidence_quote": "ошибка"
        }])
        self.assertIn('data-family="D"', result)


class ReviewerLockTests(TestCase):
    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer.ChatOpenRouter")
    def test_provider_error_releases_review_slot(self, chat_class):
        chat_class.return_value.with_structured_output.return_value.invoke.side_effect = RuntimeError("provider failed")
        for _ in range(2):
            with self.assertRaisesRegex(ReviewError, "provider failed"):
                run_review("document", "test/model", ["D"], 1)
