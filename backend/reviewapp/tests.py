from io import BytesIO
from unittest.mock import patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from .models import OpenRouterModel, Review, ReviewSettings
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from .reviewer import ReviewError, _recover_parsed_response, _resolve_evidence_quote, run_review


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
            "filters": ["D", "T", "U"], "model": "minimax/minimax-m3",
        })
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "D01")
        self.assertContains(response, 'data-family="D"')
        self.assertContains(response, 'data-family="T"')
        self.assertContains(response, 'data-family="U"')
        self.assertNotContains(response, 'name="filters"')
        self.assertEqual(run_review.call_args.args[2], ["D", "T", "U"])
        self.assertEqual(Review.objects.get().findings[0]["error_type_id"], "D01")

    @patch("reviewapp.views.run_review")
    def test_rejects_model_not_enabled_by_admin(self, run_review):
        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", b"# spec"),
            "model": "unknown/model",
        })
        self.assertContains(response, "Выберите доступную модель OpenRouter")
        run_review.assert_not_called()

    def test_upload_lists_only_enabled_models(self):
        OpenRouterModel.objects.create(name="Disabled", slug="disabled/model", enabled=False)
        response = self.client.get("/")
        self.assertContains(response, "minimax/minimax-m3")
        self.assertNotContains(response, "disabled/model")


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


class ReviewerResilienceTests(TestCase):
    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer._invoke_schema")
    def test_provider_error_does_not_poison_next_request(self, invoke_schema):
        invoke_schema.side_effect = RuntimeError("provider failed")
        for _ in range(2):
            with self.assertRaisesRegex(ReviewError, "provider failed"):
                run_review("document", "test/model", ["D"], 1)

    def test_recovers_json_from_markdown_fence(self):
        class Raw:
            content = 'Ответ:\n```json\n{"errors": []}\n```'

        parsed = _recover_parsed_response({"raw": Raw()})
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.errors, [])

    def test_empty_schema_response_does_not_crash(self):
        self.assertIsNone(_recover_parsed_response({"raw": None}))

    def test_resolves_unique_quote_with_changed_whitespace(self):
        document = "Строка с двумя  пробелами\nи переносом."
        self.assertEqual(
            _resolve_evidence_quote(document, "Строка с двумя пробелами и переносом."),
            document,
        )

    def test_does_not_resolve_ambiguous_quote(self):
        self.assertIsNone(_resolve_evidence_quote("поле id и поле id", "поле id"))

    @patch("reviewapp.reviewer._judge_candidates")
    @patch("reviewapp.reviewer._run_candidate_review")
    def test_quality_mode_runs_family_passes_and_judge(self, candidate_review, judge):
        candidate_review.side_effect = [
            [{"error_type_id": "D01", "evidence_quote": "d", "family": "D"}],
            [{"error_type_id": "T01", "evidence_quote": "t", "family": "T"}],
            [{"error_type_id": "U01", "evidence_quote": "u", "family": "U"}],
        ]
        judge.return_value = [{"error_type_id": "U01", "evidence_quote": "u", "family": "U"}]

        result = run_review("document", "test/model", ["D", "T", "U"], 8, 2048, "low", "quality")

        self.assertEqual(candidate_review.call_count, 3)
        judge.assert_called_once()
        self.assertEqual(result[0]["error_type_id"], "U01")
