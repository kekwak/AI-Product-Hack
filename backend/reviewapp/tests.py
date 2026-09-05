from io import BytesIO
from unittest.mock import Mock, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from .models import OpenRouterModel, Review, ReviewFinding
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from run_inference import Findings

from .reviewer import (
    ReviewError,
    _invoke_schema,
    _recover_parsed_response,
    _resolve_evidence_quote,
    run_review,
)


class UploadTests(TestCase):
    def test_get_renders_form(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)

    def test_rejects_non_markdown(self):
        response = self.client.post("/", {"document": SimpleUploadedFile("x.txt", b"hello")})
        self.assertContains(response, "только файлы .md")

    def test_missing_document_uses_inline_warning(self):
        response = self.client.post("/", {})

        self.assertContains(response, "Для проверки добавьте файлы")
        self.assertNotContains(response, "Загрузить другой файл")

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
        self.assertEqual(run_review.call_args.args, ("# ТЗ\nфрагмент", "minimax/minimax-m3"))
        self.assertEqual(Review.objects.get().findings[0]["error_type_id"], "D01")
        saved_finding = ReviewFinding.objects.get()
        self.assertEqual(saved_finding.error_type_id, "D01")
        self.assertEqual(saved_finding.result, run_review.return_value[0])
        self.assertEqual(saved_finding.review, Review.objects.get())

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

    @patch("reviewapp.views.run_review", side_effect=ReviewError("Ошибка провайдера"))
    def test_error_page_allows_uploading_another_file(self, run_review):
        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", b"# spec"),
            "model": "minimax/minimax-m3",
        })

        self.assertContains(response, "Ошибка провайдера")
        self.assertContains(response, "Загрузить другой файл")
        self.assertContains(response, 'href="/"')


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

    def test_renders_list_without_blank_line_before_it(self):
        document = "Продуктовые метрики\n- Задержка: < 1 мин\n- Пропускная способность: 100 000\n\nЗаказчики\n- BigData"

        result = rendered_markdown(document, [])

        self.assertIn("<ul>", result)
        self.assertEqual(result.count("<li>"), 3)
        self.assertIn("<p>Продуктовые метрики</p>", result)

    def test_does_not_normalize_list_markers_inside_code_fence(self):
        result = rendered_markdown("```text\nheading\n- not a list\n```", [])

        self.assertIn("- not a list", result)
        self.assertNotIn("<li>", result)

    def test_rendered_markdown_preserves_exact_multiline_evidence_boundaries(self):
        document = "Kafka — 24 ч\n\nData Catalog\n- http://catalog/item"
        findings = [{
            "error_type_id": "D02", "family": "D",
            "evidence_quote": "Data Catalog\n- http://catalog/item",
        }]

        result = rendered_markdown(document, findings)

        self.assertEqual(result.count("\ue000R0S\ue001"), 1)
        self.assertEqual(result.count("\ue000R0E\ue001"), 1)

    def test_source_marks_include_family(self):
        result = highlighted_source("ошибка здесь", [{
            "error_type_id": "D01", "family": "D", "evidence_quote": "ошибка"
        }])
        self.assertIn('data-family="D"', result)

    def test_source_marks_support_other_family(self):
        result = highlighted_source("иная ошибка", [{
            "error_type_id": "OTHER", "family": "OTHER", "evidence_quote": "иная ошибка"
        }])
        self.assertIn('data-family="OTHER"', result)


class ReviewerResilienceTests(TestCase):
    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer.httpx.post")
    def test_reports_when_model_has_no_compatible_endpoint(self, post):
        no_endpoint = Mock(status_code=404, is_error=True, reason_phrase="Not Found")
        no_endpoint.json.return_value = {
            "error": {"message": "No endpoints found that can handle the requested parameters"}
        }
        post.return_value = no_endpoint

        with self.assertRaisesRegex(ReviewError, "Выберите другую модель"):
            _invoke_schema("test/model", Findings, [{"role": "user", "content": "x"}])

        self.assertEqual(post.call_count, 1)
        payload = post.call_args.kwargs["json"]
        self.assertNotIn("max_tokens", payload)
        self.assertNotIn("reasoning", payload)
        self.assertNotIn("provider", payload)

    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer._invoke_schema")
    def test_provider_error_does_not_poison_next_request(self, invoke_schema):
        invoke_schema.side_effect = RuntimeError("provider failed")
        for _ in range(2):
            with self.assertRaisesRegex(ReviewError, "provider failed"):
                run_review("document", "test/model")

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
