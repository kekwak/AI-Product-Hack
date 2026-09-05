from io import BytesIO
from unittest.mock import call, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from .models import OpenRouterModel, Review, ReviewFinding
from .rendering import highlight_terms, highlighted_source, rendered_markdown
from run_inference import BASE_INSTRUCTIONS, Finding, Findings, build_prompt

from .reviewer import (
    ReviewError,
    _invoke_schema,
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
        self.assertEqual(run_review.call_args.kwargs, {
            "max_tokens": 65536,
            "reasoning_effort": "high",
            "no_reasoning": False,
            "temperature": 0.0,
            "no_temperature": False,
            "provider": "",
        })
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

    @patch("reviewapp.views.run_review", return_value=[])
    def test_passes_admin_model_settings_to_inference(self, run_review):
        OpenRouterModel.objects.create(
            name="Configured",
            slug="configured/model",
            provider="deepinfra",
            max_tokens=32768,
            reasoning_effort="xhigh",
            no_reasoning=False,
            temperature=0.7,
            no_temperature=True,
        )

        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", b"# spec"),
            "model": "configured/model",
        })

        self.assertEqual(response.status_code, 200)
        self.assertEqual(run_review.call_args.kwargs, {
            "max_tokens": 32768,
            "reasoning_effort": "xhigh",
            "no_reasoning": False,
            "temperature": 0.7,
            "no_temperature": True,
            "provider": "deepinfra",
        })

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
    @patch("reviewapp.reviewer.ChatOpenRouter")
    def test_uses_benchmark_model_options_without_optional_parameters(self, chat_class):
        structured = chat_class.return_value.with_structured_output.return_value
        structured.invoke.return_value = {"parsed": Findings(errors=[])}
        messages = [("system", "prompt"), ("human", "document")]

        parsed = _invoke_schema(
            "test/model",
            Findings,
            messages,
            max_tokens=32768,
            reasoning_effort="xhigh",
            no_temperature=True,
        )

        self.assertEqual(parsed.errors, [])
        chat_class.assert_called_once_with(
            model="test/model",
            api_key="test-key",
            max_tokens=32768,
            max_retries=0,
            reasoning={"effort": "xhigh", "exclude": True},
        )
        chat_class.return_value.with_structured_output.assert_called_once_with(
            Findings, method="json_schema", strict=True, include_raw=True,
        )
        structured.invoke.assert_called_once_with(messages)

    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer.ChatOpenRouter")
    def test_sends_temperature_and_provider_only_when_configured(self, chat_class):
        structured = chat_class.return_value.with_structured_output.return_value
        structured.invoke.return_value = {"parsed": Findings(errors=[])}

        _invoke_schema(
            "test/model",
            Findings,
            [],
            no_reasoning=True,
            temperature=0.25,
            provider="deepinfra",
        )

        chat_class.assert_called_once_with(
            model="test/model",
            api_key="test-key",
            max_tokens=65536,
            max_retries=0,
            reasoning={"enabled": False},
            temperature=0.25,
            openrouter_provider={
                "only": ["deepinfra"],
                "allow_fallbacks": False,
                "require_parameters": True,
            },
        )

    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer.time.sleep")
    @patch("reviewapp.reviewer._invoke_schema")
    def test_retries_like_batch_inference(self, invoke_schema, sleep):
        invoke_schema.side_effect = RuntimeError("provider failed")

        with self.assertRaisesRegex(ReviewError, "provider failed"):
            run_review("document", "test/model")

        self.assertEqual(invoke_schema.call_count, 3)
        self.assertEqual(sleep.call_args_list, [call(1), call(2)])

    @patch.dict("os.environ", {"OPENROUTER_API_KEY": "test-key"})
    @patch("reviewapp.reviewer._invoke_schema")
    def test_uses_exact_prompt_and_does_not_filter_model_findings(self, invoke_schema):
        findings = Findings(errors=[
            Finding(
                error_type_id="O",
                evidence_quote="цитата, которой нет в исходном документе",
                title="Первая",
                problem="Описание",
            ),
            Finding(
                error_type_id="O",
                evidence_quote="цитата, которой нет в исходном документе",
                title="Дубликат",
                problem="Другое описание",
            ),
        ])
        invoke_schema.return_value = findings

        result = run_review("исходный документ", "test/model", retries=0)

        self.assertEqual(result, [item.model_dump() for item in findings.errors])
        messages = invoke_schema.call_args.args[2]
        self.assertEqual(messages, [
            ("system", build_prompt(BASE_INSTRUCTIONS)),
            ("human", "# ПРОВЕРЯЕМЫЙ ДОКУМЕНТ\n\n```\nисходный документ\n```"),
        ])
