from io import BytesIO
import uuid
from unittest.mock import call, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase

from .models import OpenRouterModel, Review, ReviewFinding
from .rendering import highlight_groups, highlight_terms, highlighted_source, rendered_markdown
from run_inference import BASE_INSTRUCTIONS, Finding, Findings, build_prompt
from .views import CHECK_ERROR_MESSAGES, _execute_review, _finding_sort_key

from .reviewer import (
    ReviewError,
    _invoke_schema,
    _resolve_evidence_quote,
    run_review,
)


class UploadTests(TestCase):
    def test_findings_are_sorted_by_family_and_numeric_id(self):
        findings = [
            {"error_type_id": "U02"},
            {"error_type_id": "D08"},
            {"error_type_id": "D01"},
            {"error_type_id": "OTHER"},
            {"error_type_id": "T01"},
            {"error_type_id": "U01"},
        ]
        self.assertEqual(
            [item["error_type_id"] for item in sorted(findings, key=_finding_sort_key)],
            ["D01", "D08", "T01", "U01", "U02", "OTHER"],
        )

    def test_get_renders_form(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'action="/api/reports/"')

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
        self.assertContains(response, 'data-family="D" data-count="1"')
        self.assertContains(response, 'data-family="T" data-count="0"')
        self.assertNotContains(response, 'name="filters"')
        self.assertNotContains(response, "{{ predictions|length }}")
        self.assertNotContains(response, "{{ count }}")
        self.assertContains(response, 'class="review-checkbox"')
        self.assertContains(response, "Проверено")
        self.assertContains(response, 'class="btn new-review-btn"')
        self.assertContains(response, "Новая проверка")
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

    @patch("reviewapp.views.choice", return_value="Упс, что-то пошло не так. Попробуйте ещё раз.")
    @patch("reviewapp.views.run_review", side_effect=ReviewError("Ошибка провайдера"))
    def test_error_page_allows_uploading_another_file(self, run_review, choice):
        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", b"# spec"),
            "model": "minimax/minimax-m3",
        })

        self.assertContains(response, "Упс, что-то пошло не так")
        self.assertNotContains(response, "Ошибка провайдера")
        self.assertContains(response, "Загрузить другой файл")
        self.assertContains(response, 'href="/"')
        self.assertEqual(Review.objects.get().error, "Ошибка провайдера")
        choice.assert_called_once_with(CHECK_ERROR_MESSAGES)

    @patch("reviewapp.views.run_review")
    def test_completed_review_has_permanent_page(self, run_review):
        run_review.return_value = [{
            "error_type_id": "D01",
            "title": "Ошибка",
            "problem": "Описание",
            "evidence_quote": "фрагмент",
        }]

        response = self.client.post("/", {
            "document": SimpleUploadedFile("spec.md", "# ТЗ\nфрагмент".encode()),
            "model": "minimax/minimax-m3",
        })

        review = Review.objects.get()
        report_path = f"/reports/{review.public_id}/"
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Report-URL"], report_path)
        self.assertEqual(review.document, "# ТЗ\nфрагмент")
        saved_response = self.client.get(report_path)
        self.assertEqual(saved_response.status_code, 200)
        self.assertContains(saved_response, "D01")
        self.assertContains(saved_response, "фрагмент")

    @patch("reviewapp.views._submit_review_job")
    @patch("reviewapp.views.run_review")
    def test_api_creates_and_returns_report_by_uuid(self, run_review, submit_review_job):
        run_review.return_value = [{
            "error_type_id": "U02",
            "title": "Замечание",
            "problem": "Описание",
            "evidence_quote": "исходник",
        }]

        create_response = self.client.post("/api/reports/", {
            "document": SimpleUploadedFile("api.md", "# API\nисходник".encode()),
            "model": "minimax/minimax-m3",
        })

        self.assertEqual(create_response.status_code, 202)
        created = create_response.json()
        self.assertEqual(created["status"], "pending")
        self.assertEqual(created["findings_count"], 0)
        self.assertTrue(created["report_url"].endswith(f"/reports/{created['id']}/"))
        review = Review.objects.get(public_id=created["id"])
        submit_review_job.assert_called_once_with(review.pk)

        pending_page = self.client.get(f"/reports/{created['id']}/")
        self.assertContains(pending_page, "Проверяем документ")
        _execute_review(review, OpenRouterModel.objects.get(slug="minimax/minimax-m3"))
        detail_response = self.client.get(f"/api/reports/{created['id']}/")
        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(detail_response.json()["status"], "completed")
        self.assertEqual(detail_response.json()["findings"], run_review.return_value)

    @patch("reviewapp.views._submit_review_job")
    @patch("reviewapp.views.run_review", side_effect=ReviewError("Секрет провайдера"))
    def test_api_saves_llm_error_without_exposing_it(self, run_review, submit_review_job):
        response = self.client.post("/api/reports/", {
            "document": SimpleUploadedFile("api.md", b"# API"),
            "model": "minimax/minimax-m3",
        })

        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["status"], "pending")
        review = Review.objects.get()
        _execute_review(review, OpenRouterModel.objects.get(slug="minimax/minimax-m3"))
        result_response = self.client.get(f"/api/reports/{review.public_id}/")
        self.assertEqual(result_response.json()["status"], "failed")
        self.assertNotContains(result_response, "Секрет провайдера")
        review.refresh_from_db()
        self.assertEqual(review.error, "Секрет провайдера")

    def test_api_rejects_missing_document(self):
        response = self.client.post("/api/reports/", {})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"], "Для проверки добавьте файлы.")

    def test_api_returns_json_404_for_unknown_report(self):
        response = self.client.get(f"/api/reports/{uuid.uuid4()}/")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"error": "Отчёт не найден."})


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

    def test_highlight_terms_preserve_repeated_cells(self):
        terms = highlight_terms("| Greenplum | - |\n| Greenplum | - |")
        self.assertEqual(terms, ["Greenplum", "-", "Greenplum", "-"])

    def test_highlight_terms_split_inline_code_from_text(self):
        terms = highlight_terms("Шаг 1. Фильтрация: `FIELD_LAT IS NOT NULL`.")
        self.assertEqual(terms, ["Шаг 1. Фильтрация:", "FIELD_LAT IS NOT NULL"])

    def test_highlight_terms_ignore_contextless_numbers_and_placeholders(self):
        terms = highlight_terms("| TABLE | 100 | ... | - |\nЗадержка: < 1 мин")
        self.assertEqual(terms, ["TABLE", "-", "Задержка: < 1 мин"])

    def test_highlight_terms_include_link_label_and_source_url(self):
        terms = highlight_terms("| [DC: DDS](http://datacatalog.corp/dds) |")
        self.assertEqual(terms, ["DC: DDS", "http://datacatalog.corp/dds"])

    def test_highlight_terms_split_adjacent_sentences_for_overlapping_findings(self):
        terms = highlight_terms("Обработка выполняется в DDS/CDM. На этапе RAW — не применимо.")
        self.assertEqual(terms, [
            "Обработка выполняется в DDS/CDM.",
            "На этапе RAW — не применимо.",
        ])

    def test_highlight_groups_keep_table_cells_in_the_same_row(self):
        groups = highlight_groups(
            "| FIELD_DATE_EVENT | date | Kafka |\n| FIELD_TIME_START | long | Kafka |"
        )
        self.assertEqual(groups, [
            ["FIELD_DATE_EVENT", "date", "Kafka"],
            ["FIELD_TIME_START", "long", "Kafka"],
        ])

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

    def test_rendered_markdown_highlight_does_not_break_table_structure(self):
        document = "Kafka — 24 ч\n\nData Catalog\n- http://catalog/item"
        findings = [{
            "error_type_id": "D02", "family": "D",
            "evidence_quote": "Data Catalog\n- http://catalog/item",
        }]

        result = rendered_markdown(document, findings)

        self.assertNotIn("\ue000R0S\ue001", result)
        self.assertIn("<ul>", result)

        table = "| Поле | Тип |\n|---|---|\n| user_id | bigint |"
        table_result = rendered_markdown(table, [{
            "error_type_id": "D01", "family": "D", "evidence_quote": table,
        }])
        self.assertEqual(table_result.count("<table>"), 1)
        self.assertEqual(table_result.count("<tr>"), 2)

    def test_source_is_pristine_before_shared_client_highlighting(self):
        result = highlighted_source("ошибка здесь", [{
            "error_type_id": "D01", "family": "D", "evidence_quote": "ошибка"
        }])
        self.assertEqual(result, "ошибка здесь")
        self.assertNotIn("<mark", result)

    def test_source_escapes_html_before_shared_client_highlighting(self):
        result = highlighted_source("иная ошибка", [{
            "error_type_id": "OTHER", "family": "OTHER", "evidence_quote": "иная ошибка"
        }])
        self.assertEqual(result, "иная ошибка")

        escaped = highlighted_source("<script>alert(1)</script>", [])
        self.assertNotIn("<script>", escaped)
        self.assertIn("&lt;script&gt;", escaped)


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

    def test_resolves_unique_quote_with_changed_whitespace(self):
        document = "Строка с двумя  пробелами\nи переносом."
        self.assertEqual(
            _resolve_evidence_quote(document, "Строка с двумя пробелами и переносом."),
            document,
        )

    def test_resolves_quote_with_small_text_difference(self):
        document = "Шаг 1. Фильтрация данных выполняется до обогащения.\nСледующий этап."
        self.assertEqual(
            _resolve_evidence_quote(
                document,
                "Шаг 1. Фильтрация данных выполняется перед обогащением.\nСледующий этап.",
            ),
            document,
        )

    def test_does_not_resolve_ambiguous_quote(self):
        self.assertIsNone(_resolve_evidence_quote("поле id и поле id", "поле id"))
