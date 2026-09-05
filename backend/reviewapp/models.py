from django.db import models


class OpenRouterModel(models.Model):
    name = models.CharField("название", max_length=120)
    slug = models.CharField("идентификатор OpenRouter", max_length=160, unique=True)
    enabled = models.BooleanField("доступна пользователям", default=True)
    sort_order = models.PositiveSmallIntegerField("порядок", default=0)

    class Meta:
        ordering = ["sort_order", "name"]
        verbose_name = "модель OpenRouter"
        verbose_name_plural = "модели OpenRouter"

    def __str__(self):
        return f"{self.name} ({self.slug})"


class Review(models.Model):
    created_at = models.DateTimeField("дата", auto_now_add=True)
    document_name = models.CharField("файл", max_length=255)
    model = models.CharField("модель", max_length=160)
    findings = models.JSONField("результат", default=list)
    error = models.TextField("ошибка", blank=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "проверка"
        verbose_name_plural = "проверки"

    def __str__(self):
        return f"{self.document_name} — {self.created_at:%d.%m.%Y %H:%M}"


class ReviewFinding(models.Model):
    review = models.ForeignKey(
        Review,
        on_delete=models.CASCADE,
        related_name="result_items",
        verbose_name="проверка",
    )
    error_type_id = models.CharField("ID типа ошибки", max_length=32, db_index=True)
    result = models.JSONField("JSON результата")

    class Meta:
        ordering = ["id"]
        verbose_name = "результат проверки"
        verbose_name_plural = "результаты проверок"

    def __str__(self):
        return f"{self.error_type_id} — {self.review.document_name}"
