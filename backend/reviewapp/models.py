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


class ReviewSettings(models.Model):
    PIPELINE_CHOICES = [
        ("fast", "Быстрый — один запрос"),
        ("quality", "Качественный — D/T/U + Judge"),
    ]
    REASONING_CHOICES = [
        ("low", "Быстро"),
        ("medium", "Сбалансированно"),
        ("high", "Глубокий анализ"),
    ]

    model = models.CharField("модель OpenRouter", max_length=160, default="minimax/minimax-m3")
    allow_client_model = models.BooleanField("разрешить клиенту выбирать модель", default=True)
    enabled_d = models.BooleanField("проверять D-критерии", default=True)
    enabled_t = models.BooleanField("проверять T-критерий", default=True)
    enabled_u = models.BooleanField("проверять U-критерии", default=True)
    max_findings = models.PositiveSmallIntegerField("максимум замечаний", default=8)
    max_output_tokens = models.PositiveIntegerField("максимум токенов ответа", default=2048)
    reasoning_effort = models.CharField(
        "глубина анализа", max_length=10, choices=REASONING_CHOICES, default="low"
    )
    pipeline_mode = models.CharField(
        "режим проверки", max_length=10, choices=PIPELINE_CHOICES, default="fast"
    )

    class Meta:
        verbose_name = "настройки проверки"
        verbose_name_plural = "настройки проверки"

    def __str__(self):
        return "Настройки проверки"

    def save(self, *args, **kwargs):
        self.pk = 1
        super().save(*args, **kwargs)

    @classmethod
    def load(cls):
        obj, _ = cls.objects.get_or_create(pk=1)
        return obj


class Review(models.Model):
    created_at = models.DateTimeField("дата", auto_now_add=True)
    document_name = models.CharField("файл", max_length=255)
    model = models.CharField("модель", max_length=160)
    pipeline_mode = models.CharField("режим проверки", max_length=10, default="fast")
    filters = models.JSONField("фильтры", default=list)
    findings = models.JSONField("результат", default=list)
    error = models.TextField("ошибка", blank=True)

    class Meta:
        ordering = ["-created_at"]
        verbose_name = "проверка"
        verbose_name_plural = "проверки"

    def __str__(self):
        return f"{self.document_name} — {self.created_at:%d.%m.%Y %H:%M}"
