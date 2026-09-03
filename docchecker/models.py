from django.db import models


class HeadingRule(models.Model):

    LEVEL_CHOICES = [(i, f"H{i} ('{'#' * i}')") for i in range(1, 7)]

    title = models.CharField(
        max_length=255,
        help_text="Текст заголовка, например 'Описание' или 'Установка'.",
    )
    level = models.PositiveSmallIntegerField(
        choices=LEVEL_CHOICES,
        null=True,
        blank=True,
        help_text="Уровень заголовка (# = 1 ... ###### = 6). Оставьте пустым, чтобы уровень не проверялся.",
    )
    is_required = models.BooleanField(
        default=True,
        help_text="Если выключено, заголовок необязателен и будет только проверен для информации.",
    )
    order = models.PositiveIntegerField(
        default=0,
        help_text="Порядок отображения/проверки правил.",
    )
    is_active = models.BooleanField(
        default=True,
        help_text="Выключенные правила не участвуют в проверке.",
    )

    class Meta:
        ordering = ["order", "id"]

    def __str__(self) -> str:
        level = f"H{self.level} " if self.level else ""
        required = "" if self.is_required else " (опционально)"
        return f"{level}{self.title}{required}"
