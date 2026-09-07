from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(name="ReviewSettings", fields=[
            ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
            ("model", models.CharField(default="minimax/minimax-m3", max_length=160, verbose_name="модель OpenRouter")),
            ("allow_client_model", models.BooleanField(default=True, verbose_name="разрешить клиенту выбирать модель")),
            ("enabled_d", models.BooleanField(default=True, verbose_name="проверять D-критерии")),
            ("enabled_t", models.BooleanField(default=True, verbose_name="проверять T-критерий")),
            ("enabled_u", models.BooleanField(default=True, verbose_name="проверять U-критерии")),
            ("max_findings", models.PositiveSmallIntegerField(default=15, verbose_name="максимум замечаний")),
        ], options={"verbose_name": "настройки проверки", "verbose_name_plural": "настройки проверки"}),
        migrations.CreateModel(name="Review", fields=[
            ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
            ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="дата")),
            ("document_name", models.CharField(max_length=255, verbose_name="файл")),
            ("model", models.CharField(max_length=160, verbose_name="модель")),
            ("filters", models.JSONField(default=list, verbose_name="фильтры")),
            ("findings", models.JSONField(default=list, verbose_name="результат")),
            ("error", models.TextField(blank=True, verbose_name="ошибка")),
        ], options={"verbose_name": "проверка", "verbose_name_plural": "проверки", "ordering": ["-created_at"]}),
    ]
