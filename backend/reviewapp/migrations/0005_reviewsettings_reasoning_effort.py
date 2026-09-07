from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0004_reduce_default_findings")]
    operations = [
        migrations.AddField(
            model_name="reviewsettings",
            name="reasoning_effort",
            field=models.CharField(
                choices=[("low", "Быстро"), ("medium", "Сбалансированно"), ("high", "Глубокий анализ")],
                default="low",
                max_length=10,
                verbose_name="глубина анализа",
            ),
        ),
    ]
