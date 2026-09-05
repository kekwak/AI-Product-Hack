from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0005_reviewsettings_reasoning_effort")]
    operations = [
        migrations.AddField(
            model_name="reviewsettings",
            name="pipeline_mode",
            field=models.CharField(
                choices=[("fast", "Быстрый — один запрос"), ("quality", "Качественный — D/T/U + Judge")],
                default="fast", max_length=10, verbose_name="режим проверки",
            ),
        ),
        migrations.AddField(
            model_name="review",
            name="pipeline_mode",
            field=models.CharField(default="fast", max_length=10, verbose_name="режим проверки"),
        ),
    ]
