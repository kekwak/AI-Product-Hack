from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0001_initial")]
    operations = [
        migrations.AddField(
            model_name="reviewsettings",
            name="max_output_tokens",
            field=models.PositiveIntegerField(default=4096, verbose_name="максимум токенов ответа"),
        ),
    ]
