from django.db import migrations, models


def configure_benchmark_models(apps, schema_editor):
    model = apps.get_model("reviewapp", "OpenRouterModel")
    settings = {
        "openai/gpt-5.6-luna:nitro": {
            "max_tokens": 32768,
            "reasoning_effort": "high",
            "no_temperature": True,
        },
        "openai/gpt-5.6-sol:nitro": {
            "max_tokens": 65536,
            "reasoning_effort": "high",
            "no_temperature": True,
        },
        "qwen/qwen3.8-27b": {
            "max_tokens": 65536,
            "reasoning_effort": "xhigh",
            "temperature": 0.0,
            "no_temperature": False,
        },
    }
    for slug, values in settings.items():
        model.objects.filter(slug=slug).update(**values)


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0011_reviewfinding")]

    operations = [
        migrations.AddField(
            model_name="openroutermodel",
            name="provider",
            field=models.CharField(blank=True, max_length=120, verbose_name="провайдер OpenRouter"),
        ),
        migrations.AddField(
            model_name="openroutermodel",
            name="max_tokens",
            field=models.PositiveIntegerField(default=65536, verbose_name="максимум токенов ответа"),
        ),
        migrations.AddField(
            model_name="openroutermodel",
            name="reasoning_effort",
            field=models.CharField(
                choices=[
                    ("minimal", "Minimal"), ("low", "Low"), ("medium", "Medium"),
                    ("high", "High"), ("xhigh", "XHigh"), ("max", "Max"),
                ],
                default="high",
                max_length=10,
                verbose_name="reasoning effort",
            ),
        ),
        migrations.AddField(
            model_name="openroutermodel",
            name="no_reasoning",
            field=models.BooleanField(default=False, verbose_name="отключить reasoning (--no-reasoning)"),
        ),
        migrations.AddField(
            model_name="openroutermodel",
            name="temperature",
            field=models.FloatField(default=0.0, verbose_name="temperature"),
        ),
        migrations.AddField(
            model_name="openroutermodel",
            name="no_temperature",
            field=models.BooleanField(default=False, verbose_name="не передавать temperature (--no-temperature)"),
        ),
        migrations.RunPython(configure_benchmark_models, migrations.RunPython.noop),
    ]
