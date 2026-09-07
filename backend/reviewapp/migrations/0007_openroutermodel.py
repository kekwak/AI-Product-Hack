from django.db import migrations, models


def seed_models(apps, schema_editor):
    model = apps.get_model("reviewapp", "OpenRouterModel")
    model.objects.bulk_create([
        model(name="MiniMax M3", slug="minimax/minimax-m3", enabled=True, sort_order=10),
        model(name="Gemma 4 26B A4B (free)", slug="google/gemma-4-26b-a4b-it:free", enabled=True, sort_order=20),
    ], ignore_conflicts=True)


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0006_pipeline_modes")]
    operations = [
        migrations.CreateModel(
            name="OpenRouterModel",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120, verbose_name="название")),
                ("slug", models.CharField(max_length=160, unique=True, verbose_name="идентификатор OpenRouter")),
                ("enabled", models.BooleanField(default=True, verbose_name="доступна пользователям")),
                ("sort_order", models.PositiveSmallIntegerField(default=0, verbose_name="порядок")),
            ],
            options={"verbose_name": "модель OpenRouter", "verbose_name_plural": "модели OpenRouter", "ordering": ["sort_order", "name"]},
        ),
        migrations.RunPython(seed_models, migrations.RunPython.noop),
    ]
