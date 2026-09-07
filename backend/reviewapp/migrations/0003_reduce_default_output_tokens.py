from django.db import migrations, models


def reduce_existing_limit(apps, schema_editor):
    settings_model = apps.get_model("reviewapp", "ReviewSettings")
    settings_model.objects.filter(max_output_tokens__gt=2048).update(max_output_tokens=2048)


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0002_reviewsettings_max_output_tokens")]
    operations = [
        migrations.AlterField(
            model_name="reviewsettings",
            name="max_output_tokens",
            field=models.PositiveIntegerField(default=2048, verbose_name="максимум токенов ответа"),
        ),
        migrations.RunPython(reduce_existing_limit, migrations.RunPython.noop),
    ]
