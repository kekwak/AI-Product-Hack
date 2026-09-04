from django.db import migrations, models


def reduce_existing_limit(apps, schema_editor):
    settings_model = apps.get_model("reviewapp", "ReviewSettings")
    settings_model.objects.filter(max_findings__gt=8).update(max_findings=8)


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0003_reduce_default_output_tokens")]
    operations = [
        migrations.AlterField(
            model_name="reviewsettings",
            name="max_findings",
            field=models.PositiveSmallIntegerField(default=8, verbose_name="максимум замечаний"),
        ),
        migrations.RunPython(reduce_existing_limit, migrations.RunPython.noop),
    ]
