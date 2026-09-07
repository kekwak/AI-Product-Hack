from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0007_openroutermodel")]
    operations = [
        migrations.AddField(
            model_name="reviewsettings",
            name="enabled_other",
            field=models.BooleanField(default=True, verbose_name="проверять OTHER"),
        ),
    ]
