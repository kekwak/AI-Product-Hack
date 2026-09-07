from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0009_remove_reviewsettings")]

    operations = [
        migrations.RemoveField(model_name="review", name="pipeline_mode"),
        migrations.RemoveField(model_name="review", name="filters"),
    ]
