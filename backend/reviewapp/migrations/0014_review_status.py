from django.db import migrations, models


def mark_failed_reviews(apps, schema_editor):
    review_model = apps.get_model("reviewapp", "Review")
    review_model.objects.exclude(error="").update(status="failed")


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0013_review_public_id_document")]

    operations = [
        migrations.AddField(
            model_name="review",
            name="status",
            field=models.CharField(
                choices=[
                    ("pending", "Ожидает"),
                    ("running", "Выполняется"),
                    ("completed", "Готов"),
                    ("failed", "Ошибка"),
                ],
                db_index=True,
                default="completed",
                max_length=16,
                verbose_name="статус",
            ),
        ),
        migrations.RunPython(mark_failed_reviews, migrations.RunPython.noop),
    ]
