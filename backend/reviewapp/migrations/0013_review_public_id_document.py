import uuid

from django.db import migrations, models


def populate_public_ids(apps, schema_editor):
    review_model = apps.get_model("reviewapp", "Review")
    for review in review_model.objects.filter(public_id__isnull=True).iterator():
        review.public_id = uuid.uuid4()
        review.save(update_fields=["public_id"])


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0012_openroutermodel_inference_settings")]

    operations = [
        migrations.AddField(
            model_name="review",
            name="document",
            field=models.TextField(blank=True, verbose_name="исходный документ"),
        ),
        migrations.AddField(
            model_name="review",
            name="public_id",
            field=models.UUIDField(editable=False, null=True, unique=True, verbose_name="публичный ID"),
        ),
        migrations.RunPython(populate_public_ids, migrations.RunPython.noop),
        migrations.AlterField(
            model_name="review",
            name="public_id",
            field=models.UUIDField(default=uuid.uuid4, editable=False, unique=True, verbose_name="публичный ID"),
        ),
    ]
