from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0010_remove_review_pipeline_and_filters")]

    operations = [
        migrations.CreateModel(
            name="ReviewFinding",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("error_type_id", models.CharField(db_index=True, max_length=32, verbose_name="ID типа ошибки")),
                ("result", models.JSONField(verbose_name="JSON результата")),
                ("review", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="result_items", to="reviewapp.review", verbose_name="проверка")),
            ],
            options={
                "verbose_name": "результат проверки",
                "verbose_name_plural": "результаты проверок",
                "ordering": ["id"],
            },
        ),
    ]
