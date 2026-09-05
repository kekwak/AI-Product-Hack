from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [("reviewapp", "0008_reviewsettings_enabled_other")]
    operations = [migrations.DeleteModel(name="ReviewSettings")]
