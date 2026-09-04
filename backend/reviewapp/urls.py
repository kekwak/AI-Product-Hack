from django.urls import path
from . import views

app_name = "reviewapp"
urlpatterns = [path("", views.upload, name="upload")]
