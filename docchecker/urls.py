from django.urls import path

from . import views

app_name = "docchecker"

urlpatterns = [
    path("", views.upload_view, name="upload"),
    path("download/", views.download_view, name="download"),
]
