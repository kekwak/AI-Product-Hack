from django.urls import path

from . import views

app_name = "docchecker"

urlpatterns = [
    path("", views.upload_view, name="upload"),
]
