from django.urls import path
from . import views

app_name = "reviewapp"
urlpatterns = [
    path("", views.upload, name="upload"),
    path("reports/<uuid:report_id>/", views.report_detail, name="report_detail"),
    path("api/reports/", views.reports_api, name="reports_api"),
    path("api/reports/<uuid:report_id>/", views.report_api_detail, name="report_api_detail"),
]
