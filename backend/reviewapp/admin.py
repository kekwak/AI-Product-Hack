from django.contrib import admin
from .models import OpenRouterModel, Review, ReviewSettings


@admin.register(OpenRouterModel)
class OpenRouterModelAdmin(admin.ModelAdmin):
    list_display = ("name", "slug", "enabled", "sort_order")
    list_editable = ("enabled", "sort_order")
    list_filter = ("enabled",)
    search_fields = ("name", "slug")


@admin.register(ReviewSettings)
class ReviewSettingsAdmin(admin.ModelAdmin):
    def has_add_permission(self, request):
        return not ReviewSettings.objects.exists()

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    list_display = ("document_name", "model", "pipeline_mode", "created_at", "finding_count", "has_error")
    list_filter = ("pipeline_mode", "model", "created_at")
    search_fields = ("document_name", "error")
    readonly_fields = ("created_at", "document_name", "model", "pipeline_mode", "filters", "findings", "error")

    @admin.display(description="Замечаний")
    def finding_count(self, obj):
        return len(obj.findings)

    @admin.display(boolean=True, description="Ошибка")
    def has_error(self, obj):
        return bool(obj.error)

    def has_add_permission(self, request):
        return False
