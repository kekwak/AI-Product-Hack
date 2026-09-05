from django.contrib import admin
from .models import OpenRouterModel, Review


@admin.register(OpenRouterModel)
class OpenRouterModelAdmin(admin.ModelAdmin):
    list_display = ("name", "slug", "enabled", "sort_order")
    list_editable = ("enabled", "sort_order")
    list_filter = ("enabled",)
    search_fields = ("name", "slug")


@admin.register(Review)
class ReviewAdmin(admin.ModelAdmin):
    list_display = ("document_name", "model", "created_at", "finding_count", "has_error")
    list_filter = ("model", "created_at")
    search_fields = ("document_name", "error")
    readonly_fields = ("created_at", "document_name", "model", "findings", "error")

    @admin.display(description="Замечаний")
    def finding_count(self, obj):
        return len(obj.findings)

    @admin.display(boolean=True, description="Ошибка")
    def has_error(self, obj):
        return bool(obj.error)

    def has_add_permission(self, request):
        return False
