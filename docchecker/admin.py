from django.contrib import admin

from .models import HeadingRule


@admin.register(HeadingRule)
class HeadingRuleAdmin(admin.ModelAdmin):
    list_display = ("title", "level", "is_required", "is_active", "order")
    list_editable = ("level", "is_required", "is_active", "order")
    list_filter = ("is_required", "is_active", "level")
    search_fields = ("title",)
    ordering = ("order", "id")
