from django.contrib import admin

# Register your models here.

from .models import ActivityLog
from .models import AccessControl
from .models import ApiToken


class UserActivityLogAdmin(admin.ModelAdmin):
    list_display = ('date', 'username', 'activity', 'ip_address')


class UserAccessControlAdmin(admin.ModelAdmin):
    list_display = ('username', 'key')


class ApiTokenAdmin(admin.ModelAdmin):
    list_display = ('username', 'token', 'last_used')


admin.site.register(ActivityLog, UserActivityLogAdmin)
admin.site.register(AccessControl, UserAccessControlAdmin)
admin.site.register(ApiToken, ApiTokenAdmin)
