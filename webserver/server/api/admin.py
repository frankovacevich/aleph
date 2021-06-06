from django.contrib import admin

# Register your models here.

from .models import ActivityLog
from .models import AccessControl
from .models import NamespaceExtra
from .models import Resource
from .models import ApiToken


class UserActivityLogAdmin(admin.ModelAdmin):
    list_display = ('date', 'username', 'activity', 'ip_address')


class UserAccessControlAdmin(admin.ModelAdmin):
    list_display = ('username', 'key')


class ResourceAdmin(admin.ModelAdmin):
    list_display = ('uri', 'key')


class DataExplorerHelpAdmin(admin.ModelAdmin):
    list_display = ('key', 'field', 'alias', 'tooltip', 'show_on_explorer')


class ApiTokenAdmin(admin.ModelAdmin):
    list_display = ('username', 'token', 'last_used')


admin.site.register(ActivityLog, UserActivityLogAdmin)
admin.site.register(AccessControl, UserAccessControlAdmin)
admin.site.register(NamespaceExtra, DataExplorerHelpAdmin)
admin.site.register(Resource, ResourceAdmin)
admin.site.register(ApiToken, ApiTokenAdmin)
