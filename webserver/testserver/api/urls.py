from django.urls import path
from django.views.decorators.csrf import csrf_exempt
from . import views, views_users, views_namespace, views_namespace_manager, views_access_control

urlpatterns = [
    path("auth", csrf_exempt(views.api_auth), name="api_auth"),  # POST

    path("users", csrf_exempt(views_users.users_all), name="users_all"),  # GET / POST
    path("users/new", csrf_exempt(views_users.user_create), name="users_new"),  # POST
    path("users/<str:user>/delete", csrf_exempt(views_users.user_remove), name="users_delete"),  # POST
    path("users/<str:user>/change_password", csrf_exempt(views_users.user_set_password), name="users_all"),  # POST

    #
    path('namespace', csrf_exempt(views_namespace.namespace), name='namespace_get'),  # GET / POST
    path('namespace/<str:key>', csrf_exempt(views_namespace.namespace_fields), name='namespace_fields'),  # GET / POST
    path('namespace/<str:key>/fields', csrf_exempt(views_namespace.namespace_fields), name='namespace_fields'),  # GET / POST
    path('namespace/<str:key>/data', csrf_exempt(views_namespace.namespace_data), name='namespace_data'),  # GET / POST
    path('namespace/<str:key>/publish', csrf_exempt(views_namespace.namespace_publish), name='namespace_publish'),  # POST
    path('namespace/<str:key>/set_extra', csrf_exempt(views_namespace.namespace_set_extra), name='set_extra'),  # POST

    #
    path('access_control', csrf_exempt(views_access_control.access_control_all), name="access_control"),  # GET
    path('access_control/add', csrf_exempt(views_access_control.access_control_add), name="access_control_add"),  # POST
    path('access_control/remove', csrf_exempt(views_access_control.access_control_remove), name="access_control_remove"),  # POST

    #
    path('namespace/<str:key>/rename', csrf_exempt(views_namespace_manager.namespace_rename_key), name='namespace_rename'),  # POST
    path('namespace/<str:key>/remove', csrf_exempt(views_namespace_manager.namespace_remove_key), name='namespace_rename'),  # POST
    # path('namespace/<str:key>/<str:field>/rename', csrf_exempt(views_namespace_manager.namespace_rename_field), name='namespace_rename'),  # POST
    # path('namespace/<str:key>/<str:field>/delete', csrf_exempt(views_namespace_manager.namespace_remove_field), name='namespace_rename'),  # POST
]
