from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('home', views.home, name='home'),
    path('login', views.ulogin, name='login'),
    path('logout', views.ulogout, name='logout'),
    path('password_change', views.password_change, name='password_change'),

    # path('users', views.users, name='users'),

    path('explorer', views.explorer, name='explorer'),
    # path('reports/base_report', views.resources, name='reports'),

    # path('docs', views.docs, name='docs'),
]