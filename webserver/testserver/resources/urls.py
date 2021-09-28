from django.urls import path
from . import views


urlpatterns = [
	path('<path:resource_path>', views.res_, name='resource'),
]
