from django.urls import path
from . import views


urlpatterns = [
	#path('<str:resource_type>/<str:resource_id>', views.resource, name='resource'),
	#path('<str:resource_type>/<str:resource_id>/<str:date>', views.resource, name='resource'),
	path('<path:resource_path>', views.res_, name='resource'),
]
