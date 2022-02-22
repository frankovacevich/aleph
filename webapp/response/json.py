from .response import Response
from django.http import JsonResponse as DjangoJsonResponse


class JsonResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)
        self.status = 200

    def make(self):
        return DjangoJsonResponse(self.content, safe=False, json_dumps_params={"default": str}, status=self.status)
