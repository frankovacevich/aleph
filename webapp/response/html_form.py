from .response import Response
from django.http import JsonResponse as DjangoJsonResponse


class HtmlFormResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)
        self.status = 200

    def add_content(self, data, label="data"):
        pass

    def get_html(self):
        pass

    def make(self):
        pass
