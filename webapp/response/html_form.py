from .response import Response
from django.http import JsonResponse as DjangoJsonResponse


class HtmlFormResponse(Response):

    def __init__(self, request, title="", models={}):
        super().__init__(request, title)
        self.status = 200

    def add_content(self, data, label="data"):
        pass

    def get_form(self):
        """
        Returns only the form's html as string
        """
        pass

    def make(self):
        pass
