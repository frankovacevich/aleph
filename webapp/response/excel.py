from .response import Response
import openpyxl

from django.http import JsonResponse as DjangoJsonResponse


COLUMN_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
COLUMN_LETTERS = [x for x in COLUMN_LETTERS]


class ExcelResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)
        self.status = 200

    def make(self):
        return DjangoJsonResponse(self.content, safe=False, json_dumps_params={"default": str}, status=self.status)
