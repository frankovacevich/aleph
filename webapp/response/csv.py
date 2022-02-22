import csv
from django.http import HttpResponse
from .response import Response


class JsonResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)

    def make(self):
        response = HttpResponse(
            content_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="' + self.title + '.csv"'},
        )

        csv_writer = csv.writer(response)



