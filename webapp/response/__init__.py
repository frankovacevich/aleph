from .json import JsonResponse
from .csv import CsvResponse
from .excel import ExcelResponse
from .pdf import PdfResponse


class GenericResponse:

    def __init__(self, request, title, response_format="json", models={}):
        if response_format == "csv": response = CsvResponse(request, title=title)
        elif response_format == "excel": response = ExcelResponse(request, title=title)
        elif response_format == "pdf": response = PdfResponse(request, title=title)
        else: response = JsonResponse(request, title=title)
        self.response = response

    def add_content(self, data, label="data"):
        self.response.add_content(data, label)

    def make(self):
        self.response.make()
