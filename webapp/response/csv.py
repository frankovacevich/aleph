import csv
from django.http import HttpResponse
from .response import Response
from ...common.dict_functions import flatten_dict


class CsvResponse(Response):

    def __init__(self, request, title=""):
        self.headers = None  # dict {field: header} or list [fields]
        super().__init__(request, title)

    def add_content(self, data, label="data"):
        self.content["data"] = data  # Ignore the label

    def make(self):
        # Build response
        response = HttpResponse(
            content_type='text/csv',
            headers={'Content-Disposition': 'attachment; filename="' + self.title + '.csv"'},
        )
        csv_writer = csv.writer(response)

        # Get data
        data = self.content["data"]

        # Get fields (list) and headers (dict {field: header})
        if isinstance(self.headers, list):
            fields_ = self.headers
            headers_ = {x: x for x in self.headers}
        elif isinstance(self.headers, dict):
            fields_ = list(self.headers.keys())
            headers_ = self.headers
        else:
            fields_ = []
            for i in range(0, len(data)):
                data[i] = flatten_dict(data[i])
                fields_ += [x for x in data[i] if x not in fields_]
            headers_ = {x: x for x in fields_}

        # Add headers to csv
        csv_writer.writerow([headers_[x] for x in fields_])

        # Add data to csv
        for record in data:
            if self.headers is not None: record = flatten_dict(record)
            csv_writer.writerow([record.pop(x, None) for x in fields_])

        # Return response
        return response
