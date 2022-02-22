from django.http import HttpResponse


class Response:

    def __init__(self, request, title=""):
        self.title = title
        self.request = request
        self.content = {}

    def add_content(self, data, label="data"):
        self.content[label] = data

    def make(self):
        return HttpResponse("")
