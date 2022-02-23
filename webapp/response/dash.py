from .response import Response
from django.shortcuts import render
import json


class DashResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)
        self.context = {}

    def add_content(self, data, label="data"):
        if "data" not in self.context: self.context["data"] = {}
        self.context["data"][label] = data

    def make(self):
        self.context["app_name"] = self.title
        self.context["init_args"] = json.loads(
            json.dumps({x: {"value": self.context[x]} for x in self.context}, default=str)
        )
        return render(self.request, "base/dash.html", self.context)
