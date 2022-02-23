from .response import Response
from ...common.dict_functions import flatten_dict, unflatten_dict
from django.template.loader import render_to_string
from django.http import HttpResponse
from weasyprint import HTML, CSS
import io
import base64


class PdfResponse(Response):

    def __init__(self, request, title=""):
        super().__init__(request, title)
        self.base_template = "base/report.html"
        self.page_size = "A4 portrait"
        self.page_margin = "1in"
        self.context = {}
        self.true_false_values = ("True", "False")

        # Private
        self.html_string = ""

    def add_content(self, data, label="data"):
        self.content[label] = data
        if len(data) == 1:
            self.add_title(label)
            self.add_dict(data[0])
        else:
            self.add_title(label)
            self.add_table(data)

    # ============================================================================
    # Main content
    # ============================================================================
    def add_table(self, data, headers=None):
        table_html = ""

        # Get fields (list) and headers (dict {field: header})
        if isinstance(headers, list):
            fields_ = headers
            headers_ = {x: x for x in headers}
        elif isinstance(headers, dict):
            fields_ = list(headers.keys())
            headers_ = headers
        else:
            fields_ = []
            for i in range(0, len(data)):
                data[i] = flatten_dict(data[i])
                fields_ += [x for x in data[i] if x not in fields_]
            headers_ = {x: x for x in fields_}

        # Merge headers
        # This will create the html for the table headers considering nested levels.
        # For example, if headers are [A.a, A.b, B], then the table should look like this:
        # +-----------------+
        # |     A     |     |
        # +-----+-----+  B  |
        # |  a  |  b  |     |
        # +-----+-----+-----+
        headers_list = [headers_[x] for x in fields_]
        headers_aux = []
        max_nesting_level = 0

        # Split headers and get the max nested level
        for header in headers_list:
            aux = header.split(".")
            if len(aux) > max_nesting_level: max_nesting_level = len(aux)
            headers_aux.append(aux)

        # Create a list 'nested_headers_aux' that contains information about each header
        # row and each header cell. The list contains
        # - one list for each header row
        #   - each header row contains one item for each header
        #     - each item is a tuple of three elements: [header, colspan, rowspan]
        header_rows = []
        for r in range(0, max_nesting_level):
            row = []

            for c in range(0, len(headers_aux)):
                if r + 1 < max_nesting_level and r + 1 == len(headers_aux[c]):
                    row.append([headers_aux[c][r], 1, max_nesting_level - r])
                elif r + 1 > len(headers_aux[c]):
                    row.append([None, 0, 0])
                elif len(row) == 0 or row[-1][0] != headers_aux[c][r]:
                    row.append([headers_aux[c][r], 1, 1])
                else:
                    row[-1][1] += 1

            header_rows.append(row)

        # Add headers html
        for row in header_rows:
            table_html += "<tr>"
            for th in row:
                if th[0] is None: continue
                table_html += f'<th colspan={th[1]} rowspan={th[2]}>{th[0]}</th>'
            table_html += "</tr>"

        # Add data
        for record in data:
            if headers is not None: record = flatten_dict(record)
            table_html += "<tr>"
            for f in fields_:
                value = record.pop(f, "")
                if isinstance(value, bool): value = self.true_false_values[0 if value else 1]
                table_html += f'<td>{value}</td>'
            table_html += "</tr>"

        self.html_string += "<table>" + table_html + "</table>"

    def __recursive_html_dict_generation__(self, dict_, level_=0):
        html_ = ""

        for d in dict_:
            if isinstance(dict_[d], list):
                dict_[d] = {i: dict_[d][i] for i in range(0, len(dict_[d]))}

            if isinstance(dict_[d], dict):
                html_ += f"<p style='margin-left: {level_ * 20}px'><b>{d}</b></p>"
                html_ += self.__recursive_html_dict_generation__(dict_[d], level_ + 1)
            else:
                html_ += f"<p style='margin-left: {level_ * 20}px'><b>{d}: </b>{dict_[d]}</p>"

        return html_

    def add_dict(self, dictionary, headers=None):
        print(dictionary)
        self.html_string += self.__recursive_html_dict_generation__(unflatten_dict(dictionary))

    def add_matplotlib(self, figure, caption="", size=(5, 2)):
        string_io_bytes = io.BytesIO()
        figure.set_size_inches(size)
        figure.savefig(string_io_bytes, format='png')
        string_io_bytes.seek(0)
        img_data = base64.b64encode(string_io_bytes.read())
        self.add_base64_img(img_data, caption)

    def add_base64_img(self, img_data, caption=""):
        figure_html = '<figure>'
        figure_html += f'<img src="data:image/png;base64, {img_data.decode()}">'
        figure_html += f'<figcaption>{caption}</figcaption>'
        figure_html += '</figure>'
        self.html_string += figure_html

    # ============================================================================
    # Other content
    # ============================================================================
    def add_horizontal_line(self):
        self.html_string += "<hr>"

    def add_page_break(self):
        self.html_string += "<div class='pagebreak'></div>"

    def add_title(self, title, level="h1"):
        self.html_string += f"<{level}>{str(title).title()}</{level}>"

    def add_html(self, html):
        self.html_string += html

    def make(self):
        # Get html string content
        html_string = render_to_string(self.base_template, self.context)
        html_string = html_string.replace('<div class="report-body">', '<div class="report-body">' + self.html_string)
        html_string = html_string.replace('@page{ size: A4 portrait; margin: 1in }',
                                          '@page{ size: ' + self.page_size + "; margin: " + self.page_margin + " }")
        # return HttpResponse(html_string)  # For debugging

        # Create pdf file
        html = HTML(string=html_string, base_url=self.request.build_absolute_uri())
        pdf_file = html.write_pdf()

        # Create and return response
        response = HttpResponse(pdf_file, content_type='application/pdf')
        response['Content-Disposition'] = f'filename="{self.title}.pdf"'
        return response
