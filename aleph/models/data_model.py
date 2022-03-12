"""

Records with models must be flat

HTML INPUTS:
checkbox (bool)
number (int, float)
text (string)
email (string)
url (string)
textarea (string)
date (string)
datetime (string)
time (string)
select (string, int)
hidden (boolean, int, float, string)
"""

import uuid
from decimal import Decimal
from ..common.datetime_functions import *


def cap_value(x, max_x, min_x=None):
    if min_x is None: min_x = -max_x
    if min_x <= x <= max_x: return x
    elif x > max_x: return max_x
    elif x < min_x: return min_x


class DataModel:

    def __init__(self, key, fields):
        self.key = key
        self.fields = {field.name: field for field in fields}

        # Optional
        self.db_table = key                            # Table name in db
        self.error_class = ""                          # Class that the input and label will get in case of error
        self.label_add_asterisk_if_required = True     # Add an asterix in the html form

        # Store last validation's errors
        self.fields_with_errors = []

    def validate(self, record):
        """
        Validates the record, checking the validation function
        for each field (after parsing). It also checks if
        required fields are present.

        If valid, returns a new record with parsed data and
        missing fields with default values.
        If not valid, returns None

        If not valid, errors will be stored in the list
        self.fields_with_errors
        """

        # Clear errors and validated record
        self.fields_with_errors.clear()

        # Store validated fields
        new_record = {}

        if "id_" not in record:
            self.fields_with_errors.append("id_")
        else:
            new_record["id_"] = record["id_"]

        if "t" in record:
            new_record["t"] = record["t"]

        for field in self.fields:
            df = self.fields[field]

            # Check if field in record
            if df.name not in record:
                if df.required:
                    self.fields_with_errors.append(df.name)
                elif df.default is not None:
                    new_record[field] = df.default

            # Check if field is valid (may raise an exception)
            else:
                try:
                    # Parse value
                    if df.parser is not None:
                        value = df.parser(record[field])
                    else:
                        value = record[field]

                    # Check validator
                    if df.validator is not None and not df.validator(value):
                        self.fields_with_errors.append(df.name)
                    else:
                        new_record[field] = value

                except:
                    self.fields_with_errors.append(df.name)

        if len(self.fields_with_errors) == 0:
            return new_record
        else:
            return None

    def to_html(self, record={}, as_table=True):
        """
        Returns a html form. The form inputs are organized
        in a table if 'as_table' is True, else they are
        placed one after the other
        If record is {}, returns an empty form (new)
        """
        html = ""

        # Add id_ input
        if "id_" not in record:
            if as_table: html += "<tr><th>"
            html += f'<label for="id_">id_{" (*)" if self.label_add_asterisk_if_required else ""}</label>'
            if as_table: html += "</th><td>"
            html += '<input type="text" name="id_" required></td>'
            if as_table: html += "</td></tr>"

        else:
            html += f'<input type="hidden" name="id_" value="{record["id_"]}">'

        for field in self.fields:
            df = self.fields[field]

            # Add label
            if as_table: html += "<tr><th>"
            html += f'<label for="{df.name}"'
            if df.name in self.fields_with_errors: html += f' class="{self.error_class}"'
            html += '>' + df.html_label
            if self.label_add_asterisk_if_required and df.required: html += " (*)"
            html += '</label>'

            # Add input
            if as_table: html += "</th><td>"

            # Type: select for foreign key
            # TODO

            # Type: select
            if df.html_input == "select":
                html += f'<select name="{df.name}"'
                if df.required: html += " required"
                if df.html_read_only: html += " disabled"
                html += ">"
                for c in df.choices:
                    html += f'<option value={c}'
                    if df.name in record:
                        if c == record[df.name]: html += " selected"
                    elif df.default == c:
                        html += " selected"
                    html += f'>{df.choices[c]}</option>'
                html += "</select>"

            # Other types
            else:
                html += f'<input type="{df.html_input}" name="{df.name}" placeholder="{df.html_help}"'
                if df.name in self.fields_with_errors: html += f' class="{self.error_class}"'
                if df.name in record: html += f' value="{record[df.name]}"'
                elif df.default is not None:
                    if df.type == "bool": html += "checked"
                    else: html += f' value="{df.default}"'
                if df.required: html += f' required'
                if df.html_read_only: html += f' readonly'
                html += '>'

            if as_table: html += "</td></tr>"
            continue

        return html


class DataField:

    def __init__(self, name, **kwargs):
        self.name = name
        self.type = "text"

        # Validation
        self.validator = None   # function(x) that returns True if the value is valid
        self.parser = None      # function(x) that takes a value and returns another value compatible with the type
        self.required = False

        # Database storing
        self.unique = False
        self.default = None
        self.indexed = False
        self.db_column = name

        # Html form
        self.html_input = None
        self.html_label = name.replace("_", " ").replace(".", " / ").title()
        self.html_help = ""
        self.html_read_only = False

        # Update with kwargs
        self.__dict__.update(kwargs)

    # ==================================================================================================================
    # Default field types
    # ==================================================================================================================

    @staticmethod
    def text(name, max_length=255, **kwargs):
        if max_length > 65535: raise Exception("Text max_length must be between 0 and 65535")

        return DataField(name=name,
                         type="text",
                         max_length=max_length,
                         validator=lambda x, l=max_length: isinstance(x, str) and len(x) <= l,
                         parser=str,
                         html_input="select" if "choices" in kwargs else "text",
                         **kwargs)

    @staticmethod
    def integer(name, **kwargs):
        return DataField(name=name,
                         type="int",
                         validator=lambda x: isinstance(x, int),
                         parser=int,
                         html_input="select" if "choices" in kwargs else "number",
                         **kwargs)

    @staticmethod
    def float(name, **kwargs):
        return DataField(name=name,
                         type="float",
                         validator=lambda x: isinstance(x, float),
                         parser=float,
                         html_input="number",
                         **kwargs)

    @staticmethod
    def bool(name, **kwargs):
        return DataField(name=name,
                         type="bool",
                         validator=lambda x: isinstance(x, bool),
                         parser=lambda x: True if x else False,
                         html_input="checkbox",
                         **kwargs)

    @staticmethod
    def foreign_key(name, namespace_key, foreign_display, **kwargs):
        return DataField(name=name,
                         foreign_key=namespace_key,        # Namespace key (table) to which the foreign key points
                         foreign_display=foreign_display,  # A string (field) or a function (of record) (see docs)
                         html_input="select",
                         parser=str,
                         **kwargs)

    @staticmethod
    def biginteger(name, **kwargs):
        bigint_max = 9223372036854775807
        return DataField(name=name,
                         type="biginteger",
                         validator=lambda x, m=bigint_max: isinstance(x, int) and -m < x < m,
                         parser=lambda x, m=bigint_max: int(cap_value(x, m)),
                         html_input="number",
                         **kwargs)

    @staticmethod
    def decimal(name, max_digits=10, decimal_places=2, **kwargs):
        if max_digits > 65: raise Exception("Decimal max_digits must be less than 65")
        if decimal_places > 10: raise Exception("Decimal decimal_places must be less than 10")

        return DataField(name=name,
                         type="decimal",
                         max_digits=max_digits,
                         decimal_places=decimal_places,
                         validator=lambda x, m=max_digits: isinstance(x, Decimal) and m > x > -m,
                         parser=lambda x: Decimal(str(x)),
                         html_input="number",
                         **kwargs)

    @staticmethod
    def time(name, **kwargs):
        return DataField(name=name,
                         type="time",
                         validator=lambda x: isinstance(x, str),
                         parser=parse_time_to_string,
                         html_input="time",
                         **kwargs)

    @staticmethod
    def date(name, **kwargs):
        return DataField(name=name,
                         type="date",
                         validator=lambda x: isinstance(x, str),
                         parser=parse_date_to_string,
                         html_input="date",
                         **kwargs)

    @staticmethod
    def datetime(name, **kwargs):
        return DataField(name=name,
                         type="datetime",
                         validator=lambda x: isinstance(x, str),
                         parser=lambda x: parse_datetime(x).strftime("%Y-%m-%d %H:%M:%S"),
                         html_input="datetime-local",
                         **kwargs)
