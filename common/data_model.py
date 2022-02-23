import uuid
from decimal import Decimal
from ..common.datetime_functions import *

"""

Base types     SQL default type
-------------------------------------------
bool           BOOL
int            INTEGER
float          FLOAT
string         VARCHAR(255)


Derived types     Base type       Details                                  SQL type
----------------------------------------------------------------------------------------------------------------
id                string          PRIMARY KEY, INDEXED                     VARCHAR(255)
auto              int             PRIMARY KEY, AUTOINCREMENT, INDEXED      INTEGER
datetime          string          YY-mm-dd HH:MM:SS.SSSSSS                 DATETIME(6)
time              string          HH:MM:SS.SSSSSS                          TIME(6)
date              string          YY-mm-dd                                 DATE
decimal           float           max_digits=10, decimal_places=2          DECIMAL(max_digits, decimal_places)
bigint            int                                                      BIGINT
text              string          max_length=255                           VARCHAR(max_length)
foreign_key       string          


Html Input types      Base type
------------------------------------------------------
checkbox              bool
number                int, float
text                  string
email                 string
url                   string
textarea              string
date                  string
datetime              string
time                  string
select                string, int
hidden                boolean, int, float, string

"""


def cap_value(x, max_x, min_x=None):
    if min_x is None: min_x = -max_x
    if min_x <= x <= max_x: return x
    elif x > max_x: return max_x
    elif x < min_x: return min_x


class DataModel:

    def __init__(self, key, fields):
        self.key = key
        self.fields = {field.name: field for field in fields}

        # Database storing
        self.db_table = ""

    def create(self):
        """
        Create table in database
        """
        pass

    def validate(self, record):
        """
        Validate record
        """
        pass

    def parse(self, record):
        """
        Parse record
        """
        new_record = {}

        # Parse each field
        for field in record:
            if field in self.fields:
                parser_ = self.fields[field].parser
                if parser_ is None:
                    new_record[field] = record[field]
                else:
                    try:
                        new_record[field] = parser_(record[field])
                    except:
                        pass

        # Check that all required fields are present
        for field in self.fields:
            if self.fields[field].required and field not in new_record:
                return {}

        return new_record


class DataField:

    def __init__(self, name, **kwargs):
        self.name = name
        self.type = ""

        # Validation
        self.validator = None   # function(x) that returns True if the value is valid
        self.parser = None      # function(x) that takes a value and returns another value compatible with the type
        self.required = False

        # Database storing
        self.unique = False
        self.default = None
        self.indexed = False
        self.db_column = ""
        self.autoincrement = False

        # Html form
        self.html_input = None
        self.html_label = ""
        self.html_help = ""
        self.html_read_only = False

        # Update with kwargs
        self.__dict__.update(kwargs)

    # ==================================================================================================================
    # Default field types
    # ==================================================================================================================

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
    def bool(name, **kwargs):
        return DataField(name=name,
                         type="bool",
                         validator=lambda x: isinstance(x, bool),
                         parser=lambda x: True if x else False,
                         html_input="checkbox",
                         **kwargs)

    @staticmethod
    def id(name, **kwargs):
        return DataField(name=name,
                         type="id",
                         validator=lambda x: isinstance(x, str),
                         parser=str,
                         default=str(uuid.uuid4()),
                         html_input="text",
                         **kwargs)

    @staticmethod
    def autoid(name, **kwargs):
        return DataField(name=name,
                         type="autoid",
                         autoincrement=True,
                         html_input="hidden",
                         **kwargs)

    @staticmethod
    def foreign_key(name, namespace_key, foreign_field, foreign_display, **kwargs):
        return DataField(name=name,
                         foreign_key=namespace_key,        # Namespace key (table) to which the foreign key points
                         foreign_field=foreign_field,      # Related field in the foreign key (usually 'id_')
                         foreign_display=foreign_display,  # A string (field) or a function (of record) (see docs)
                         html_input="select",
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
                         parser=lambda x: parse_date_to_string(x).split("T")[0],
                         html_input="date",
                         **kwargs)

    @staticmethod
    def datetime(name, **kwargs):
        return DataField(name=name,
                         type="datetime",
                         validator=lambda x: isinstance(x, str),
                         parser=lambda x: parse_date(x).strftime("%Y-%m-%d %H:%M:%S"),
                         html_input="datetime",
                         **kwargs)


