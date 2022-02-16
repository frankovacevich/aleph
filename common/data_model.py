"""

"""

# Type
# Required
# HTML:
# HtmlInputType
# Help text
# Default value

# Possible:
# - Primary key (other than id_)?
# -

class Types:
    BOOL = 0,
    INT = 1,
    FLOAT = 2,
    STRING = 3,


class HtmlInputTypes:
    TEXT = 0,               # String
    SELECT = 1,             # String, Number
    DATE = 2,               # String
    DATETIME = 3,           # String
    TIME = 4,               # String, Number
    NUMBER = 5,             # Number
    EMAIL = 6,              # String
    TEXTAREA = 7,           # String
    HIDDEN = 8,             # String, Number, Bool
    URL = 9,                # String
    SELECT_MULTIPLE = 10,   # String, Number
    CHECKBOX = 12,          # Bool


class DataModel:

    def __init__(self):
        pass

    def validate(self, data):
        pass

    def load(self, field_types):
        pass


class DataField:

    def __init__(self, field_type):
        self.field_type = field_type
        self.required = True
        self.default_value = None

        self.html_input_type = HtmlInputTypes.TEXT
        self.html_text = ""
        self.html_
