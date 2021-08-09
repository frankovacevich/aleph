import datetime
from dateutil.tz import tzutc, tzlocal
from dateutil import parser


def __format_data_for_saving__(data):
    # Flatten dict
    flattened_data = {}
    __flatten_dict__(data, flattened_data, ".", '')
    data = flattened_data

    # Check that "t" value is present
    if "t" not in data: raise Exception("Data missing 't' (timestamp) field")

    # Change time from local to UTC
    if isinstance(data["t"], datetime.datetime):
        data["t"] = data["t"].astimezone(tzutc()).strftime("%Y-%m-%dT%H:%M:%SZ")
    elif not data["t"].endswith("Z"):
        data["t"] = parser.parse(data["t"]).astimezone(tzutc()).strftime("%Y-%m-%dT%H:%M:%SZ")

    return data

def __flatten_dict__(source, result={}, separator=".", root=''):
    for key in source:
        new_root = root + separator + key
        if root == '':
            new_root = key
        if isinstance(source[key], dict):
            __flatten_dict__(source[key], result, separator, new_root)
        else:
            result[new_root] = source[key]
    return

def __parse_date__(date, round_if_string = False):
    """
    Parses a date to a datetime object; date can be:
    - int (days from today)
    - string (ISO datetime string)
    - empty string ("",  current datetime)
    - datetime.datetime object

    The datetime returned is in UTC format (international time)

    Use round_if_string to round a date "YYYY-MM-DD" to "YYYY-MM-DD 23:59:59"
    instead of "YYYY-MM-DD 00:00:00"
    """
    # transform since and until into datetimes
    if isinstance(date, int):
        date = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=date)
    elif date == "":
        if round_if_string:
            date = datetime.datetime.today().replace(hour=23, minute=59, second=59).astimezone(tzutc())
        else:
            date = datetime.datetime.today().replace(hour=0, minute=0, second=0).astimezone(tzutc())
    elif isinstance(date, str):
        date = parser.parse(date).astimezone(tzutc())
        if round_if_string and date.hour == 0 and date.minute == 0 and date.second == 0:
            date = date.replace(hour=23, minute=59, second=59)
    elif isinstance(date, datetime.datetime):
        date = date.astimezone(tzutc())
    return date
