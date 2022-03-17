import datetime
from dateutil.tz import tzutc, tzlocal
from dateutil import parser
import pytz
import time


# ======================================================================================================================
# Datetime functions
# ======================================================================================================================

def now(string=False, unixts=False):
    """
    Returns current datetime in UTC as datetime.datetime (default) or string
    """
    t = datetime.datetime.today().replace(tzinfo=tzlocal()).astimezone(tzutc())
    if string: return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    elif unixts: return float(int(time.time()))
    else: return t


def parse_datetime(date, round_if_string=False):
    """
    Parses a date to a datetime object; date can be:
    - int (seconds from today)
    - float (unix timestamp, seconds since Jan 01 1970 UTC)
    - string (ISO datetime string)
    - empty string ("", current datetime)
    - datetime.datetime object

    The datetime returned is in UTC (international time)

    Use round_if_string to round a date "YYYY-MM-DD" to the next day
    (previously it was "YYYY-MM-DD 23:59:59") instead of "YYYY-MM-DD 00:00:00"
    """

    # int (seconds from today)
    if isinstance(date, int):
        date = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(seconds=date)

    # unix timestamp
    elif isinstance(date, float):
        date = datetime.datetime.utcfromtimestamp(date).replace(tzinfo=tzutc())

    # empty string
    elif date == "":
        date = datetime.datetime.today().replace(hour=0, minute=0, second=0).astimezone(tzutc())
        if round_if_string: date = date + datetime.timedelta(days=1)

    # string
    elif isinstance(date, str):

        # parse string to int
        if date.isdecimal():
            date = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(seconds=int(date))

        # try parse
        else:
            date = parser.parse(date).astimezone(tzutc())
            if round_if_string and date.hour == 0 and date.minute == 0 and date.second == 0:
                date = date + datetime.timedelta(days=1)

    # datetime object
    elif isinstance(date, datetime.datetime):
        date = date.astimezone(tzutc())

    return date


def datetime_to_string(date, timezone="UTC"):
    """
    Takes a datetime.datetime and returns a string in the given timezone
    """
    if timezone == "UTC": date = date.astimezone(tzutc())
    elif timezone == "Local": date = date.astimezone(tzlocal())
    else: date = date.astimezone(pytz.timezone(timezone))

    if timezone == "UTC": return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    else: return date.strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime_to_string(date, timezone="UTC"):
    """
    Shortcut to date_to_string(parse_date(date), timezone)
    """
    if timezone == "UTC" and isinstance(date, str) and date.endswith("Z"): return date

    date = parse_datetime(date)
    if timezone == "UTC": date = date.astimezone(tzutc())
    elif timezone == "Local": date = date.astimezone(tzlocal())
    else: date = date.astimezone(pytz.timezone(timezone))

    if timezone == "UTC": return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    return date.strftime("%Y-%m-%d %H:%M:%S")


def parse_time_to_string(t):
    """
    Receives a time and returns a string HH:MM:SS.ffffff
    """
    return parser.parse(t).strftime("%H:%M:%S.%f")


def parse_date_to_string(date):
    """
    Parses only a date (not a datetime)
    """
    return parser.parse(date).strftime("%Y-%m-%d")
