import datetime
from dateutil.tz import tzutc, tzlocal
from dateutil import parser
import pytz


# ======================================================================================================================
# Datetime functions
# ======================================================================================================================

def now(string=False):
    """
    Returns current datetime in UTC as datetime.datetime (default) or string
    """
    t = datetime.datetime.today().replace(tzinfo=tzlocal()).astimezone(tzutc())
    if string: return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    else: return t


def parse_date(date, round_if_string=False):
    """
    Parses a date to a datetime object; date can be:
    - int (days from today) or int (unix timestamp)
    - string (ISO datetime string)
    - empty string ("", current datetime)
    - datetime.datetime object

    The datetime returned is in UTC (international time)

    Use round_if_string to round a date "YYYY-MM-DD" to the next day
    (previously it was "YYYY-MM-DD 23:59:59") instead of "YYYY-MM-DD 00:00:00"
    """

    # int (days from today)
    if isinstance(date, int) and date < 1000:
        date = datetime.datetime.now().astimezone(tzutc()) - datetime.timedelta(days=date)

    # unix timestamp
    elif isinstance(date, int):
        date = datetime.datetime.utcfromtimestamp(date)

    # empty string
    elif date == "":
        date = datetime.datetime.today().replace(hour=0, minute=0, second=0).astimezone(tzutc())
        if round_if_string: date = date + datetime.timedelta(days=1)

    # string
    elif isinstance(date, str):
        date = parser.parse(date).astimezone(tzutc())
        if round_if_string and date.hour == 0 and date.minute == 0 and date.second == 0:
            date = date + datetime.timedelta(days=1)

    # datetime object
    elif isinstance(date, datetime.datetime):
        date = date.astimezone(tzutc())

    return date


def date_to_string(date, timezone="UTC"):
    """
    Takes a datetime.datetime and returns a string in the given timezone
    """
    if timezone == "UTC": date = date.astimezone(tzutc())
    elif timezone == "Local": date = date.astimezone(tzlocal())
    else: date = date.astimezone(pytz.timezone(timezone))

    if timezone == "UTC": return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    else: return date.strftime("%Y-%m-%d %H:%M:%S")


def parse_date_to_string(date, timezone="UTC"):
    """
    Shortcut to date_to_string(parse_date(date), timezone)
    """
    if timezone == "UTC" and isinstance(date, str) and date.endswith("Z"): return date

    date = parse_date(date)
    if timezone == "UTC": date = date.astimezone(tzutc())
    elif timezone == "Local": date = date.astimezone(tzlocal())
    else: date = date.astimezone(pytz.timezone(timezone))

    if timezone == "UTC": return date.strftime("%Y-%m-%dT%H:%M:%SZ")
    return date.strftime("%Y-%m-%d %H:%M:%S")


def parse_time_to_string(time):
    """
    Receives a time and returns a string HH:MM:SS.ffffff
    """
    return parser.parse(time).strftime("%H:%M:%S.%f")

