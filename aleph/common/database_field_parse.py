

def db_parse_key(key):
    return key.replace("/", ".").replace(".", "_")


def db_parse_field(field):
    return field.replace("/", ".").replace(".", "__")


def db_deparse_field(field):
    return field.replace("__", ".")


def db_parse_record_fields(record):
    return {f: db_parse_field(record[f]) for f in record}


def db_deparse_record_fields(record):
    print(record)
    return {f: db_deparse_field(record[f]) for f in record}


def db_check_value(v):
    import math
    if isinstance(v, int):
        if v > 2147483647: return 2147483647
        if v < -2147483647: return -2147483647
    if isinstance(v, float):
        if math.isinf(v) or math.isnan(v): return None
    return v
