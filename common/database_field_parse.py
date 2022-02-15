

def db_parse_key(key):
    return key.replace("/", ".").replace(".", "_")


def db_parse_field(field):
    return field.replace("/", ".").replace(".", "__")


def db_deparse_field(field):
    return field.replace("__", ".")


def db_parse_record_fields(record):
    return {f: db_parse_field(record[f]) for f in record}


def db_deparse_record_fields(record):
    return {f: db_deparse_field(record[f]) for f in record}

