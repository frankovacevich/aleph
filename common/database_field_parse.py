

def db_parse_key(key):
    return key.replace(".", "_").replace("/", "_")


def db_parse_field(field):
    return field.replace(".", "_")


def db_deparse_field(field):
    return field
