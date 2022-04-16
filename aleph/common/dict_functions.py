
def flatten_dict(dictionary, parent_key='', separator='.'):
    """
    Takes a dict and flattens it.

    For example:
    > flatten_dict({"a": {"b": {"c": 1, "d": 2}}, "x": {"y": 3}})
    > {"a.b.c": 1, "a.b.d": 2, "x.y": 3}
    """
    items = []
    for k, v in dictionary.items():
        try:
            items.extend(flatten_dict(v, '%s%s.' % (parent_key, k)).items())
        except AttributeError:
            items.append(('%s%s' % (parent_key, k), v))
    return dict(items)


def unflatten_dict(dictionary):
    """
    Takes a dict and unflattens it.

    For example:
    > unflatten_dict({"a.b.c": 1, "a.b.d": 2, "x.y": 3})
    > {"a": {"b": {"c": 1, "d": 2}}, "x": {"y": 3}}
    """
    if isinstance(dictionary, list):
        dictionary = {i: dictionary[i] for i in range(0, len(dictionary))}

    result = dict()
    for key, value in dictionary.items():
        parts = key.split(".")
        d = result
        for part in parts[:-1]:
            if part not in d: d[part] = dict()
            d = d[part]
        d[parts[-1]] = value
    return result
