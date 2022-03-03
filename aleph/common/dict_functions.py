
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


def map_dict_keys(list_of_dict, key_map):
    """
    Takes a list of dictionaries and changes the key of each dict to a new set of keys,
    given by key_map = {old_key: new_key}. It returns an iterable.

    For example:
    > L = [{"A": 1, "B": 2, "C": 3}, {"A": 4, "B": 5, "C": 6}, {"A": 7, "B": 8, "C": 9}]
    > m = {"A": "aa", "B": "bb", "C": "cc"}
    > Y = list(map_dict_keys(L, m))
    > [{"aa": 1, "bb": 2, "cc": 3}, {"aa": 4, "bb": 5, "cc": 6}, {"aa": 7, "bb": 8, "cc": 9}]
    """

    def key_map_fun(dictionary):
        d = dictionary.copy()
        for k in key_map:
            d[key_map[k]] = d.pop(k, None)
        return d

    return map(key_map_fun, list_of_dict)
