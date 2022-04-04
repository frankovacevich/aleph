
def key_match(key, root):
    if root.startswith(key): return True
    if root.endswith("#") and key.startswith(root.replace("#", "")): return True
    return False
