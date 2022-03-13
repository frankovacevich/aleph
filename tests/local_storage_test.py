from aleph.common.local_storage import *

# L = LocalStorage()
# L = FileLocalStorage("test.txt")
# L = SqliteDictLocalStorage("test.db")
L = RedisLocalStorage("test2")

print()
print()
print()

print(L.get("hello"))
print(L.set("hello", 66))
print(L.get("hello"))

print()
print()
print()

print(L.get("hij"))
print(L.set("hij", {"x": 5}))
print(L.get("hij"))
print(L.set("hij", {"x": {"y": 9}}))
print(L.get("hij"))
