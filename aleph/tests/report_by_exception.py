from aleph import Connection


class MyConn(Connection):

    def __init__(self):
        super().__init__()
        self.report_by_exception = True

        self.i = -1
        self.j = -1

    def on_read_error(self, error):
        error.raise_exception()

    def read(self, key, **kwargs):

        if key == "key_with_ids":

            data = [
                [{"id_": "1", "a": 1}, {"id_": "2", "a": 1}, {"id_": "3", "a": 2}],
                [{"id_": "1", "a": 1}, {"id_": "2", "a": 5}, {"id_": "3", "a": 2}],
                [{"id_": "1", "a": 2}, {"id_": "4", "a": 1}, {"id_": "5", "a": 2}],
            ]

            self.i += 1
            return data[self.i]

        if key == "key_without_ids":

            data = [
                {"a": 1, "b": 2, "c": 3},
                {"a": 1, "b": 5, "c": 3},
                {"a": 2, "b": 5, "c": 4},
            ]

            self.j += 1
            return data[self.j]


M = MyConn()
print(M.safe_read("key_without_ids"))
print(M.safe_read("key_without_ids"))
print(M.safe_read("key_without_ids"))

print(M.local_storage.get(M.local_storage.PAST_VALUES))
