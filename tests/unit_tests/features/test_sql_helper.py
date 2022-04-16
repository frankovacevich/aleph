from aleph.common.sql_helper import SqlHelper
import datetime


S = SqlHelper("mysql")

print(S.clause_where_field_not_null("A1", "B2", "x.y"))
print(S.clause_where_time_since_until("t"))
print(S.clause_where_time_since_until("t", since=datetime.datetime.today()))
print(S.clause_where_time_since_until("t", since=datetime.datetime.today(),
      until=datetime.datetime.today() - datetime.timedelta(days=10)))

print(S.clause_query_count("tonutti.planta.produccion", ["deleted_ IS NOT TRUE"]))
print(S.clause_alter_table_add_column("my.table", "my_column", S.datetime_field()))
print(S.clause_query_table("my.fu", "*",
                           [S.clause_where_field_not_null("X", "y"), "deleted_ IS NOT TRUE"],
                           S.clause_order_by("t", "-X"),
                           S.clause_limit_offset(10, 5)))


print(S.clause_query_insert("my.table", {"x": 2, "y": "HELLO", "z": True, "a.b.c": 1.5}))

