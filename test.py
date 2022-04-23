from sql_parser import *

sql = "Rel-i-i-1000"
sql_tokens = parse_sql(sql)
print(sql_tokens)