def index_functions(sql_tokens, current_database):
    if current_database == None:
        print("You must choose the database! Please enter: USE DATABASE YOUR_DATABASE")
        print("Replace 'YOUR_DATABASE' with your target database.")
        return None
    if sql_tokens[0] == "create" and sql_tokens[3] == "on":
        table_name = sql_tokens[4]
    elif sql_tokens[0] == "drop" and sql_tokens[3] == "on":
        table_name = sql_tokens[4]
    else:
        print("Error!")
        return None