import os
import csv
database_name_list = []

def get_exist_database(path):
    for f in os.listdir(path):
        if f not in database_name_list:
            database_name_list.append(f)
    #print(database_name_list)

def databse_functions(sql_tokens):

    # Exit system
    # exit_command = ["exit", "Exit", "exit()", "Exit()"]
    # if sql_tokens[0] in exit_command:
    #     print("Have a good day! Bye!")
    #     return None

    first_token = sql_tokens[0]
    second_token = sql_tokens[1]
    root = os.path.join(os.getcwd(), "Database_System")

    ### Database_System
    get_exist_database(root)

    # Create database
    # eg: create DATABASE RUNOOB;
    if first_token == "create" and second_token == "database":
        database_name = sql_tokens[2]

        # Database exists
        if database_name in database_name_list:
            print("Woops! This database already exists!")
            database_name_list.append(database_name)

        else:
            path = os.path.join(root, database_name)
            os.mkdir(path)
            print("Created successfully!")
            # use csv to save tables
            with open(os.path.join(path, "table_name.csv"), "w", encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["Table Name"])
            f.close()
        return None

    # Drop database
    # eg: drop database RUNOOB;
    if first_token == "drop" and second_token == "database":
        database_name = sql_tokens[2]

        # Database exists
        if database_name not in database_name_list:
            print("Woops! Can not find this database.")
        else:
            os.removedirs(os.path.join(root, database_name))
            print("Dropped successfully!")
        return None

    # Use database; Pay attention to database name!
    # eg:use RUNOOB;
    if first_token == "use":
        database_name = second_token
        if database_name in database_name_list:
            current_database = database_name
            print("The current database is %s." % database_name.upper())
            return current_database
        else:
            print("Woops! Can not find this database.")
            return None

    return "Error!"


