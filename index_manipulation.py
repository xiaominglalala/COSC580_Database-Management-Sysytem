### Index

import os
import csv
from sql_parser import *


def index_functions(sql_tokens, current_database):
    if current_database == None:
        print("You must choose the database! Please enter: USE DATABASE YOUR_DATABASE")
        print("Replace 'YOUR_DATABASE' with your target database.")
        return None

    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, current_database)

    if not (sql_tokens[0] and sql_tokens[1] and sql_tokens[3]):
        print("Error! Please enter a command with correct syntax!")
    # Create Index
    # CREATE INDEX indexName ON table_name (column_name)
    if sql_tokens[0] == "create" and sql_tokens[3] == "on":
        table_name = sql_tokens[4]
        index_name = sql_tokens[2]
        column_name = create_index_parse(sql_tokens)

        # Check if index exists.
        flag = 2
        path = os.path.join(root_1, "index.csv")
        if os.path.exists(path):
            with open(path, 'r')as f:
                reader = csv.reader(f)
                for row in reader:
                    if row[0] == table_name and row[1] != index_name:
                        print("Different index exists. Please drop it first!")
                        #print("#Example: DROP INDEX 'index_name' ON 'table_name'")
                        flag = 0
                    elif row[0] == table_name and row[1] == index_name:
                        print("This index already exists! Please drop it first!")
                        flag = 0
                    else:
                        flag = 2
            f.close()

        if flag == 2:
            with open(path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([table_name, index_name, column_name])
            f.close()
            print("Index created successfully!")




    # Drop Index
    # DROP INDEX [indexName] ON mytable;
    elif sql_tokens[0] == "drop" and sql_tokens[3] == "on":
        table_name = sql_tokens[4]
        index_name = sql_tokens[2]
    else:
        print("Error!")
        return None