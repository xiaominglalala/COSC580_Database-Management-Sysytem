### Table

import os
import csv
from sql_parser import *

def table_functions(sql_tokens, current_database):
    # Create table
    if current_database == None:
        print("You must choose the database! Please enter: USE DATABASE YOUR_DATABASE")
        print("Replace 'YOUR_DATABASE' with your target database.")
        return None

    first_token = sql_tokens[0]
    second_token = sql_tokens[1]
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, current_database)

    if first_token == "create" and second_token == "table":
        table_name = sql_tokens[2]

        # check if table name exists
        f = csv.reader(open(os.path.join(root_1, "table_name.csv")), 'r')
        for i in f:
            if i == table_name:
                print("Woops! This table already exists!")
                return None

        # write table name into table_name.csv
        # 用'a' 才能不覆盖写入; newline解决多空行
        with open (os.path.join(root_1, "table_name.csv"), 'a', encoding = 'utf-8', newline = '') as f:
            writer = csv.writer(f)
            writer.writerow([table_name])
        f.close()

        # Get attribute_names and primary_key
        attribute_list = create_table_parse(sql_tokens)
        primary_key = []
        attribute_names = []
        for attribute in attribute_list:
            attribute = attribute.lstrip()
            # get the primary key
            reg = "primary\s*key.*\((.*)\)+"
            primary = re.compile(reg).findall(attribute)
            if len(primary) > 0:
                primary = primary[0]
                primary_key = primary.split(', ')
            # get attributes
            else:
                attribute = attribute.split(' ')
                attribute_names.append(attribute[0])
        print(attribute_names)
        print(primary_key)

        with open(os.path.join(root_1, "%s.csv" % table_name), 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([table_name])
        f.close()
        print("Create table successfully!")
        return

    # Drop table
    if first_token == "drop" and second_token == "table":
        table_name = sql_tokens[2]
        return
    # Alter table
    if first_token == "alter" and second_token == "table":
        table_name = sql_tokens[2]
        return

    ### Index

    # Create Index
    if first_token == "create" and second_token == "index":
        return

    # Drop Index
    if first_token == "drop" and second_token == "index":
        return

    return "Error!"