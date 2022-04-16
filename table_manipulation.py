### Table

import os
import csv
from sql_parser import *

def table_functions(sql_tokens, current_database):

    if current_database == None:
        print("You must choose the database! Please enter: USE DATABASE YOUR_DATABASE")
        print("Replace 'YOUR_DATABASE' with your target database.")
        return None

    first_token = sql_tokens[0]
    second_token = sql_tokens[1]
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, current_database)

    # Create table
    if first_token == "create" and second_token == "table":
        table_name = sql_tokens[2]

        # check if table name exists
        with open(os.path.join(root_1, "table_name.csv"), 'r')as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == table_name:
                    print("Woops! This table already exists!")
                    return None

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
        # 列数 Column Num
        col_num = len(attribute_names)

        # Save primary key:
        with open(os.path.join(root_1, "primary_key.csv"), 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([table_name, primary_key[0]])
        f.close()

        # Write attribute names
        with open(os.path.join(root_1, "%s.csv" % table_name), 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([i for i in attribute_names])
        f.close()

        # Write table name into table_name.csv
        # 用'a' 才能不覆盖写入; newline解决多空行
        with open(os.path.join(root_1, "table_name.csv"), 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([table_name])
        f.close()
        print("Create table successfully!")
        return

    # Drop table
    if first_token == "drop" and second_token == "table":
        table_name = sql_tokens[2]
        table_file = os.path.join(root_1, "%s.csv" % table_name)
        # Delete table file
        if os.path.isfile(table_file):
            os.remove(table_file)

        # Delete table name
        lines = list()
        with open(os.path.join(root_1, "table_name.csv"), 'r')as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] != table_name:
                    lines.append(row)
        # Use "w" to overwrite
        with open(os.path.join(root_1, "table_name.csv"), 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(lines)

        # Delete primary key
        lines = list()
        with open(os.path.join(root_1, "primary_key.csv"), 'r')as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] != table_name:
                    lines.append(row)
        # Use "w" to overwrite
        with open(os.path.join(root_1, "primary_key.csv"), 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(lines)

        print("Table %s dropped successfully" % table_name.upper())
        return

    # Alter table
    if first_token == "alter" and second_token == "table":
        table_name = sql_tokens[2]
        # TODO
        return

    return "Error!"