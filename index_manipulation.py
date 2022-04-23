### Index

import os
import csv
from sql_parser import *
from BTrees.OOBTree import OOBTree
import pickle

def index_functions(sql_tokens, current_database):
    if current_database == None:
        print("You must choose the database! Please enter: USE OUR_DATABASE")
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
                    try:
                        # if row[0] == table_name and row[1] != index_name:
                        #     print("Different index exists. Please drop it first!")
                        #     #print("#Example: DROP INDEX 'index_name' ON 'table_name'")
                        #     flag = 0
                        if row[0] == table_name and row[1] == index_name:
                            print("This index already exists! Please drop it first!")
                            flag = 0
                    except:
                        flag = 2
            f.close()

        if flag == 2:
            with open(path, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([table_name, index_name, column_name])
            f.close()

            # create indexing structure
            T=OOBTree()
            table_root = os.path.join(root_1, table_name + ".csv")
            index_dict = {}
            with open(table_root, 'r') as f:
                reader = csv.reader(f)
                row_number = -2
                for row in reader:
                    row_number += 2
                    columns=row
                    loc=columns.index(column_name)
                    break

                for row in reader:
                    row_number+=1
                    if row[loc] in index_dict:
                        index_dict[row[loc]][row_number] = row_number
                    else:
                        index_dict[row[loc]] = {}
                        index_dict[row[loc]][row_number] = row_number
            # print(index_dict)
            T.update(index_dict)
            # save the hash table
            index_file = os.path.join(root_1, table_name + '_' + index_name + ".pkl")
            with open(index_file, 'wb') as f:
                pickle.dump(T, f)
            print("Index created successfully!")

    # Drop Index
    # DROP INDEX index_name ON table_name;
    elif sql_tokens[0] == "drop" and sql_tokens[3] == "on":
        table_name = sql_tokens[4]
        index_name = sql_tokens[2]

        path = os.path.join(root_1, "index.csv")
        if not os.path.exists(path):
            print("Error! Please enter a command with correct syntax!")

        lines = list()
        with open(os.path.join(root_1, "index.csv"), 'r')as f:
            reader = csv.reader(f)
            for row in reader:
                if row[0] == table_name and row[1]==index_name:
                    continue
                else:
                    lines.append(row)
        f.close()

        # Use "w" to overwrite
        with open(os.path.join(root_1, "index.csv"), 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(lines)
        f.close()

        # remove indexing structure
        index_file = os.path.join(root_1, table_name + '_' + index_name + ".pkl")
        try:
            os.remove(index_file)
            print("Index %s dropped successfully" % index_name.upper())
        except Exception as e:
            print(e)
        return

    else:
        print("Error! Please enter a command with correct syntax!")
        return None


# index_functions(['create','index','index1','on','play','(index)'],'play')
# index_functions(['create','index','index2','on','play','(name)'],'play')
#index_functions(['drop','index','index1','on','play','(index)'],'play')
