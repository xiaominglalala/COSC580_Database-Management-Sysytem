import os
import csv
import pandas as pd
import sql_parser

def check_dub(path,key_index,value):
    flag = True
    with open(path,'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, delimiter=',')
        for row in reader:
            if row[key_index] == value:
                flag = False
    f.close()
    if flag:
        return True
    else:
        return False


# INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);
# tokens = ['INSERT', 'INTO', 'table_name', [column1, column2, column3], 'VALUES', [value1, value2, value3]
def insert_row(path,values):
    with open(path,'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f, delimiter=',')
        for i,row in enumerate(reader):
            if i == 0:
                attributes = row
                break
        f.close()
    # print(attributes)
    attrib = []
    attrib.append(attributes)
    # print(attrib)
    # print(values)
    if len(attrib[0]) == len(values):

        with open( path,'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows([values])
    else:
        print("Lack Attributes")

def insert_part_row(path,columns,values):
    df2 = dict(zip(columns,values))
    df = pd.read_csv(path)
    df = df.astype(str)
    columns = df.columns.values.tolist()
    #print(columns)
    new_col = df2.keys()
    #print(new_col)
    if set(columns) >= set(new_col):
        df =  df.append(df2,ignore_index = True)
    else:
        print("Lack Attributes")
    df.to_csv(path, index=False)


def insert(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # print("-----------------------------")
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.


    # try:
    table_name = tokens[2]
    # what if this table not exist

    path = os.path.join(root_1, table_name+".csv")

    key_path = os.path.join(root_1, "primary_key.csv")
    key_flag = os.path.exists(key_path)

    if key_flag:
        with open(key_path,'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                if row[0] == table_name:
                    key = row[1]
                    break
        f.close()

        with open(path,'r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f, delimiter=',')
            for i,row in enumerate(reader):
                if i == 0:
                    key_index = row.index(key)
                    break
        f.close()
    else:
        key = None
        key_index = None
    # print(key)
    # print(key_index)



    # print(len(tokens))
    if len(tokens)<6:
    # insert whole rows in table
        values = tokens[4]

        if key_flag:
            key_value = values[key_index]
            if check_dub(path,key_index,key_value):
                pass
            else:
                print("Primary Key Duplicate")
                # exit()
                return None
                print("do something")
        else:
            pass

        insert_row(path,values)
    else:
    # insert some columns of row
        # print(isinstance(tokens[3],list))
        # print(isinstance(tokens[5],list))
        # print(tokens[4].lower() == "values")
        if "values" in tokens[4].lower() and isinstance(tokens[3],list) and isinstance(tokens[5],list):
            columns = tokens[3]
            # for item in columns:
            #     item
            columns = [i.strip() for i in columns]
            values = tokens[5]

            if key_flag:
                key_value = values[key_index]
                if check_dub(path,key_index,key_value):
                    pass
                else:
                    print("Primary Key Duplicate")
                    # exit()
                    return None
                    print("do something")
            else:
                pass
            # print(columns)
            # print(values)
            insert_part_row(path,columns,values)
        else:
            # check sql valid
            print("Error!! input is wrong")
    print("Insert Done!")
                
    # except:
    #     print("Something went wrong.")


# path = "play.csv"
# # insert_row(path,['3','john','c'])
# # insert_part_row(path,['index','name'],['4','rob'])
# insert(['insert', 'into', 'employee', ' values ', ['3','5','3']],'zz')

