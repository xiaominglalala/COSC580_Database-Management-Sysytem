import os
import csv
import pandas as pd
import sql_parser

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
        print("lack attributes")

def insert_part_row(path,columns,values):
    df2 = dict(zip(columns,values))
    df = pd.read_csv(path)
    df = df.astype(str)
    df =  df.append(df2,ignore_index = True)
    df.to_csv(path, index=False)


def insert(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # print("-----------------------------")
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.
    try:
        table_name = tokens[2]
        # what if this table not exist

        path = os.path.join(root_1, table_name+".csv")
        # print(len(tokens))
        if len(tokens)<6:
        # insert whole rows in table
            values = tokens[4]
            insert_row(path,values)
        else:
        # insert some columns of row
            # print(isinstance(tokens[3],list))
            # print(isinstance(tokens[5],list))
            # print(tokens[4].lower() == "values")
            if "values" in tokens[4].lower() and isinstance(tokens[3],list) and isinstance(tokens[5],list):
                columns = tokens[3]
                values = tokens[5]
                insert_part_row(path,columns,values)
            else:
                # check sql valid
                print("Error!! input is wrong")
                
    except:
        print("something went wrong, may be table name is wrong .")


# path = "play.csv"
# # insert_row(path,['3','john','c'])
# insert_part_row(path,['index','name'],['4','rob'])
# insert(['insert', 'into', 'play', ['index','name'], ' values ', ['4','rob']],'play')

