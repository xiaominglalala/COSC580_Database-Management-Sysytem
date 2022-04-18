import os
import csv
import pandas as pd

# INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);
# tokens = ['INSERT', 'INTO', 'table_name', [column1, column2, column3, ...], 'VALUES', [value1, value2, value3, ...)]
def insert_row(path,values):
    with open( path,'a', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([values])

def insert_part_row(path,columns,values):
    df2 = dict(zip(columns,values))
    df = pd.read_csv(path)
    df =  df.append(df2,ignore_index = True)
    print(df)
    df.to_csv(path, index=False)


def delete(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.
    try:
        table_name = token[2]
        # what if this table not exist

        path = os.path.join(root_1, table_name+".csv")
        if token[3].lower() == "values":
        # insert whole rows in table
            values = tokens[4]
            insert_row(path,values)
        else:
        # insert some columns of row
            if token[4].lower() == "values" and isinstance(token[3],list) and isinstance(token[5],list):
                columns = tokens[3]
                values = tokens[5]
                insert_part_row(path,columns,values)
            else:
                # check sql valid
                print("Error!! input is wrong")
                
    except:
        print("something went wrong, may be table name is wrong .")


path = "play.csv"
# insert_row(path,['3','john','c'])
insert_part_row(path,['index','name'],['4','rob'])