import os
import csv
import pandas as pd
import re

# UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;
# tokens = ['UPDATE', 'table_name', 'SET', 'column1 = value1, column2 = value2, ...', 'WHERE', 'column = value']
def update_row(path,value_dict,cond):
    df = pd.read_csv(path)
    index = df.index
    condition = df[cond[0]] == cond[1]
    apples_indices = index[condition]
    # get only rows with "apple"

    apples_indices_list = apples_indices.tolist()
    new_df = pd.DataFrame(value_dict,index=apples_indices_list)
    df.update(new_df)
    df.to_csv(path, index=False)

def update_whole_row(path,value_dict):
    df = pd.read_csv(path)
    index = df.index
    condition = df['index'] == df['index']
    apples_indices = index[condition]
    # get only rows with "apple"

    apples_indices_list = apples_indices.tolist()
    new_df = pd.DataFrame(value_dict,index=apples_indices_list)
    df.update(new_df)
    df.to_csv(path, index=False)



def update(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.
    try:
        table_name = token[1]
        # what if this table not exist

        path = os.path.join(root_1, table_name+".csv")
        if token[4].lower()=='where':
        # update condition rows in table
            # TODO check columns exist,len right
            value_list = re.split(' |=|,',token[3])
            value_list = [i for i in value_list if i]
            value_dict = {}
            i=0
            while i+1 < len(value_list):
                print(i)
                value_dict[value_list[i]]=value_list[i+1]
                print("++++++++")
                i+=2
            
            # TODO check condition exist
            condition = re.split(' |=|,',token[5])
            condition = [i for i in condition if i]

            update_row(path,value_dict,value_dict)
        else:
        # update all row
            update_whole_row(path,index)
    except:
        print("something went wrong, may be table name is wrong .")


path = "play.csv"
# update_row(path,{'name': 'ame', 'id': 'v'},['sha','what'])
update_whole_row(path,{'name': 'ame', 'id': 'v'})