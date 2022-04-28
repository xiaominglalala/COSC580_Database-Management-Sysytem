import os
import csv
import pandas as pd
import sql_parser
import re

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

# UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;
# tokens = ['UPDATE', 'table_name', 'SET', [column1 = value1, column2 = value2], 'WHERE', [('condition', '=', '1 '), 'or', (' aaa', '=', '2')]]
def update_row(path,value_dict,cond):
    df = pd.read_csv(path)
    columns = df.columns.values.tolist()
    # df = df.astype(str)
    index = df.index
    # condition = df
    apples_indices_list = []
    for tup in cond:
        if tup == 'and' or tup == 'or':
            apples_indices_list.append(tup)
        else:
            operator = tup[1]
            condition = df
            # num = tup[2]
            # print(num)
            try:
                num = int(tup[2])
            except:
                num=tup[2]

            try:
                att = int(condition[tup[0]])
            except:
                att=condition[tup[0]]
            # print(att)
            # print(num)
            if operator == "=":
                condition = att == num
            elif operator == "!=":
                condition = att != num
            elif operator == "<":
                condition = att < num
            elif operator == ">":
                condition = att > num
            elif operator == "<=":
                condition = att <= num
            elif operator == ">=":
                condition = att >= num

            apples_indices = index[condition]
            # print(condition)

            apples_indices_list.append(apples_indices.tolist())
    # print(apples_indices_list)
  
    while len(apples_indices_list)>1:
        # print(apples_indices_list)
        if apples_indices_list[1] == 'and':
            set1 = set(apples_indices_list[0])
            set2 = set(apples_indices_list[2])
            # iset = set1.intersection(set2)
            iset = set1 & set2
            del apples_indices_list[0:3]
            
            apples_indices_list.insert(0,list(iset))
            
        elif apples_indices_list[1] == 'or':
            set1 = set(apples_indices_list[0])
            set2 = set(apples_indices_list[2])
            iset = set1 | set2
            del apples_indices_list[0:3]
            apples_indices_list.insert(0,list(iset))
        else:
            break
            
    new_df = pd.DataFrame(value_dict,index=apples_indices_list[0])
    # df.update(new_df)
    # df.to_csv(path, index=False)
    # print('Update Done!')
    if set(columns) >= set(value_dict.keys()):
        df.update(new_df)
        df.to_csv(path, index=False)
        print('Update Done!')
    else:
        print("No matching columns")

def update_whole_row(path,value_dict):
    df = pd.read_csv(path)
    # df = df.astype(str)
    columns = df.columns.values.tolist()
    # print(columns)
    # print(value_dict.keys())
    index = df.index
    condition = df['index'] == df['index']
    apples_indices = index[condition]
    # get only rows with "apple"

    apples_indices_list = apples_indices.tolist()
    new_df = pd.DataFrame(value_dict,index=apples_indices_list)
    if set(columns) >= set(value_dict.keys()):
        df.update(new_df)
        df.to_csv(path, index=False)
        print('Update Done!')
    else:
        print("No matching columns")



def update(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.
    try:
        table_name = tokens[1]
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



        # TODO check columns exist,len right
        value_list = re.split(' |=|,',','.join(tokens[3]))
        value_list = [i for i in value_list if i]
        value_dict = {}
        i=0
        while i+1 < len(value_list):
            # print(i)
            value_dict[value_list[i]]=value_list[i+1]
            # print("++++++++")
            i+=2
        # print(value_dict)
        if len(tokens)>4:
        # update condition rows in table
            
            
            # TODO check condition exist
            condition = tokens[5]
            # condition = [i for i in condition if i]

            if key_flag:
                if key in value_dict.keys():
                    key_value = value_dict[key]
                    if check_dub(path,key_index,key_value):
                        pass
                    else:
                        print("Primary Key Duplicate")
                        # exit()
                        return None
                        print("do something")
                else:
                    pass
            else:
                pass

            update_row(path,value_dict,condition)
        else:
        # update all row
            if key_flag:
                if key in value_dict.keys():
                    key_value = value_dict[key]
                    if check_dub(path,key_index,key_value):
                        pass
                    else:
                        print("Primary Key Duplicate")
                        # exit()
                        return None
                        print("do something")
                else:
                    pass
            else:
                pass

            update_whole_row(path,value_dict)
        # print('Update Done!')
    except:
        print("Something went Wrong.")


# path = "play.csv"
# # update_row(path,{'name': 'ccc'},['id','v'])
# # update_row(path,{'name': 'ttttttt'},[('id','=','c'),'and',('sha','=','3333'),'or',('mmm','=','111')])
# update_row(path,{'name': 'gggggg'},[('sha','=','2222')])

# update_whole_row(path,{'name': 'ame', 'id': 'v'})
# update(['update', 'employee', 'set', ['emp#=1'], 'where', [('name','=','7')]],'zz')
