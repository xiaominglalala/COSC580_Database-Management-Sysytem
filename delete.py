import os
import csv
import pandas as pd
import sql_parser

# DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';
# tokens = ['DELETE', 'FROM', 'Customers', 'WHERE', [('CustomerName','=',"'Alfreds Futterkiste'""]]
def delete_all_rows(path):
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

    with open(path,'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(attrib)
        f.close()

def delete_row(path,cond):
    df = pd.read_csv(path)
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

    # con = apples_indices_list[0]

    df =  df.drop(index=apples_indices_list[0])
    df.to_csv(path, index=False)


def delete(tokens,database):
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, database) 
    # tokens from parser, should be a list of string after splited input. database is the database we should in.

    # what if no databease seleted? this should be solved in father py file.
    try:
        table_name = tokens[2]
        # what if this table not exist

        path = os.path.join(root_1, table_name+".csv")
        if len(tokens) < 4:
        # delete all rows in table
            delete_all_rows(path)
        else:
        # delete determain row
            if tokens[3].upper() != "WHERE":
            # check sql valid
                print("Error!! it cannot be %s here" % tokens[3])
            else:
                # find index of the row
                condition = tokens[4]
                delete_row(path,condition)
    except:
        print("something went wrong, may be table name is wrong .")


# path = "play.csv"
# delete_row(path,[('id','=','c'),'and',('sha','=','3323'),'or',('mmm','=','111')])

# delete(['delete', 'from', 'play', 'where', [('id','=','c')]],'play')
