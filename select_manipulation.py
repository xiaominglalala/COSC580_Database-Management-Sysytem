import sql_parser
import os
import csv
import numpy as np



def con_manu(array,condition,columns,join_flag):
    value=condition[2]
    operand=condition[1]
    if join_flag:
        col = condition[0]
    else:
        col=condition[0].split('.')[-1]
    condition_index = columns.index(col)
    temp_a1 = array[:, condition_index]
    if operand == "=":
        condition_index = np.where(temp_a1 == value)
    elif operand == "!=":
        condition_index = np.where(temp_a1 != value)
    elif operand == "<":
        condition_index = np.where(temp_a1 < value)
    elif operand == ">":
        condition_index = np.where(temp_a1 > value)
    elif operand == "<=":
        condition_index = np.where(temp_a1 <= value)
    elif operand == ">=":
        condition_index = np.where(temp_a1 >= value)
    return condition_index

def load_data(open_file,col_n):
    a=np.zeros((1, col_n))
    for row in csv.reader(open_file):
        row = np.array(row).reshape(1, col_n)
        a = np.r_[a, row]
    a = np.delete(a, 0, axis=0)
    open_file.close()
    return a

def con_index(conditions,array,columns,join_flag):
    if len(conditions)==0:
        return [i for i in range(len(array))]
    match_rows = []
    if len(conditions)==1:
        match_rows.append(set(con_manu(array, conditions[0], columns, join_flag)[0]))
    else:
        for state in conditions:
            if state != 'and' and state != 'or':
                match_rows.append(set(con_manu(array, state, columns,join_flag)[0]))

    if len(match_rows) == 1:
        indexes = match_rows[0]
    else:
        for state in conditions:
            if state == 'and':
                indexes = set(match_rows[0] & match_rows[1])
                match_rows.pop(0)
                match_rows.pop(0)
                match_rows.insert(0, indexes)
            elif state == 'or':
                t = match_rows.pop(0)
                match_rows.insert(len(match_rows), t)
        i = 0
        while i < len(match_rows) - 1:
            indexes = match_rows[i] | match_rows[i + 1]
            i += 1
    return indexes

def check_data(array,attributes):
    pass

def order_data(array,orders,select_columns,join_flag):
    if len(orders)==0:
        return array
    temp_a=array
    for order in orders:
        if join_flag:
            col=order[0]
        else:
            col=order[0].split('.')[-1]
        index=select_columns.index(col)
        if order[1]=='aesc':
            temp_a=np.array(sorted(temp_a,key=lambda x:x[index]))
        else:
            temp_a=np.array(sorted(temp_a,key=lambda x:x[index],reverse=True))
    return temp_a

def join_table(tables,join_table,database_path):
    table_1, table_2 = tables[0], tables[1]
    col_1, col_2 = join_table[1][0].split('.')[-1], join_table[1][1].split('.')[-1]

    r_table_1 = open(os.path.join(database_path, "{}.csv".format(table_1)), 'r')
    r_table_2 = open(os.path.join(database_path, "{}.csv".format(table_2)), 'r')
    for row in csv.reader(r_table_1):
        columns_1 = row
        break
    for row in csv.reader(r_table_2):
        columns_2 = row
        break
    if col_1 not in columns_1 or col_2 not in columns_2:
        print('No index in the tables')
        return None

    key_index1 = columns_1.index(col_1)
    key_index2 = columns_2.index(col_2)
    a1=load_data(r_table_1,len(columns_1))
    a2=load_data(r_table_2,len(columns_2))

    t = np.tile(a1, (len(a2), 1))
    p = np.repeat(a2, len(a1), axis=0)
    # combine two tables
    final_full = np.column_stack((t, p))
    if join_table[0] == 'inner':
        lines = np.zeros((1,len(columns_1)+len(columns_2)))
        for row in final_full:
            if row[key_index1] == row[key_index2 + key_index1 + 1]:
                row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
        lines = np.delete(lines, 0, axis=0)
        # lines = np.delete(lines, key_index2+key_index1+1, axis=1)
        columns_1 = [table_1 + '.' + i for i in columns_1]
        columns_2 = [table_2 + '.' + i for i in columns_2]
        columns = columns_1 + columns_2
        # columns.pop(key_index2+key_index1+1)

    elif join_table[0] == 'left':
        a1_index = []
        columns_1 = [table_1 + '.' + i for i in columns_1]
        columns_2 = [table_2 + '.' + i for i in columns_2]
        columns = columns_1 + columns_2
        lines = np.zeros((1, len(columns_1)+len(columns_2)))
        for index, row in enumerate(final_full):
            if row[key_index1] == row[key_index2 + key_index1 + 1]:
                a1_index.append(index % len(a1))
                row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
        lines = np.delete(lines, 0, axis=0)
        left_index=set(range(len(a1)))
        l_index=left_index-set(a1_index)
        if len(l_index)>=1:
            for index in l_index:
                row=np.array(a1[index].tolist()+['NULL']*len(columns_2)).reshape(1,len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
    elif join_table[0] == 'right':
        a2_index=[]
        columns_1 = [table_1 + '.' + i for i in columns_1]
        columns_2 = [table_2 + '.' + i for i in columns_2]
        columns = columns_1 + columns_2
        lines = np.zeros((1, len(columns_1)+len(columns_2)))
        for index,row in enumerate(final_full):
            if row[key_index1] == row[key_index2 + key_index1 + 1]:
                a2_index.append(int(index/len(a1)))
                row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
        lines = np.delete(lines, 0, axis=0)
        right_index=set(range(len(a2)))
        l_index=right_index-set(a2_index)
        if len(l_index)>=1:
            for index in l_index:
                row=np.array(['NULL']*len(columns_1) + a2[index].tolist()).reshape(1,len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]

    elif join_table[0] == 'full':
        a1_index=[]
        a2_index = []
        columns_1 = [table_1 + '.' + i for i in columns_1]
        columns_2 = [table_2 + '.' + i for i in columns_2]
        columns = columns_1 + columns_2
        lines = np.zeros((1, len(columns_1)+len(columns_2)))
        for index, row in enumerate(final_full):
            if row[key_index1] == row[key_index2 + key_index1 + 1]:
                a1_index.append(index % len(a1))
                a2_index.append(int(index / len(a1)))
                row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
        lines = np.delete(lines, 0, axis=0)
        right_index = set(range(len(a2)))
        l_index = right_index - set(a2_index)
        if len(l_index) >= 1:
            for index in l_index:
                row = np.array(['NULL'] * len(columns_1)+a2[index].tolist()).reshape(1, len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]
        left_index=set(range(len(a1)))
        l_index=left_index-set(a1_index)
        if len(l_index)>=1:
            for index in l_index:
                row=np.array(a1[index].tolist()+['NULL']*len(columns_2)).reshape(1,len(columns_1)+len(columns_2))
                lines = np.r_[lines, row]

    return columns,lines

def excute_select(sql,current_db):
    input=sql_parser.parse_select(sql)
    print(input)
    # current_db=None
    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, current_db)

    if len(input[1])==1:
        tables=(input[1])
    else:
        tables=(input[1][0],input[1][1][1])
        joins=(input[1][1][0],input[1][1][2])

    # check tables
    with open(os.path.join(root_1, "table_name.csv"), 'r')as f:
        reader = csv.reader(f)
        rows = [row[0] for row in reader]
        f.close()
    for table in tables:
        if table not in rows:
            print('Table is not in the current Database!')
            return


    # merge
    if len(tables)>1:
        # join two tables
        columns,lines=join_table(tables,joins,root_1)

        select_columns = input[0]
        columns_index = []
        # check columns
        if select_columns==['*']:
            columns_index=[i for i in range(len(columns))]
            select_columns=columns
        else:
            for i, col in enumerate(select_columns):
                if col not in columns:
                    print('The {} column is not in the table, please enter correct names'.format(col))
                    print('The columns after join is',columns)
                    return
                columns_index.append(columns.index(col))

        # check out required indexes
        conditions=np.array(input[2])
        indexes=con_index(conditions,lines,columns,True)

        lines = lines[list(indexes)]

        # order the table
        lines=lines[:,columns_index]
        lines=order_data(lines,input[3],select_columns,True)
        final_table=np.vstack((select_columns,lines))
        print(final_table)


    # select without join
    else:
        columns = [i.split('.')[-1] for i in input[0]]
        columns_index=[]
        read_table = open(os.path.join(root_1, "{}.csv".format(tables[0])), 'r')

        # load columns
        for row in csv.reader(read_table):
            columns_1=row
            break
        # check columns
        if columns==['*']:
            columns_index=[i for i in range(len(columns_1))]
            columns=columns_1
        else:
            for i,col in enumerate(columns):
                if col not in columns_1:
                    print('The column is not in the table')
                    return
                columns_index.append(columns_1.index(col))

        # read data
        a1=load_data(read_table,len(columns_1))
        # select out condition index
        # conditions=np.array(input[2])
        # match_rows=[]
        # for state in conditions:
        #     if state!='and' and state!='or':
        #         match_rows.append(set(con_manu(a1,state,columns_1)[0]))
        #
        # if len(match_rows)==1:
        #     condition_rows=match_rows[0]
        # else:
        #     for state in conditions:
        #         if state=='and':
        #             indexes = set(match_rows[0] & match_rows[1])
        #             match_rows.pop(0)
        #             match_rows.pop(0)
        #             match_rows.insert(0,indexes)
        #         elif state=='or':
        #             t=match_rows.pop(0)
        #             match_rows.insert(len(match_rows),t)
        #     i=0
        #     while i<len(match_rows)-1:
        #         indexes=match_rows[i]|match_rows[i+1]
        #         i+=1
        indexes=con_index(np.array(input[2]),a1,columns_1,False)
        a1=a1[list(indexes)]

        #select columns
        a1=a1[:,columns_index]

        #order by
        orders=input[3]
        final_table=order_data(a1,orders,columns,False)

        # combine columns and table
        final_table=np.vstack((columns,final_table))
        print(final_table)
        return final_table


# sql='select www.apple,www.pear,www.orange from www full join ww on www.a=ww.b where www.a!=5 and ww.b>6 or ww.b<3 order by www.apple,www.pear'
# excute_select(sql,'www')










