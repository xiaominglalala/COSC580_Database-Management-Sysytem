import sql_parser
import os
import csv
import numpy as np
import pandas as pd
from tabulate import tabulate
import warnings
import re
warnings.filterwarnings('ignore')

def con_manu(array,condition,columns,join_flag):
    # try:
    #     value=int(condition[2])
    # except:
    #     value=condition[2]
    operand=condition[1]
    if join_flag:
        col = condition[0]
    else:
        col=condition[0].split('.')[-1]
    try:
        condition_index = columns.index(col)
    except:
        print('The condition column is not in the table!')
        return
    temp_a1 = array[:, condition_index]
    try:
        temp_a1=temp_a1.astype(int)
        value = int(condition[2])
    except:
        value = condition[2]
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

def load_data_s(open_file,col_n):
    a=[]
    for row in csv.reader(open_file):
        a.append(row)
    return np.array(a)
def con_index(conditions,array,columns,join_flag):
    if len(conditions)==0:
        return [i for i in range(len(array))]
    if len(conditions)==1:
        state=conditions[0]
        if join_flag:
            col = state[0]
        else:
            col = state[0].split('.')[-1]
        if col not in columns:
            print('The condition column is not in the table!')
            return None
    else:
        for state in conditions:
            if state!='and' and state!='or':
                if join_flag:
                    col = state[0]
                else:
                    col = state[0].split('.')[-1]
                if col not in columns :
                    print('The condition column is not in the table!')
                    return None
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
    temp_orders=[]
    cols=[]
    temp_df = pd.DataFrame(temp_a)
    temp_df.columns=select_columns
    for col in temp_df.columns:
        try:
            temp_df[col]=temp_df[col].apply(lambda x:int(x))
        except:
            pass

    for order in orders:
        if join_flag:
            col=order[0]
        else:
            col=order[0].split('.')[-1]
        cols.append(col)
        if order[1] == 'aesc':
            temp_orders.append(True)
        else:
            temp_orders.append(False)
        temp_df=temp_df.sort_values(by=cols,ascending=temp_orders)
        sort_indexes=temp_df.index
    return temp_a[sort_indexes]

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
    a1=load_data_s(r_table_1,len(columns_1))
    a2=load_data_s(r_table_2,len(columns_2))
    if len(a1)>=10000 and len(a2)>=5000:
        # lines=[]
        # for i1,l1 in enumerate(a1):
        #     for i2,l2 in enumerate(a2):
        #         if l1[key_index1]==l2[key_index2]:
        #             lines.append(l1+l2)
        # lines=np.array(lines)
        # if join_table[0] == 'left' or join_table[0] == 'outer' or join_table[0] == 'full':
        #     pass
        # columns_1 = [table_1 + '.' + i for i in columns_1]
        # columns_2 = [table_2 + '.' + i for i in columns_2]
        # columns = columns_1 + columns_2
        # return columns,lines
        a1_df=pd.DataFrame(a1)
        a2_df=pd.DataFrame(a2)
        a1_df.columns=columns_1
        a2_df.columns=columns_2
        if join_table[0] == 'inner':
            post_df=pd.merge(a1_df,a2_df,left_on=col_1,right_on=col_2,how='inner',suffixes=('.'+table_1,'.'+table_2))
        elif join_table[0] == 'left':
            post_df = pd.merge(a1_df, a2_df, left_on=col_1, right_on=col_2, how='left',
                               suffixes=('.' + table_1, '.' + table_2))
        elif join_table[0] == 'right':
            post_df = pd.merge(a1_df, a2_df, left_on=col_1, right_on=col_2, how='right',
                               suffixes=('.' + table_1, '.' + table_2))
        elif join_table[0] == 'outer' or join_table[0]=='full':
            post_df = pd.merge(a1_df, a2_df, left_on=col_1, right_on=col_2, how='outer',
                               suffixes=('.' + table_1, '.' + table_2))
        else:
            print('Join Syntax is wrong! please check')
            return None
        if len(post_df.columns)==len(columns_1)+len(columns_2):
            columns=post_df.columns
            columns=[i.split('.')[-1]+'.'+i.split('.')[0] for i in columns]
            lines=post_df.values
        else:
            columns_1 = [table_1 + '.' + i for i in columns_1]
            columns_2 = [table_2 + '.' + i for i in columns_2]
            columns = columns_1 + columns_2
            df_columns=post_df.columns
            df_columns=[i.split('.')[-1]+'.'+i.split('.')[0] if len(i.split('.'))>1 else i for i in df_columns]
            post_df.columns=df_columns
            for i,col in enumerate(df_columns):
                if col!=columns[i]:
                    df_columns[i]=extra_col=columns[i]
                    break
            post_df.columns=df_columns
            add_col=list(set(columns)-set(df_columns))[0]
            post_df[add_col]=post_df[[extra_col]]
            post_df=post_df[columns]
            lines=post_df.values
        return columns,lines
    else:
        t = np.tile(a1, (len(a2), 1))
        p = np.repeat(a2, len(a1), axis=0)
        # combine two tables
        final_full = np.column_stack((t, p))
        if join_table[0] == 'inner':
            lines = np.zeros((1,len(columns_1)+len(columns_2)))
            for row in final_full:
                if row[key_index1] == row[key_index2 + len(columns_1)]:
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
                if row[key_index1] == row[key_index2 + len(columns_1)]:
                    a1_index.append(index % len(a1))
                    row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]
            lines = np.delete(lines, 0, axis=0)
            left_index=set(range(len(a1)))
            l_index=left_index-set(a1_index)
            if len(l_index)>=1:
                for index in l_index:
                    row=np.array(a1[index].tolist()+[np.nan]*len(columns_2)).reshape(1,len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]
        elif join_table[0] == 'right':
            a2_index=[]
            columns_1 = [table_1 + '.' + i for i in columns_1]
            columns_2 = [table_2 + '.' + i for i in columns_2]
            columns = columns_1 + columns_2
            lines = np.zeros((1, len(columns_1)+len(columns_2)))
            for index,row in enumerate(final_full):
                if row[key_index1] == row[key_index2 + len(columns_1)]:
                    a2_index.append(int(index/len(a1)))
                    row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]
            lines = np.delete(lines, 0, axis=0)
            right_index=set(range(len(a2)))
            l_index=right_index-set(a2_index)
            if len(l_index)>=1:
                for index in l_index:
                    row=np.array([np.nan]*len(columns_1) + a2[index].tolist()).reshape(1,len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]

        elif join_table[0] == 'full' or join_table[0]=='outer':
            a1_index=[]
            a2_index = []
            columns_1 = [table_1 + '.' + i for i in columns_1]
            columns_2 = [table_2 + '.' + i for i in columns_2]
            columns = columns_1 + columns_2
            lines = np.zeros((1, len(columns_1)+len(columns_2)))
            for index, row in enumerate(final_full):
                if row[key_index1] == row[key_index2 + len(columns_1)]:
                    a1_index.append(index % len(a1))
                    a2_index.append(int(index / len(a1)))
                    row = np.array(row).reshape(1, len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]
            lines = np.delete(lines, 0, axis=0)
            right_index = set(range(len(a2)))
            l_index = right_index - set(a2_index)
            if len(l_index) >= 1:
                for index in l_index:
                    row = np.array([np.nan] * len(columns_1)+a2[index].tolist()).reshape(1, len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]
            left_index=set(range(len(a1)))
            l_index=left_index-set(a1_index)
            if len(l_index)>=1:
                for index in l_index:
                    row=np.array(a1[index].tolist()+[np.nan]*len(columns_2)).reshape(1,len(columns_1)+len(columns_2))
                    lines = np.r_[lines, row]

    return columns,lines

def excute_select(sql,current_db):
    try:
        input=sql_parser.parse_select(sql)
    except:
        print('The sql syntax maybe wrong!')
        return None
    # current_db=None
    try:
        root_0 = os.path.join(os.getcwd(), "Database_System")
        root_1 = os.path.join(root_0, current_db)
    except:
        print('Please use database first!')
        return None

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
            return None


    # merge
    if len(tables)>1:
        # join two tables
        columns,lines=join_table(tables,joins,root_1)
        select_columns = input[0]
        columns_index = []
        distinct_col = False
        agg_oprations = []
        # check columns
        if select_columns==['*']:
            columns_index=[i for i in range(len(columns))]
            select_columns=columns
        else:
            for i, col in enumerate(select_columns):
                if re.match(r'distinct ',col):
                    col=col.split()[-1]
                    distinct_col=True
                if re.match(r'max |min |sum |count |avg ',col):
                    agg_oprations.append(re.match(r'max |min |sum |count |avg ',col)[0].strip())
                    col=col.split()[-1]
                if col not in columns:
                    print('The {} column is not in the table, please enter correct names'.format(col))
                    print('The columns after join is',columns)
                    return None
                columns_index.append(columns.index(col))
        if len(agg_oprations) > 0:
            if len(agg_oprations) != len(columns_index):
                print('The aggregation function can only appear together!')
                print('Please check your syntax!')
        # check out required indexes
        conditions=np.array(input[2])
        if len(conditions)>0:
            indexes=con_index(conditions,lines,columns,True)
        else:
            indexes=list(i for i in range(len(lines)))
        if indexes is None:
            return None
        lines = lines[list(indexes)]

        #check agg functions
        if len(agg_oprations)>0:
            final_table=np.zeros((1,len(agg_oprations)))

            for index,agg in enumerate(agg_oprations):
                try:
                    if agg=='max':
                        temp_a=lines[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a=temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index]=np.max(temp_a)
                    elif agg=='min':
                        temp_a = lines[:, columns_index[index]]
                        temp_a=temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.min(temp_a)
                    elif agg=='count':
                        temp_a = lines[:, columns_index[index]]
                        final_table[0][index] = len(temp_a)
                    elif agg=='sum':
                        temp_a = lines[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.sum(temp_a)
                    else:
                        temp_a = lines[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.mean(temp_a)
                except:
                    print('The aggregation function only deal with int or float index!')
                    return final_table
            final_table = np.vstack((select_columns, final_table))
            print(tabulate(final_table, headers='firstrow', tablefmt='fancy_grid', missingval='N/A'))
            return final_table

        # select the table
        lines=lines[:,columns_index]
        if distinct_col:
            lines=np.unique(lines,axis=0)
            select_columns[0] = select_columns[0].split()[-1]
        # order the table
        lines=order_data(lines,input[3],select_columns,True)


        if len(lines)==0:
            print('No qualified indexes in the table!')
        else:
            final_table=np.vstack((select_columns,lines))
            print(tabulate(final_table,headers='firstrow',tablefmt='fancy_grid', missingval='N/A'))



    # select without join
    else:
        columns = [i.split('.')[-1] for i in input[0]]
        columns_index=[]
        distinct_col=False
        read_table = open(os.path.join(root_1, "{}.csv".format(tables[0])), 'r')
        agg_oprations=[]
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
                if re.match(r'distinct ',col):
                    col=col.split()[-1]
                    distinct_col=True
                if re.match(r'max |min |sum |count |avg ',col):
                    agg_oprations.append(re.match(r'max |min |sum |count |avg ',col)[0].strip())
                    col=col.split()[-1]
                if col not in columns_1:
                    print('The column is not in the table')
                    return None
                columns_index.append(columns_1.index(col))

        if len(agg_oprations)>0:
            if len(agg_oprations)!=len(columns_index):
                print('The aggregation function can only appear together!')
                print('Please check your syntax!')
                return None
        # read data
        a1 = load_data_s(read_table, len(columns_1))

        # a1=load_data(read_table,len(columns_1))
        if len(input[2])>0:
            indexes=con_index(np.array(input[2]),a1,columns_1,False)
        else:
            indexes=list(i for i in range(len(a1)))
        if indexes is None:
            return None
        a1=a1[list(indexes)]


        #check agg functions
        if len(agg_oprations)>0:
            final_table=np.zeros((1,len(agg_oprations)))

            for index,agg in enumerate(agg_oprations):
                try:
                    if agg=='max':
                        temp_a=a1[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index]=np.max(temp_a)
                    elif agg=='min':
                        temp_a = a1[:, columns_index[index]]
                        temp_a=temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.min(temp_a)
                    elif agg=='count':
                        temp_a = a1[:, columns_index[index]]
                        final_table[0][index] = len(temp_a)
                    elif agg=='sum':
                        temp_a = a1[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.sum(temp_a)
                    else:
                        temp_a = a1[:, columns_index[index]]
                        temp_a = temp_a.astype(np.float)
                        temp_a = temp_a[np.where(~np.isnan(temp_a))]
                        final_table[0][index] = np.mean(temp_a)
                except:
                    print('The aggregation function only deal with int or float index!')
                    return None
            final_table = np.vstack((columns, final_table))
            print(tabulate(final_table, headers='firstrow', tablefmt='fancy_grid', missingval='N/A'))
            return final_table
        #select columns
        a1 = a1[:, columns_index]
        if distinct_col:
            a1=np.unique(a1,axis=0)
            columns[0]=columns[0].split()[-1]
        #order by
        orders=input[3]
        final_table=order_data(a1,orders,columns,False)

        if len(final_table)==0:
            print('No qualified indexes in the table!')
        else:
        # combine columns and table
            final_table=np.vstack((columns,final_table))
            print(tabulate(final_table,headers='firstrow',tablefmt='fancy_grid',missingval='N/A'))

    return final_table


# sql='select * from www right join ww on www.a=ww.b where www.a!=5 and ww.b>6 or ww.b<3 order by www.apple,www.pear'
# sql='SELECT Apple From www '
# excute_select(sql,'www'）










