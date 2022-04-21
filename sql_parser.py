import sqlparse
import re

def parse_sql(input):
    input = input.replace(';', '')
    while input.find("'") != -1:
        input = input.replace("'", "")
    while input.find('\t') != -1:
        input = input.replace("\t", " ")
    while input.find('\n') != -1:
        input = input.replace("\n", " ")


    sql_tokens = input.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    # 之后删掉
    # for i in sql_tokens:
    #     print("token list:", i)
    return sql_tokens


def create_table_parse(input):
    def get_attribute_list(shit_attributes):
        attributes = shit_attributes.split(",")
        i = 0
        attribute_list = []
        length = len(attributes)
        while i < length - 1:
            if "(" in attributes[i] and ")" not in attributes[i] and ")" in attributes[i + 1] and "(" not in attributes[
                i + 1]:
                attributes[i] += "," + attributes[i + 1]
                attributes.remove(attributes[i + 1])
            i += 1
            length = len(attributes)
        # 去除左空格
        for attribute in attributes:
            attribute = attribute.lstrip()
            attribute_list.append(attribute)
        return attribute_list

    #input = "CREATE TABLE EMPLOYEE (emp# SMALLINT NOT NULL, name CHAR(20) NOT NULL, salary DECIMAL(5,2) NULL,primary key (emp#));"
    rule = "\((.*)\)"
    tokens_update = ' '.join(input)
    attribute_bad_list = re.compile(rule).findall(tokens_update)
    if not attribute_bad_list:
        return None
    else:
        attribute_list = get_attribute_list(attribute_bad_list[0])
        return attribute_list

def create_index_parse(input):
    rule = "\((.*)\)"
    tokens_update = ' '.join(input)
    attribute_bad_list = re.compile(rule).findall(tokens_update)
    if not attribute_bad_list:
        return None
    else:
        index_name = attribute_bad_list[0]
        return index_name

def parse_three_part(sql):
    # keywords=['insert','into','calues','updata','set','delete','frpm','where']
    sql=sql.lower()
    # select_lists = sqlparse.format(sql, reindent=True, indent_tabs=True, comma_first=True).split('\n')
    # select_lists = [i.strip() for i in select_lists]
    # print(select_lists)
    
    if 'insert' in sql:
        tokens = parse_insert(sql)
    elif 'update' in sql:
        tokens = parse_update(sql)
    elif 'delete' in sql:
        tokens = parse_delete(sql)
    else:
        print("Not Me")
        return None
    # print(tokens)
    return tokens
        

# INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...);
# tokens = ['INSERT', 'INTO', 'table_name', [column1, column2, column3], 'VALUES', [value1, value2, value3]
def parse_insert(sql):
    tokens = []
    parts = sql.split('(')
    for part in parts:
        tokens+=part.split(')')
    if ';' in tokens:
        del tokens[-1]

    res = []
    tokens = [x.strip() for x in tokens if x.strip()!='']
    # print(tokens)
    if len(tokens) == 4:
    # insert part row
        res+=tokens[0].split()
        res.append(tokens[1].split(","))
        res.append(tokens[2])
        res.append(tokens[3].split(","))
        # print("++++++++++")
        # print(res)
    elif len(tokens)==2:
        res+=tokens[0].split(" ")
        # res.append(tokens[1].split(","))
        # res.append(tokens[1])
        res.append(tokens[1].split(","))
    # print(sql)
    print('Insert Done')
    return res


# DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';'
# tokens = ['DELETE', 'FROM', 'Customers', 'WHERE', [('CustomerName','=',"'Alfreds Futterkiste'""]]

def parse_delete(sql):
    tokens = []
    if ';' in sql:
        sql = sql[:-1]
    if 'where' in sql:
        parts = sql.split('where')
        if len(parts)!=2:
            print("ERROR")
            return NULL
        else:
            tokens+=parts[0].split()
            tokens.append("where")

            conditions = []
            # if where_part:
            # where_part=' '.join([i for i in parts[1] if i.split()[0]=='where'or i.split()[0]=='and' or i.split()[0]=='or'])[6:].split()
            and_part = parts[1].strip().split('and')
            where = []
            for item in and_part:
                where.append(item)
                where.append('and')
            where = where[:-1]
            condition=[]
            for item in where:
                or_part=item.split('or')
                temp = []
                for j in or_part:
                    temp.append(j)
                    temp.append('or')
                temp = temp[:-1]
                condition+=temp
            conditions = []
            # print(condition)
            for i in condition:
                if i == 'and' or i == 'or':
                    conditions.append(i)
                else:
                    if "=" in i:
                        if "!=" in i:
                            index = i.find('!=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '!=', v))
                        elif ">=" in i:
                            index = i.find('>=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '>=', v))
                        elif "<=" in i:
                            index = i.find('<=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '<=', v))
                        else:
                            index = i.find('=')
                            c, v = i[:index].strip(), i[index + 1:].strip()
                            conditions.append((c, '=', v))

                    elif "<" in i:
                        index = i.find('<')
                        c, v = i[:index].strip(), i[index + 1:].strip()
                        conditions.append((c, '<', v))
                    elif ">" in i:
                        index = i.find('>')
                        c, v = i[:index].strip(), i[index + 1:].strip()
                        conditions.append((c, '>', v))
                    else:
                        print('Wrong syntax for conditions')
                        return
            # print(conditions)
            tokens.append(conditions)
    else:
        tokens = sql.split()
    # print(tokens)
    print('Delete Done')
    return tokens


# UPDATE table_name SET column1 = value1, column2 = value2, ... WHERE condition;
# tokens = ['UPDATE', 'table_name', 'SET', [column1 = value1, column2 = value2], 'WHERE', [('column', '=', 'value')]]

def parse_update(sql):
    tokens = []
    if ';' in sql:
        sql = sql[:-1]
    if 'where' in sql:
        parts = sql.split('where')
        if len(parts)!=2:
            print("ERROR")
            return NULL
        else:
            first_part = parts[0].split('set')

            tokens+=first_part[0].split()
            tokens.append("set")
            tokens.append(first_part[1].split(','))
            tokens.append("where")
            # tokens.append([].append(parts[1]))
            # print(parts[1])
            # parse conditions
            conditions = []
            # if where_part:
            # where_part=' '.join([i for i in parts[1] if i.split()[0]=='where'or i.split()[0]=='and' or i.split()[0]=='or'])[6:].split()
            and_part = parts[1].strip().split('and')
            where = []
            for item in and_part:
                where.append(item)
                where.append('and')
            where = where[:-1]
            condition=[]
            for item in where:
                or_part=item.split('or')
                temp = []
                for j in or_part:
                    temp.append(j)
                    temp.append('or')
                temp = temp[:-1]
                condition+=temp
            conditions = []
            # print(condition)
            for i in condition:
                if i == 'and' or i == 'or':
                    conditions.append(i)
                else:
                    if "=" in i:
                        if "!=" in i:
                            index = i.find('!=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '!=', v))
                        elif ">=" in i:
                            index = i.find('>=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '>=', v))
                        elif "<=" in i:
                            index = i.find('<=')
                            c, v = i[:index].strip(), i[index + 2:].strip()
                            conditions.append((c, '<=', v))
                        else:
                            index = i.find('=')
                            c, v = i[:index].strip(), i[index + 1:].strip()
                            conditions.append((c, '=', v))

                    elif "<" in i:
                        index = i.find('<')
                        c, v = i[:index].strip(), i[index + 1:].strip()
                        conditions.append((c, '<', v))
                    elif ">" in i:
                        index = i.find('>')
                        c, v = i[:index].strip(), i[index + 1:].strip()
                        conditions.append((c, '>', v))
                    else:
                        print('Wrong syntax for conditions')
                        return
            # print(conditions)
            tokens.append(conditions)
    else:
        parts = sql.split('set')
        tokens+=parts[0].split()
        tokens.append("set")
        tokens.append(parts[1].split(','))

    print('Update Done')
    return tokens


# parse_three_part("INSERT INTO play (index,name) VALUES (4,rob);")
# parse_three_part("UPDATE play SET name=gggg WHERE id=c;")
# parse_three_part("DELETE FROM play WHERE id=c;")

def parse_select(sql):
    keywords=['select','from','inner join','outer join','left join','right join','where','order by','group by']
    sql=sql.lower()
    select_lists = sqlparse.format(sql, reindent=True, indent_tabs=True, comma_first=True).split('\n')

    select_lists = [i.strip() for i in select_lists]
    join_part=None
    where_part=None
    for i, p in enumerate(select_lists):
        if 'where' in p:
            where_part = select_lists[i:]
        elif 'from' in p:
            from_part = p
        elif 'order by' in p:
            order_part=select_lists[i:]
        elif 'join' in p:
            join_part=p

    # parse columns from select
    select_columns = re.findall(r'select (.+) from', sql)
    if len(select_columns)==0:
        print("The sql syntax is wrong!")
        return None
    columns=select_columns[0].split(',')

    # parse tables from select

    res=from_part.split()[1:]
    if len(res)>1:
        tables = res
        alias = res[1]
    else:
        tables=res
    if join_part:
        res=join_part.split()
        join_type = res[0]
        join_table = res[2]
        join_key = tuple(res[-1].split('='))
        tables.append((join_type,join_table,join_key))

    # parse conditions
    conditions = []
    if where_part:
        where_part=' '.join([i for i in where_part if i.split()[0]=='where'or i.split()[0]=='and' or i.split()[0]=='or'])[6:].split()
        for i in where_part:
            if i == 'and' or i == 'or':
                conditions.append(i)
            else:
                if "=" in i:
                    if "!=" in i:
                        index = i.find('!=')
                        c, v = i[:index], i[index + 2:]
                        conditions.append((c, '!=', v))
                    elif ">=" in i:
                        index = i.find('>=')
                        c, v = i[:index], i[index + 2:]
                        conditions.append((c, '>=', v))
                    elif "<=" in i:
                        index = i.find('<=')
                        c, v = i[:index], i[index + 2:]
                        conditions.append((c, '<=', v))
                    else:
                        index = i.find('=')
                        c, v = i[:index], i[index + 1:]
                        conditions.append((c, '=', v))

                elif "<" in i:
                    index = i.find('<')
                    c, v = i[:index], i[index + 1:]
                    conditions.append((c, '<', v))
                elif ">" in i:
                    index = i.find('>')
                    c, v = i[:index], i[index + 1:]
                    conditions.append((c, '>', v))
                else:
                    print('Wrong syntax for conditions')
                    return None

    # parse orderby

    select_columns = re.findall(r'order by (.+)', sql)
    order_columns=[]
    if len(select_columns)!=0:
        select_columns= select_columns[0].split(',')
        for i in select_columns:
            if len(i.split())==2:
                order_columns.append((i.split()[0],i.split()[1]))
            else:
                order_columns.append((i.split()[0],'aesc'))
            if i.split()[0] not in columns:
                if columns==['*']:
                    pass
                else:
                    print('Order by column is not in the select columns!')
                    return None
    else:
        order_columns=select_columns
    return [columns,tables,conditions,order_columns]
