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
    attribute_list = get_attribute_list(attribute_bad_list[0])
    return attribute_list

def create_index_parse(input):
    rule = "\((.*)\)"
    tokens_update = ' '.join(input)
    attribute_bad_list = re.compile(rule).findall(tokens_update)
    index_name = attribute_bad_list[0]
    return index_name
