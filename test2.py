from sql_parser import *

import re

def get_attribute_list(attrsCons):
    attributes = attrsCons.split(",")
    i=0
    attribute_list = []
    length = len(attributes)
    while i < length-1:
        if "(" in attributes[i] and ")" not in attributes[i] and ")" in attributes[i+1] and "(" not in attributes[i+1]:
            attributes[i] += "," + attributes[i+1]
            attributes.remove(attributes[i+1])
        i += 1
        length = len(attributes)
    # 去除左空格
    for attribute in attributes:
        attribute = attribute.lstrip()
        attribute_list.append(attribute)
    return attribute_list

input = "CREATE TABLE EMPLOYEE (emp# SMALLINT NOT NULL, name CHAR(20) NOT NULL, salary DECIMAL(5,2) NULL,primary key (emp#));"

tokens = parse_sql_normal(input)
rule = "\((.*)\)"
tokens_update = ' '.join(tokens)
attribute_bad_list = re.compile(rule).findall(tokens_update)
attribute_list = get_attribute_list(attribute_bad_list[0])
print(attribute_list)

primary_key = []
attribute_names = []
for attribute in attribute_list:
    attribute = attribute.lstrip()
    # get the primary key
    reg = "primary\s*key.*\((.*)\)+"
    primary = re.compile(reg).findall(attribute)
    if len(primary) > 0:
        primary = primary[0]
        primary_key = primary.split(', ')

    # get attributes
    else:
        attribute = attribute.split(' ')
        attribute_names.append(attribute[0])
print(attribute_names)
print(primary_key)

