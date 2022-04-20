from database_manipulation import *
from table_manipulation import *
from index_manipulation import *
from sql_parser import *
from insert import *
from delete import *
from update import *


if __name__ == '__main__':

    sql = ""
    exit_command = ["exit", "Exit", "exit()", "Exit()"]
    current_database = None
    print("Hi! Please enter yor command!")
    print("If you want to leave, please enter 'exit' or 'Exit'")

    while sql not in exit_command:
        sql = input()
        sql_tokens = parse_sql(sql)
        #eg: CREATE TABLE foo (id integer primary key,title varchar(200) not null,description text);
        #eg: select K.a,K.b from (select H.b from (select G.c from (select F.d from(select E.e from A, B, C, D, E), F), G), H), I, J, K order by 1,2;

        # 下面两行之后删掉
        #first_token = sql_tokens[0]
        #print("first token:", first_token)
        first_token = sql_tokens[0]


        exit_command = ["exit", "Exit", "exit()", "Exit()"]
        if sql_tokens[0] in exit_command:
            print("Have a good day! Bye!")
            break

        elif first_token == "use":
            current_database = databse_functions(sql_tokens)
        elif sql_tokens[1] == "database":
            databse_functions(sql_tokens)
        elif sql_tokens[1]== "table":
            table_functions(sql_tokens, current_database)
        elif sql_tokens[1] == "index":
            index_functions(sql_tokens, current_database)
        elif first_token =='insert':
            subtokens = parse_three_part(sql)
            insert(subtokens, current_database)
        elif first_token =='update':
            subtokens = parse_three_part(sql)
            update(subtokens, current_database)
        elif first_token =='delete':
            subtokens = parse_three_part(sql)
            delete(subtokens, current_database)
        else:
            print("Error! Please enter a command with correct syntax!")



