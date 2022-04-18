import os
import csv
import pandas as pd

# DELETE FROM Customers WHERE CustomerName='Alfreds Futterkiste';'
# tokens = ['DELETE', 'FROM', 'Customers', 'WHERE', 'CustomerName='Alfreds Futterkiste'']
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

def delete_row(path,index):
    # with open(path,'r', encoding='utf-8', newline='') as f:
    #     reader = csv.reader(f, delimiter=',')
    #     for i,row in enumerate(reader):
    #         if i == 0:
    #             attributes = row
    #             break
    #     f.close()
    # # print(attributes)
    # attrib = []
    # attrib.append(attributes)
    # # print(attrib)

    # with open(path,'w', encoding='utf-8', newline='') as f:
    #     writer = csv.writer(f)
    #     writer.writerows(attrib)
    #     f.close()
    df = pd.read_csv(path)
    df =  df[df.index != index] 
    # print('what')

    # df.column_name != whole string from the cell
    # now, all the rows with the column: Name and Value: "dog" will be deleted

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
        if len(token) < 4:
        # delete all rows in table
            delete_all_rows(path)
        else:
        # delete determain row
            if token[3].upper() != "WHERE":
            # check sql valid
                print("Error!! it cannot be %s here" % token[3])
            else:
                # find index of the row
                delete_row(path,index)
    except:
        print("something went wrong, may be table name is wrong .")


path = "play.csv"
delete_row(path,1)