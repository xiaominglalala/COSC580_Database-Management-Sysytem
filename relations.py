import os
import csv
def relation_function(type, current_database):

    if current_database == None:
        print("You must choose the database! Please enter: USE YOUR_DATABASE")
        print("Replace 'YOUR_DATABASE' with your target database.")
        return None

    root_0 = os.path.join(os.getcwd(), "Database_System")
    root_1 = os.path.join(root_0, current_database)

    r1 = {"row_number":1001, "col_number":2}
    r2 = {"row_number":1001, "col_number":2}
    r3 = {"row_number":10001, "col_number":2}
    r4 = {"row_number":10001, "col_number":2}
    r5 = {"row_number":100001, "col_number": 2}
    r6 = {"row_number":100001, "col_number": 2}


    path_1 = os.path.join(root_1, "Rel-i-i-1000.csv")
    path_2 = os.path.join(root_1, "Rel-i-1-1000.csv")
    path_3 = os.path.join(root_1, "Rel-i-i-10000.csv")
    path_4 = os.path.join(root_1, "Rel-i-1-10000.csv")
    path_5 = os.path.join(root_1, "Rel-i-i-100000.csv")
    path_6 = os.path.join(root_1, "Rel-i-1-100000.csv")


    if type == "rel-i-i-1000":
        if not os.path.exists(path_1):
            with open(path_1, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r1["row_number"]):
                    writer.writerow([row, row])
            f.close()
            #print("Success!")

        else:
            print("Already created.")

    elif type == "rel-i-1-1000":
        if not os.path.exists(path_2):
            with open(path_2, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r2["row_number"]):
                    writer.writerow([row, 1])
            f.close()
            # print("Success!")
        else:
            print("Already created.")

    elif type ==  "rel-i-i-10000":
        if not os.path.exists(path_3):
            with open(path_3, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r3["row_number"]):
                    writer.writerow([row, row])
            f.close()
            #print("Success!")
        else:
            print("Already created.")

    elif type == "rel-i-1-10000":
        if not os.path.exists(path_4):
            with open(path_4, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r4["row_number"]):
                    writer.writerow([row, 1])
            f.close()
            #print("Success!")
        else:
            print("Already created.")

    elif type == "rel-i-i-100000":
        if not os.path.exists(path_5):
            with open(path_5, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r5["row_number"]):
                    writer.writerow([row, row])
            f.close()
            #print("Success!")
        else:
            print("Already created.")

    elif type == "rel-i-1-100000":
        if not os.path.exists(path_6):
            with open(path_6, 'a', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["col_1", "col_2"])
                for row in range(1, r6["row_number"]):
                    writer.writerow([row, 1])
            f.close()
            #print("Success!")
        else:
            print("Already created.")















