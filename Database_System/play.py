import csv 
print("++++++++++++++")
with open( "play.csv",'w', encoding='utf-8', newline='') as f:
    print("**************")
    writer = csv.writer(f)
    writer.writerows([['index','name','id','sha'],['1','Spam','a','that'],['2','jason','b','hii']])
    # reader = csv.reader(f, delimiter=',')
    # # for row in reader:
    # #     print("______________")
    # #     print(row)
    # first_row = reader
    # print(first_row)

    # print(f.readrow()[0])    
    f.close()

# # ['2','john','c']

# import pandas as pd
# df = pd.read_csv("play.csv")
# print(df)
# index = df.index
# condition = df["name"] == "ame"
# apples_indices = index[condition]
# # get only rows with "apple"

# apples_indices_list = apples_indices.tolist()

# print(apples_indices_list)