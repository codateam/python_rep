import mysql.connector
import pymongo
from pymongo import MongoClient
import credentials
from credentials import CONNECTION_STRING as C_String



class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = C_String
# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)

delete_existing_documents = True
mysqldb = mysql.connector.connect(host="localhost",   database="dawahbox_dawahcast_users",    user="dawahbox_admin",    password="")
mycursor1 = mysqldb.cursor()
mycursor2 = mysqldb.cursor(dictionary=True)

mycursor1.execute("Show tables;")
all_tables = mycursor1.fetchall()
print(f"The following data tables will be fetched from the database and prototype will be created in mongodb \n {all_tables}")
print("")

mydb = client['dboxcluster']
# mydb = client['dawahbox_oct2019']
# mydb = client['dawahbox_nov20']

for x in all_tables:
    # print(x[0])
    mycursor2.execute("SELECT * from {0};".format(x[0]))
    myresult = mycursor2.fetchall() 
    # print(myresult, '\n')

    collection_names = mydb.list_collection_names()

    data_tables = x[0]
    # print(data_tables)
    mycol = mydb[data_tables]
   
    if len(myresult) > 0:
        skip = False
        if data_tables in collection_names:
            me = 'dummy'
            print(f"{bcolors.OKBLUE} The collection {data_tables} already exists skipping to next one.{bcolors.ENDC}")
           
        else:
            # print(data_tables)
            print(f"{bcolors.WARNING} The collection {data_tables} does not exist, it is being created.{bcolors.ENDC}")
            

            try:
                x2 = mycol.insert_many(myresult) #myresult comes from mysql cursor
                print(f"{len(x2.inserted_ids)} documents created in mongo for {data_tables} ")
                print(data_tables, 'Data transfer complete....')
                print("")
            except Exception as e:
                print(data_tables, 'skipped due to error !!!')
                print(e)
                print("")
                skip = True
                continue
      