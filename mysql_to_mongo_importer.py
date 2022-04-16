import mysql.connector
import pymongo
from pymongo import MongoClient
import datetime

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

begin_time = datetime.datetime.now()
print(f"{bcolors.HEADER}Script started at: {begin_time} {bcolors.ENDC}")

delete_existing_documents = True
mysql_host="localhost"
mysql_database="dawahbox_nov20"
# mysql_schema = "myschhema"
mysql_user="root"
mysql_password=""


# Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = "lol"
myclient = MongoClient(CONNECTION_STRING)

# mongodb_host = "mongodb://localhost:27017/"
mongodb_dbname = "dboxcluster"

print(f"{bcolors.HEADER}Initializing database connections...{bcolors.ENDC}")
print("")

#MySQL connection
print(f"{bcolors.HEADER}Connecting to MySQL server...{bcolors.ENDC}")
mysqldb = mysql.connector.connect(
    host=mysql_host,
    database=mysql_database,
    user=mysql_user,
    password=mysql_password
)
print(f"{bcolors.HEADER}Connection to MySQL Server succeeded.{bcolors.ENDC}")

#MongoDB connection
print(f"{bcolors.HEADER}Connecting to MongoDB server...{bcolors.ENDC}")
mydb = myclient[mongodb_dbname]
print(f"{bcolors.HEADER}Connection to MongoDB Server succeeded.{bcolors.ENDC}")

print(f"{bcolors.HEADER}Database connections initialized successfully.{bcolors.ENDC}")

#Start migration
print(f"{bcolors.HEADER}Migration started...{bcolors.ENDC}")
dblist = myclient.list_database_names()

if mongodb_dbname in dblist:
    print(f"{bcolors.OKBLUE}The database exists.{bcolors.ENDC}")
else:
    print(f"{bcolors.WARNING}The database does not exist, it is being created.{bcolors.ENDC}")

# Function migrate_table 
def get_sql_tables(db):
    mycursor1 = db.cursor()
    mycursor1.execute("Show tables;")
    fetched_tables = mycursor1.fetchall()
    return fetched_tables
# get_sql_tables(mysqldb)

all_tables = get_sql_tables(mysqldb)
def fetch_table_contents(db, table):
    mycursor2 = db.cursor(dictionary=True)
    for x in table:
        data_tables = x[0]
        mycursor2.execute("SELECT * from {0};".format(data_tables))
        myresult = mycursor2.fetchall()

        mycol = mydb[data_tables]
        if delete_existing_documents:
            #delete all documents in the collection
            mycol.delete_many({})
        
        #insert the documents
        if len(myresult) > 0:
            x = mycol.insert_many(myresult)
            return [myresult, len(x.inserted_ids)]
        else:
            return [myresult, 0]

        # total_count = len(tables)
        success_count = 0
        fail_count = 0
        
fetch_table_contents(mysqldb, all_tables)
