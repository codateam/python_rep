import mysql.connector as sqlcon
from mysql.connector import Error
import pandas as pd

conn = sqlcon.connect(
    host='localhost',
    user='root',
    password='',
    db='dawahbox_nov20',
)

mycursor = conn.cursor()

if conn.is_connected():
    print("databse connected...")
    mycursor.execute("Show tables;")
    all_tables = mycursor.fetchall()
    print(all_tables)
    print("")
else:
    print("i am not connected....")


def read_database_table():

    db_list = []
    try:
        mycursor.execute("SELECT * from {0};".format('tbl_mp3'))
        myresult = mycursor.fetchall() 
        for i in range(0,2):
        # for row in myresult:
            db_list.append(myresult[i])
    except Exception as e:
        print(f"error: {e}")

    print(db_list)
    print("")
# read_database_table()



a = pd.read_csv("mp3_files.csv")
csv_id = a.id
csv_cid = a.cat_id
csv_aid =a.album_id
csv_mptype = a.mp3_type
csv_mptitle =a.mp3_title
csv_mpurl = a.mp3_url
csv_mpthum = a.mp3_thumbnail
csv_mpdur = a.mp3_duration
csv_mpart =a.mp3_artist
csv_mpdesc = a.mp3_description
csv_mpshurl = a.mp3_share_url


def read_csv_data():

    # csvdata = pd.read_csv('mp3_files.csv', index_col=False, delimiter = ',')
    # csvdata.head()

    for i in range(0,3):
        # for  x in csv_id:
        print(csv_id[i])
        sql = "SELECT `id`,`mp3_duration` FROM tbl_mp3"
        mycursor.execute(sql)
        result = mycursor.fetchall()

read_csv_data()


# empdata = pd.read_csv('C:\\Users\\XXXXX\\empdata.csv', index_col=False, delimiter = ',')