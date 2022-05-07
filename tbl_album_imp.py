from ast import Return
from logging import exception
from pickle import EMPTY_DICT
from numpy import empty
import pandas as pd
from pymongo import MongoClient,InsertOne,UpdateOne
import json
import re
import credentials as cstring
import hashlib
from datetime import datetime
import sys


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
CONNECTION_STRING = cstring.CONNECTION_STRING
# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)
#-----collections access----------------#
db = client["dboxcluster"]
rp_coll = db['tbl_rp']

mp3Coll = db['tbl_mp3']
langColl = db['tbl_lang']

startTime = datetime.now()

print("------------------------ALBUM IMPORTER----------------------")

def Optpicker(file_name,conv_type):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """

    #-----------------------------------------#
    #-------------------pick csv files----------------------#
    if conv_type == 1:
        try:
            data = pd.read_csv(file_name)
            payload = json.loads(data.to_json(orient='records'))
            data_len = len(payload)
        except Exception as e:
            print('error:', e)
        
        convert_name = file_name[0:len(filename) - 4]+ ".json"
        with open("{0}".format(convert_name), "w") as outfile:
            outfile.write(json.dumps(payload, indent=4))
        
        print("conversion of csv to json complete...")

        try:
            f = open(convert_name)
            payload = json.load(f)
            data_len = len(payload)
        except Exception as e:
            print('error:', e)
    
    #-----------------------------------------#
    #------------------pick json files-----------------------#
    elif conv_type == 2:
        try:
            f = open(file_name)
            payload = json.load(f)
            data_len = len(payload)
            convert_name = file_name
        except Exception as e:
            print('error:', e)
    
    else:
        print("pass...........")
    
    return [payload,data_len,convert_name]


#-----------------------------------------#
    #------------------pick all album lec no-----------------------#
def pick_albLec_no(collname,albId):
    allColldata = []
    alb_coll = db[collname]
    num_of_lec = alb_coll.count_documents({ 'album_id': int(albId)})
    return num_of_lec

#-----------------------------------------#
    #------------------pick all records of source ids immediately-----------------------#
def load_all_ids(dbcoll,dataSource, maxitem):

    print("""Start all data loading to improve performance, if data size > 5000
        only 5000 records will be loaded & checked for due to performance""")

    f = open(dataSource)
    payload = json.load(f)

    mp_id_arr = []
    for i in range(0,maxitem):
        mp_id_arr.append(payload[i]['aid'])
    
    cursor1 = db[dbcoll].find({'id':{'$in' :mp_id_arr}})

    print("fetching all related ids records....")
    all_ids_data = {}
    for data in cursor1:
        all_ids_data[data['id']] = data
    
    print("Done fetching all related records........")
    return all_ids_data


def sendToFile(filename, maxlength, all_dbId_data,collname):
    payload = filename
    overallArr = []
    for i in range(0,maxlength):
        #-----datas coming from the json file-----------#
        ids = int(payload[i]['aid'])
        album_name = payload[i]['album_name']
        img = payload[i]['full_img']
        rp_id = payload[i]['cat_id']
        lang_id = payload[i]['lang_id']
        arts = payload[i]['artist']
        Keywords = payload[i]['Album Keywords']
        key_id = payload[i]['key_id']
        
        num_of_lec = pick_albLec_no('tbl_mp3',ids)
        # print("{0} number of lecture found in album {1}".format(num_of_lec, ids))

    #------------search and produce lang using langid-------------------//
        if lang_id == "" or lang_id is None:
            language = ""
        else:
            langData = langColl.find({'id': int(lang_id)})
            for document in langData:
                del document['_id']
                language = document['name']
    

        #---------dic data for all records to be posted on mongo-------#
        
        raw_data = "{0},{1},{2},{3},{4},{5}".format(album_name,img,rp_id,lang_id,arts,key_id)
        hash_obj = hashlib.md5(raw_data.encode())
        

        emp_rec1 = {
            "id":ids,
            "hashed":hash_obj.hexdigest(),
            "name":album_name,
            "img":img,
            "lec_no": num_of_lec,
            "rp_id":rp_id,
            "lang_id":lang_id,
            "categories": arts,
            "Keywords": Keywords,
            "key_id": key_id,
            "language":language,
        }

        unique = { 
            "id":ids,
            "hashed":hash_obj.hexdigest(),
        }

        #-------------------#
        print("Document-----{0}".format(i))
        #------for crosschecking before updating
        hashed_data = data_hasher(ids,hash_obj.hexdigest(),emp_rec1,all_dbId_data)

        prepared = UpdateOne(unique, {'$set': hashed_data}, upsert=True )
      
        if hashed_data: #make sure that hashed_data is not empty dictionary
            overallArr.append(prepared)

    try:
        print("Total logic carried out finished in: {0} time".format(datetime.now() - startTime))
        print("{0} trying to update to db now".format(bcolors.OKBLUE))
        
        if overallArr:
            db[collname].bulk_write(overallArr)
            # db[collname].insert_many(overallArr)
            print("{0} Collection updating complete...".format(bcolors.OKGREEN))
        else:
            print("no update perform !!")

        print("Collection updated with {0} documents at {1}".format(maxlength,datetime.now() - startTime))
    except exception as e:
        print("{0} error: ".format(bcolors.FAIL), e)
        print(datetime.now() - startTime)


#--------------------Dispatcher------------------------#
def take_choice(filename,collname,all_dbId_data,choice, pick_option):

    payload = Optpicker(filename, pick_option)

    if choice == 1:
        sendToFile(payload[0],payload[1],all_dbId_data,collname)

    elif choice == 2:
        sendToFile(payload[0],payload[1],all_dbId_data,collname)



#-----------------------------------------#
    #------------------pick data & hash-----------------------#
def data_hasher(alb_id,hash_obj,fulldata,all_dbId_data):
    print("{0} Hashing....{1}--to--{2}".format(bcolors.OKGREEN,alb_id,hash_obj))
        
    processed_Data = {} #empty dictionary to represent data
    
    try: # if key mp_id exist from the fetched data
        if all_dbId_data[alb_id]:
            
            print("{0}---###---{1}".format(all_dbId_data[alb_id]['hashed'],hash_obj))
            if hash_obj == all_dbId_data[alb_id]['hashed']: # print both hashes and compare them
                print("{0} Same id and Hash detected for {1} skipping document.......".format(bcolors.FAIL,all_dbId_data[alb_id]['hashed']))

                processed_Data = processed_Data #returned epty processed data cox of duplicate data detected

            
            else: #if not same update with new hash values
                print("{0} Same id but different Hash detected, updating current data......".format(bcolors.WARNING))
                try:
                    if all_dbId_data[alb_id]['views']:
                        fulldata['views'] = all_dbId_data[alb_id]['views']
                except KeyError as err:
                    print('error trying to check data existence for views', err)
                    fulldata['views'] = 0
                    
                try:
                    if all_dbId_data[alb_id]['downloads']:
                        fulldata['downloads'] = all_dbId_data[alb_id]['downloads']
                except KeyError as err:
                    print('error trying to check data existence for downloads', err)
                    fulldata['downloads'] = 0

                processed_Data = fulldata #returned the fulldata to update existing values with
        
    
    except KeyError as e: #if key doesn't exist create new document set
        print('error trying to check data existence', e)
        try:
            print("{0} New nid detected, creating new document.....".format(bcolors.OKBLUE))
            processed_Data = fulldata #returned the fresh fulldata to create new document
        except Exception as e:
            print("error",e)
    
    return processed_Data
 
if __name__ == '__main__':
    
    option = int(input("""Pick an Option
        1.) Perform a CSV -> JSON file conversion -> Data manipulation -> DB Collection Upload
        2.) Input a JSON file and system reads from file -> Data manipulate -> DB Collection Upload
        """))
    
    if option == 1:
        filename = input("Input the CSV filename e.g mydata.csv: ")
    elif option == 2:
        filename = input("Input the JSON filename e.g mydata.json: ")
    else:
        print("Invalid Option Quitting System")
        sys.exit()
        
    collname = input("Input the collection name to import all lectures to: ")

    payload = Optpicker(filename, option)
    maxlimit = payload[1]
    
    all_dbId_data = load_all_ids(collname,payload[2],maxlimit)
    take_choice(payload[2], collname, all_dbId_data, 1, option)


