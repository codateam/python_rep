from ast import Return
from logging import exception
from pickle import EMPTY_DICT
from numpy import empty
import pandas as pd
from pymongo import MongoClient,UpdateOne
import json
import re
import credentials as cstring
import hashlib
from datetime import datetime

# sudo mv /home/umarv24/Downloads/export_convert.php /var/www/html/export_convert.php
# sudo gedit /var/www/html/export2.csvjson
# chmod 777 /var/www/html/export_convert.php

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

startTime = datetime.now()

def Optpicker(file_name,conv_type):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    # conv_type =int(input("pick the file type to upload\n 1.) CSV->JSON->Upload \n 2.) JSON->File Upload \n "))

    #-----------------------------------------#
    #-------------------pick csv files----------------------#
    if conv_type == 1:
        try:
            data = pd.read_csv(file_name)
            payload = json.loads(data.to_json(orient='records'))
        except Exception as e:
            print('error:', e)
        
        with open("{0}.json", "w").format(file_name) as outfile:
            outfile.write(json.dumps(payload, indent=4))
    
    #-----------------------------------------#
    #------------------pick json files-----------------------#
    elif conv_type == 2:
        try:
            f = open(file_name)
            payload = json.load(f)
        except Exception as e:
            print('error:', e)
    
    else:
        print("pass...........")
    
    return payload

#-----------------------------------------#
    #------------------pick all categ data once-----------------------#
def pick_allColl_data(collname):
    allColldata = []
    categ_coll = db[collname]
    cursor = categ_coll.find({})
    for document in cursor:
        del document['_id']
        del document['img']
        allColldata.append(document)    # print(allColldata)
    return allColldata


#-----------------------------------------#
    #------------------pick all keyword data once-----------------------#
def pick_keyColl_data(collname):
    allKeydata = []
    Keyword_coll = db[collname]
    cursor = Keyword_coll.find({})
    for document in cursor:
        del document['_id']
        allKeydata.append(document)    # print(allKeydata)
    return allKeydata

#-----------------------------------------#
    #------------------pick all records of source ids immediately-----------------------#
def load_all_ids(dbcoll,dataSource, maxitem):

    print("Start all data loading for performace......")
    f = open(dataSource)
    payload = json.load(f)

    mp_id_arr = []
    for i in range(0,maxitem):
    # for datum in dataSource:
        mp_id_arr.append(payload[i]['id'])
    
    cursor1 = db[dbcoll].find({'id':{'$in' :mp_id_arr}})

    print("fetched all related ids records....")
    all_ids_data = {}
    for data in cursor1:
        # print(data['id'])
        all_ids_data[data['id']] = data
        # print(f"{all_ids_data}\n")
    
    print("Done fetching all related records........")
    # print(all_ids_data)
    return all_ids_data


#-----------------------------------------#
    #------------------pick all keyword data once-----------------------#
def multi_data_splitter(dataSource, dataIndex, keycheck, dbDatas, total_data_ids):
    data_id = ""
    #----------splitting the category string-----------#
    splitted = dataSource[dataIndex][keycheck].split(",")
    for x in range(0, len(splitted)):
        
        #-------clean the whitespace frm splited names ----------#
        data_name_split = splitted[x].strip()
        #--------locate each category id & data from collection-------#
        findData = dbDatas
        #---------loop through the object cox it's mongo object-------#
        for value in findData:
            if value['name'] == data_name_split:
                data_id_val = value['id']
                data_id += str(data_id_val)+ ','
                total_data_ids.append(data_id_val)
            else:
                pass
        
    data_id = data_id[0:len(data_id)-1]  
    return total_data_ids


AllCateg = pick_allColl_data("tbl_category_try")
AllKeys = pick_keyColl_data("all_keywords")
# all_dbId_data = load_all_ids('tbl_mp3',"export_apr_30.json",10)

# payload = Optpicker('export2.json')
# print(AllCateg)

def sendToFile(filename, maxlength, all_dbId_data):
    payload = filename
    # print(payload)
    # with open("sample_export.json", "w") as outfile:
    overallArr = []
    for i in range(0,maxlength):
        # print(i)
        #---------find equiv art id using artist name------#
        total_categ_ids = []
        if payload[i]['mp3_artist'] == "":
            categ_id = 0
            
        else:
            try:
                key_content = 'mp3_artist'
                total_categ_ids = multi_data_splitter(payload, i, key_content, AllCateg, total_categ_ids)
            except Exception as e:
                print(e)
            
        
        #---------find equiv cat id using categ name------#
        total_keyword_ids  = []
        if payload[i]['Keywords'] == "":
            key_id = 0
        
        else:
            try:
                key_content = 'mp3_artist'
                total_keyword_ids = multi_data_splitter(payload, i, key_content, AllKeys, total_keyword_ids)
            except Exception as e:
                print(e)
        
                
        
        #-----datas coming from the json file-----------#
        ids = int(payload[i]['id'])
        mp3_title = payload[i]['mp3_title']
        mp3_url = payload[i]['mp3_url']
        mp3_thumbnail = payload[i]['mp3_thumbnail']
        categ_name = payload[i]['mp3_artist']
        mp3_duration = payload[i]['mp3_duration']
        mp3_description = payload[i]['mp3_description']
        mp3_share_url = payload[i]['mp3_share_url']
        status = int(payload[i]['status'])
        full_img = payload[i]['full_img']
        

    #----------catch exception for empty album id---------#
        if payload[i]['album_id'] == "" or payload[i]['album_id'] is None:
            album_id = 0
        else:
            album_id = int(payload[i]['album_id'])
    
    #----------catch exception for empty lang id---------#
        if payload[i]['lang_id'] == "" or payload[i]['lang_id'] is None:
            lang_id = 0
        else:
            lang_id = int(payload[i]['lang_id'])
    
    #----------catch exception for empty rp id---------#
        if payload[i]['cat_id'] == "" or payload[i]['cat_id'] is None:
            rp_id = 0
        else:
            rp_id = int(payload[i]['cat_id'])
        
    #----------catch exception for empty keyword---------#
        if payload[i]['Keywords'] == '' or payload[i]['Keywords'] is None:
            keyword = ""
        else:
            keyword = payload[i]['Keywords']

        # if payload[i]['key_id'] == '':
        #     key_id = 0
        # else:
        #     key_id = int(payload[i]['key_id'])

    
        #----------regular expre to clean description nd get size---------#
        try:
            extr_size = re.search('Size:(.+?)P3]', mp3_description).group()
            clean_size1 = extr_size.split(":")
            clean_size2 = clean_size1[1][1:9]
            mp3_size = str(clean_size2)
        except Exception as e:
            mp3_size = ""

        #---------dic data for all records to be posted on mongo-------#
        
        raw_data = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12}".format(album_id,rp_id,lang_id,total_keyword_ids,total_categ_ids,mp3_title,mp3_url,mp3_thumbnail,mp3_duration,mp3_description,mp3_share_url,full_img,mp3_size)
        hash_obj = hashlib.md5(raw_data.encode())
        
        emp_rec1 = {
            "id":ids,
            "hashed":hash_obj.hexdigest(),
            "status":status,
            "album_id":album_id,
            "rp_id": rp_id,
            "lang_id":lang_id,
            "key_id":total_keyword_ids,
            "cat_id": total_categ_ids,
            "mp3_title": mp3_title,
            "mp3_url": mp3_url,
            "mp3_thumbnail":mp3_thumbnail,
            "cat_name":categ_name,
            "key_name":keyword,
            "mp3_duration":mp3_duration,
            "mp3_description":mp3_description,
            "mp3_share_url":mp3_share_url,
            "img":full_img,
            "mp3_size": mp3_size,
        }

        unique = { 
            "id":ids,
            "hashed":hash_obj.hexdigest(),
        }

        
        #------for crosschecking before updating
        # print()
        # (mp_id,hash_obj,fulldata,filename,maxlimit)
        hashed_data = data_hasher(ids,hash_obj.hexdigest(),emp_rec1,all_dbId_data)

        # print(hashed_data)
        if hashed_data:
            overallArr.append(hashed_data)
        
        # overallArr.append(emp_rec1)
        # overallArr.append(UpdateOne(unique,{"$set":emp_rec1},upsert=True))
        
#-----------print inserting doc----------#
        # print(f"inserting {ids} records with desc {mp3_description}Title: {mp3_title}\nMp3 Url: {mp3_url}")
        # print(f"inserted record:---------------{i} out of {no_of_rec}-----------------------------------------------------------------------------------\n")
    
    try:
        
        print("{0} trying to update to db now".format(bcolors.OKBLUE))
        # db['tbl_mp3'].bulk_write(overallArr,ordered=False)
        # bulk.execute()
        # print(overallArr)
        if overallArr:
            db['tbl_mp3'].insert_many(overallArr)
            print("{0} Collection updating complete...".format(bcolors.OKGREEN))
        else:
            print("no update perform !!")

        print(datetime.now() - startTime)
    except exception as e:
        print("{0} error: ".format(bcolors.FAIL), e)
        print(datetime.now() - startTime)


#--------------------------------------------#
# def readFrmFile(filename, collname):
#     print("{0} now reading from json file to populate the database......".format(bcolors.HEADER))
    
#     # Opening JSON file
#     f = open(filename)
    
#     # returns JSON object as a dictionary
#     data = json.load(f)
#     # print(data)
    # collname.insert_many(data)
    
#     # Closing file
#     f.close()
#     print("{0} population complete! file closed".format(bcolors.OKGREEN))

#--------------------Dispatcher------------------------#
def take_choice(filename,collname,maxlimit,all_dbId_data,choice):

    # choice = int(input("pick the operation to perform\n 1.) Read->Process->Import \n 2.) Read->Process \n "))
    # filename = input("Input the filename e.g mydata.json: ")
    payload = Optpicker(filename, 2)

    if choice == 1:
        sendToFile(payload,maxlimit,all_dbId_data)
        # readFrmFile("sample_export.json", db[collname])
    elif choice == 2:
        sendToFile(payload,maxlimit,all_dbId_data)



#-----------------------------------------#
    #------------------pick data & hash-----------------------#
def data_hasher(mp_id,hash_obj,fulldata,all_dbId_data):
    print("{0} Hashing....{1}--to--{2}".format(bcolors.OKGREEN,mp_id,hash_obj))
        
    processed_Data = {} #empty dictionary to represent data
    
    try: # if key mp_id exist from the fetched data
        if all_dbId_data[mp_id]:
            
            print("{0}---###---{1}".format(all_dbId_data[mp_id]['hashed'],hash_obj))
            if hash_obj == all_dbId_data[mp_id]['hashed']: # print both hashes and compare them
                print("{0} Same id and Hash detected for {1} skipping document.......".format(bcolors.FAIL,all_dbId_data[mp_id]['hashed']))

                processed_Data = processed_Data #returned epty processed data cox of duplicate data detected

            
            else: #if not same update with new hash values
                print("{0} Same id but different Hash detected, updating current data......".format(bcolors.WARNING,all_dbId_data[mp_id]['hashed']))
                if all_dbId_data[mp_id]['views']:
                    fulldata['views'] = all_dbId_data[mp_id]['views']
                if all_dbId_data[mp_id]['downloads']:
                    fulldata['downloads'] = all_dbId_data[mp_id]['downloads']

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
    
    # filename = input("Input the filename e.g mydata.json: ")
    # collname = input("Input the collection name to import all lectures to: ")
    # maxlimit = int(input("Input the maximum amount of data to work on: "))
    
    filename = "export_apr_30.json"
    collname = "tbl_mp3"
    maxlimit = 10000
    all_dbId_data = []
    # all_dbId_data = load_all_ids('tbl_mp3',Optpicker("export_apr_30.json"),maxlimit)
    # load_all_ids('tbl_mp3',Optpicker(filename),maxlimit)
    all_dbId_data = load_all_ids(collname,filename,maxlimit)
    take_choice(filename, collname, maxlimit, all_dbId_data,1)
# 20244,20245,20246,20247,20248,20249,20250,20251

# 0:00:29.427551