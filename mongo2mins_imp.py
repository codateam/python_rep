from ast import Return
import pandas as pd
from pymongo import MongoClient
import json
import re


# Provide the mongodb atlas url to connect python to mongodb using pymongo
CONNECTION_STRING = "lol"
# Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
client = MongoClient(CONNECTION_STRING)
#-----collections access----------------#
db = client["dboxcluster"]
rp_coll = db['tbl_rp']

def Optpicker(file_name):
    """ Imports a csv file at path csv_name to a mongo colection
    returns: count of the documants in the new collection
    """
    conv_type =int(input("pick the file type to upload\n 1.) CSV->JSON->Upload \n 2.) JSON->File Upload \n "))

    #-----------------------------------------#
    #-------------------pick csv files----------------------#
    if conv_type == 1:
        try:
            data = pd.read_csv(file_name)
            payload = json.loads(data.to_json(orient='records'))
        except Exception as e:
            print('error:', e)
    
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


AllCateg = pick_allColl_data("tbl_category_try")
payload = Optpicker('newExport.json')
# print(AllCateg)

def sendToFile(maxlength):

    with open("sample_export.json", "w") as outfile:
        overallArr = []
        for i in range(0,maxlength):

            total_categ_ids = []
            #---------find equiv art id using artist name------#
            if payload[i]['mp3_artist'] == "":
                categ_id = 0
                # print(f'empty categ id: {categ_id}')
                
            else:
                try:
                    categ_id = ""
                    #----------splitting the category string-----------#
                    splitted = payload[i]['mp3_artist'].split(",")
                    for x in range(0, len(splitted)):
                        #-------clean the whitespace frm splited names ----------#
                        categ_name_split = splitted[x].strip()
                        #--------locate each category id & data from collection-------#
                        findcateg = AllCateg
                        #---------loop through the object cox it's mongo object-------#
                        for value in findcateg:
                            if value['name'] == categ_name_split:
                                categ_id_val = value['id']
                                categ_id += str(categ_id_val)+ ','
                                total_categ_ids.append(categ_id_val)
                            else:
                                pass
                        
                    categ_id = categ_id[0:len(categ_id)-1]  
                    # print(total_categ_ids)

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
            if payload[i]['album_id'] == "":
                album_id = 0
            else:
                album_id = int(payload[i]['album_id'])
        
        #----------catch exception for empty lang id---------#
            if payload[i]['lang_id'] == "":
                lang_id = 0
            else:
                lang_id = int(payload[i]['lang_id'])
        
        #----------catch exception for empty rp id---------#
            if payload[i]['cat_id'] == "":
                rp_id = 0
            else:
                rp_id = int(payload[i]['cat_id'])
        
            #----------regular expre to clean description nd get size---------#
            try:
                extr_size = re.search('Size:(.+?)P3]', mp3_description).group()
                clean_size1 = extr_size.split(":")
                clean_size2 = clean_size1[1][1:9]
                mp3_size = str(clean_size2)
            except Exception as e:
                mp3_size = ""

            #---------dic data for all records to be posted on mongo-------#
            emp_rec1 = {
                "id":ids,
                "status":status,
                "album_id":album_id,
                "rp_id": rp_id,
                "lang_id":lang_id,
                "cat_id": total_categ_ids,
                "mp3_title": mp3_title,
                "mp3_url": mp3_url,
                "mp3_thumbnail":mp3_thumbnail,
                "cat_name":categ_name,
                "mp3_duration":mp3_duration,
                "mp3_description":mp3_description,
                "mp3_share_url":mp3_share_url,
                "img":full_img,
                "mp3_size": mp3_size,
            }

            unique = { 
                "id":ids,
                "status":status,
                "cat_id": total_categ_ids,
                "album_id":album_id,
            }
            overallArr.append(emp_rec1)

    #-----------print inserting doc----------#
            # print(f"inserting {ids} records with desc {mp3_description}Title: {mp3_title}\nMp3 Url: {mp3_url}")
            # print(f"inserted record:---------------{i} out of {no_of_rec}-----------------------------------------------------------------------------------\n")

        json_object = json.dumps(overallArr, indent = 4)
        outfile.write(json_object)



#--------------------------------------------#
def readFrmFile(filename, collname):
    print("now reading from json file to populate the database.............")
    
    # Opening JSON file
    f = open(filename)
    
    # returns JSON object as a dictionary
    data = json.load(f)
    collname.insert_many(data)
    
    # Closing file
    f.close()

# sendToFile(10000)
readFrmFile('sample_export.json', db['tbl_mp3_try'])