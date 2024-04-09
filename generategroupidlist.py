from ADSDynamo import ADSDynamo
import glob
import os
import decimal
import json
import pandas as pd
import pickle

datainterface = ADSDynamo()


s3location = ""
with open('s3location.txt') as f:
    s3location=f.readline().replace('\n','')
print(f"Path to s3 data: {s3location}")
searchlist = {  "Deployment_2_SEOhio/Blue Route/OU Pacifica/",
                "Deployment_2_SEOhio/RedRoute/OU Pacifica/",
                "Deployment_2_SEOhio/GreenRoute/OU Pacifica/",
                "Deployment_2_SEOhio/RedRoute/TRCVan1/",
                "Deployment_2_SEOhio/RedRoute/TRCVan2/",
                "Deployment_2_SEOhio/GreenRoute/TRCVan1/",
                "Deployment_2_SEOhio/GreenRoute/TRCVan2/",
                "Deployment_2_SEOhio/Blue Route/TRCVan1/",
                "Deployment_2_SEOhio/Blue Route/TRCVan2/"
                }
group_list = []
skipped = []
for search in searchlist:
    searchpath = os.path.join(s3location, search)
    exp_folderlist = glob.glob(searchpath+"*")
    exp_folderlist.sort()
    #print(exp_folderlist)
    for folder_item in exp_folderlist:
        fullfolder = folder_item#os.path.join(searchpath,folder_item)
        try:
            with open(os.path.join(fullfolder,'groupid.txt')) as f:
                groupid = f.readline()
        except FileNotFoundError:
            print(f"Skipping path - no groupid file {folder_item}")
            skipped.append(folder_item)
            continue
        print(f"Folder: {folder_item} GroupID: {groupid}")
        group_list.append(groupid)
print(f"Found {len(group_list)} ids to check")
#print(group_list)
metalist=[]
ProjectionExpression={}
ProjectionExpression['ProjectionExpression'] = "#_id, #time, endTime, filename, groupMetadataID, size, msgnum, vehicleID, experimentID"
ProjectionExpression['ExpressionAttributeNames'] = {"#_id":"_id", "#time":"time"}
for groupid in group_list:
    metadata = datainterface.GrabMetaDataByGroupID(groupid, ProjectionExpression)
    if(len(metadata)==0):
        continue
    #print(metadata)
    metadata.sort(key=lambda x:x['time'])
    start_time = metadata[0]['time']
    end_time = metadata[-1]['time']
    nummsgs = 0
    for meta in metadata:
        nummsgs = nummsgs + meta['msgnum']
    vid = metadata[0]['vehicleID']
    if(isinstance(vid, decimal.Decimal)):
        vid = int(metadata[0]['vehicleID'])
    eid = metadata[0]['experimentID']
    if(isinstance(eid, decimal.Decimal)):
        eid = int(metadata[0]['experimentID'])
    metapkg = {'groupMetadataID':metadata[0]['groupMetadataID'],
               'start_time':int(start_time),
               'end_time': int(end_time),
               'nummsgs': int(nummsgs),
               'filebase': metadata[0]['filename'],
               'folder': metadata[0]['filename'],
               'vehilceID':vid,
               'experimentID':eid}
    #print(metapkg)
    metalist.append(metapkg)
metalist.sort(key=lambda x:x['start_time'])
pickle.dump(metalist,open('metalist.pkl','wb'))


df = pd.DataFrame(metalist)
df.to_csv('ads_data_index.csv', index=True)  

#id, filename, time, endtime, number of msgs, exp, driver