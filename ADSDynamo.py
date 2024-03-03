import boto3
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr

from halo import Halo
import time

class ADSDynamo:
    
    def __init__(self):
        EPURL = "https://dynamodb.us-east-2.amazonaws.com:443"
        self.dynamodb = boto3.resource('dynamodb', endpoint_url=EPURL,
                                #aws_access_key_id=akey,
                                #aws_secret_access_key=skey,
                                region_name="us-east-2", )
        self.metatable = self.dynamodb.Table('ads_passenger_processed_metadata')
        self.cybertable = self.dynamodb.Table('ads_passenger_processed')

        print("Table status: ", self.metatable.table_status)
        
     
    def GrabMetaDataByGroupID(self, groupid, ProjectionExpression=None):
        kwargs = {
                    'IndexName': 'groupMetadataID-index',
                    'KeyConditionExpression': Key('groupMetadataID').eq(groupid)
               }
        if(ProjectionExpression is not None):
            kwargs['ProjectionExpression'] = ProjectionExpression['ProjectionExpression']
            kwargs['ExpressionAttributeNames'] = ProjectionExpression['ExpressionAttributeNames']
        return self.QueryMetaDataUsingCondition(kwargs)
    
    def GrabMetaDataByTime(self, time):
        kwargs = {
                    'IndexName': 'groupMetadataID-index',
                    'KeyConditionExpression': Key('time').eq(time)
                }
        return self.QueryMetaDataUsingCondition(kwargs)
    
    def QueryMetaDataUsingCondition(self, kwargs):
        spinner = Halo(text='Performing query', text_color= 'cyan', color='red', spinner='dots')
        spinner.start()
        items = []
        items_scanned = 0
        item_count = 0
        
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    kwargs["ExclusiveStartKey"] = start_key
                response = self.metatable.query(**kwargs)
                items_scanned = items_scanned + response['ScannedCount']
                item_count = response['Count']
                items.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                #print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
                spinner.text = f'Items found via query -> {items_scanned}'
                done = start_key is None
        except botocore.exceptions.ClientError as err:
            print(f"Couldn't scan for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
        spinner.stop()
        return items   
    
    def GrabItemsByIDList(self, idlist, limit=None):
        time_start = time.time()
        totallist = []
        n=100 #max request
        for i in range(0, len(idlist), n):
            batch_keys = {
                self.cybertable.name: {
                    'Keys': [{'_id': checkid['_id'], 'time': checkid['time']} for checkid in idlist[i:i+n]],
                    'ConsistentRead': True
                },
            }
            
            response = self.dynamodb.batch_get_item(RequestItems=batch_keys)  
            items = response['Responses']['ads_passenger_processed']
            for item in items:
                totallist.append(item)
        time_total = time.time()-time_start #in seconds
        #return {"items":items,"duration_sec":time_total}
        return totallist
     
    def GrabCanbusChassis(self,groupID, limit=None):
        return self.GrabCyberDataByTopic(groupID,'/apollo/canbus/chassis',limit=limit)
    
    def GrabDriveEvent(self,groupID, limit=None):
        return self.GrabCyberDataByTopic(groupID,'/apollo/drive_event',limit=limit)
    
    def GrabGPSDataSet(self,groupID, limit=None):
        return self.GrabCyberDataByTopic(groupID,'/apollo/sensor/gnss/best_pose',limit=limit)
    
    def GrabGPSDataSetFast(self,groupID, limit=None):
        topicName='/apollo/sensor/gnss/best_pose'
        time_start = time.time()
        spinner = Halo(text='Performing query', text_color= 'cyan', color='green', spinner='dots')
        spinner.start()
        scan_kwargs = {
                    'IndexName': 'groupMetadataID-index',
                    'KeyConditionExpression': Key('groupMetadataID').eq(groupID),
                    "ProjectionExpression": "#_id, #time, topic",
                    "ExpressionAttributeNames": { "#_id": "_id" , "#time": "time"},
                }
        items_found = 0
        
        filteredItems = []
        try:
            done = False
            start_key = None
            while not done:
                items = []
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = self.cybertable.query(**scan_kwargs)
                items_found = items_found + response['ScannedCount']
                #item_count = response['Count']
                items.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                #print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
                done = start_key is None
                for newitem in items:
                    if(limit != None and len(filteredItems) >= limit):
                        break
                    if(topicName == newitem['topic']):
                        filteredItems.append(newitem)
                spinner.text = 'Items found via query -> {}'.format(len(filteredItems))
                if(limit != None and len(filteredItems) >= limit):
                    break
        except botocore.exceptions.ClientError as err:
            print(f"Couldn't scan for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
        #print(items_scanned)   
        idlist = []
        for item in filteredItems:
            idlist.append({'_id':item['_id'],'time':item['time']})
        
        filteredItems = self.GrabItemsByIDList(idlist)
        
        
        spinner.stop()
        time_total = time.time()-time_start #in seconds

        return {"items":filteredItems,"duration_sec":time_total} 
    
    def GrabCyberDataByTopic(self, groupID, topicName, limit=None):
        time_start = time.time()
        spinner = Halo(text='Performing query', text_color= 'cyan', color='green', spinner='dots')
        spinner.start()
        scan_kwargs = {
                    'IndexName': 'groupMetadataID-index',
                    'KeyConditionExpression': Key('groupMetadataID').eq(groupID)
                      }
        items_found = 0
        
        filteredItems = []
        try:
            done = False
            start_key = None
            while not done:
                items = []
                if start_key:
                    scan_kwargs["ExclusiveStartKey"] = start_key
                response = self.cybertable.query(**scan_kwargs)
                items_found = items_found + response['ScannedCount']
                #item_count = response['Count']
                items.extend(response.get("Items", []))
                start_key = response.get("LastEvaluatedKey", None)
                #print(f"{start_key} / {items_scanned} - {len(items)} + {item_count}")
                done = start_key is None
                for newitem in items:
                    if(topicName == newitem['topic']):
                    #if('pose' in newitem['topic'] and newitem['topic'] != '/apollo/localization/pose'):
                        filteredItems.append(newitem)
                spinner.text = 'Items found via query -> {}'.format(len(filteredItems))
                if(limit != None and len(filteredItems) >= limit):
                    break
        except botocore.exceptions.ClientError as err:
            print(f"Couldn't scan for item. Here's why: {err.response['Error']['Code']} -> {err.response['Error']['Message']}")
        #print(items_scanned)   
        spinner.stop()
        time_total = time.time()-time_start #in seconds

        return {"items":filteredItems,"duration_sec":time_total} 