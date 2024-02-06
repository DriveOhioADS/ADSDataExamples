from ADSDynamo import ADSDynamo

#setup the database interface
datainterface = ADSDynamo()


#available so far...
groupids=[
'3a116996-93a9-11ee-956e-9da2d070324c',
'ba6e1072-9524-11ee-956e-9da2d070324c',
'2837eb9c-9542-11ee-956e-9da2d070324c',
'da853e0c-a10f-11ee-981c-d126ddbe9afa',
'154fab12-a43f-11ee-88ec-eb6a8d5269b4',
'f6ac3c82-a445-11ee-88ec-eb6a8d5269b4',
'58263e34-a45c-11ee-88ec-eb6a8d5269b4',
'c335d84c-a45c-11ee-88ec-eb6a8d5269b4',
'5976b77a-a504-11ee-88ec-eb6a8d5269b4',
'2bc6ebb8-a529-11ee-88ec-eb6a8d5269b4',
'7f09f6c6-a5b0-11ee-88ec-eb6a8d5269b4',
'f671c05c-a5e4-11ee-88ec-eb6a8d5269b4',
'90101c36-a621-11ee-88ec-eb6a8d5269b4']


# Fetch each metadata set by group id
# datain = []
# for groupid in groupids:
#     print(f"looking for {groupid}")
#     data = datainterface.GrabMetaDataByGroupID(groupid)
#     datain.append(data)
# for data in datain:
#     print(f"groupid -> {len(data)} @ {data[0]['foldername']}")


response = datainterface.GrabGPSDataSetFast(groupids[0], limit = 299)
items = response['items']
duration_s = response['duration_sec']
print(f"Query took {duration_s} seconds")
count = 1
for item in items:
    print(f"{count}: {item['_id']},{item['time']},{item['topic']},{item['latitude']},{item['longitude']},{item['heightMsl']}")
    count = count + 1

  

    