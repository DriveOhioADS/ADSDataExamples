from ADSDynamo import ADSDynamo
#setup the database interface
datainterface = ADSDynamo()
response = datainterface.GrabCyberDataByTopic(
    groupID = '90101c36-a621-11ee-88ec-eb6a8d5269b4', 
    topicName = '/apollo/sensor/gnss/odometry',#'/apollo/sensor/gnss/best_pose',
    limit = 1)

print(response)
