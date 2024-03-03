import pickle
import matplotlib.pyplot as plt
import numpy as np

canbus_chassis = pickle.load(open('canbus.pkl','rb'))
# canbus_chassis = canbus_chassis['items']

GPSLocations = pickle.load(open('gpsloc.pkl','rb'))

print("Sorting...")

canbus_chassis.sort(key=lambda x:x['header']['timestampSec'])
GPSLocations.sort(key=lambda x:x['header']['timestampSec'])
print(f"CAN # {len(canbus_chassis)}")
print(f"GPS # {len(GPSLocations)}")

eventLocations = []
count = 0
for can in canbus_chassis:
    time_to_find = can['time']
    gpsfound = None
    for gps in GPSLocations:
        if(gps['time'] >= time_to_find):
            gpsfound = gps
            break
    if(gpsfound is not None):
        #print(f"Found gps {can['time']} / {gpsfound['time']} {gpsfound['longitude']} {gpsfound['latitude']}")
        eventLocations.append({'can':can,'gps':gpsfound})
        count = count + 1
    else:
        print(f"Missing {can['time']}")
        
#print(eventLocations)
print(f"Found {count} pairs")
x=[]
y=[]
for gps in GPSLocations:
    x.append(gps['longitude'])
    y.append(gps['latitude'])

xmax = max(x)
xmin = min(x)
ymin = max(y)
ymax = min(y)
plt.scatter(x,y,s=10)
x=[]
y=[]
c=[]
s=[]
for item in eventLocations:
    color = 'red'
    size = 100
    #print(item['can']['drivingMode'])
    if(item['can']['drivingMode'] == 'COMPLETE_AUTO_DRIVE'):
        color = 'green'
        size = 50
    x.append(item['gps']['longitude'])
    y.append(item['gps']['latitude'])
    c.append(color)
    s.append(size)

plt.scatter(x, y, color=c,s=s)
ax = plt.gca()
# ax.set_xlim([xmin, xmax])
# ax.set_ylim([ymin, ymax])
plt.show()