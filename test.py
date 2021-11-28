import json
import requests
page = requests.get('https://gcc.azure-api.net/datextraffic/locations?format=json')
data1=page.json()
print(data1)

# Serializing json 
json_data1 = json.dumps(data1, indent = 4)
with open("locations2.json", "w") as outfile:
    outfile.write(json_data1)


page = requests.get('https://gcc.azure-api.net/traffic/v1/movement/now')
data3=page.json()
print(data3)

# Serializing json 
json_data3 = json.dumps(data3, indent = 12)


with open("movement2.json", "w") as outfile:
    outfile.write(json_data3)

page = requests.get('https://gcc.azure-api.net/traffic/v1/movement/history')
data4=page.json()
print(data4)


# Serializing json 
json_data4 = json.dumps(data4, indent = 4)


with open("history2.json", "w") as outfile:
    outfile.write(json_data4)
 

