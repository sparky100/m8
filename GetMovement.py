#! /usr/bin/python3
import json
import requests
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime

now = datetime.now()
current_time = now.strftime("%H:%M:%S")

#page = requests.get('https://gcc.azure-api.net/traffic/v1/movement/now?size=1500')
page=requests.get('https://gcc.azure-api.net/datextraffic/movement?format=json')
currentTraffic=page.json()

# Serializing json 
movementNow = json.dumps(currentTraffic, indent = 12)

with open("movementNowdataex.json", "w") as outfile:
    outfile.write(movementNow)

with open('movementNowdataex.json') as json_file:
    data = json.load(json_file)    

measurements=data['d2LogicalModel']['payloadPublication']['siteMeasurements']
recordedtime=data['d2LogicalModel']['payloadPublication']['publicationTime']

allData=pd.json_normalize(measurements, record_path='measuredValue', meta=['measurementSiteReference'])

allFlowData=allData[allData['basicDataValue.@xsi$type']=='TrafficFlow']

allFlowData2=allFlowData[['measurementSiteReference','basicDataValue.vehicleFlow']]

#make array 
times=pd.Series([data['d2LogicalModel']['payloadPublication']['publicationTime']]*len(allFlowData2.index))
allFlowData2.insert(0,'RecTime',times.values)

df=allFlowData2.rename(columns={'measurementSiteReference':'SiteId',"basicDataValue.vehicleFlow":"Flow"})
df['RecTime']=pd.to_datetime(df['RecTime'])
df['Flow']=pd.to_numeric(df['Flow'])

import cx_Oracle

con = cx_Oracle.connect('SIMON', 'GodelEscherBach65?', 'db202006252115_high')
cur=con.cursor()

statement = 'insert into movement values (:1,:2,:3)'
df_list=df.values.tolist()
df_list

n=0
for i in df.iterrows():
    cur.execute(statement, df_list[n])
    n+=1 
con.commit()

## testing PostGres database

import psycopg2
import sys
from sqlalchemy import create_engine

# Here you want to change your database, username & password according to your own values
param_dic = {
    "host"      : "localhost",
    "database"  : "movement",
    "user"      : "simon",
    "password"  : "Babbage65?"
}

conn_string = 'postgresql+psycopg2://simon:Babbage65?@localhost:5432/movement'
  
db = create_engine(conn_string)
conn = db.connect()

df.to_sql('movement', con=conn, if_exists='append', index=False)

  
# conn.commit()
conn.close()


## testing PostGres database

import pymysql
import sys
from sqlalchemy import create_engine

# Here you want to change your database, username & password according to your own values
param_dic = {
    "host"      : "localhost",
    "database"  : "movement",
    "user"      : "simon",
    "password"  : "Babbage65?"
}
conn_string = 'mysql+pymysql://simon:Babbage65?@localhost/movement'
  
db = create_engine(conn_string)
conn = db.connect()

df.to_sql('movement', con=conn, if_exists='append', index=False)

  
# conn.commit()
conn.close()

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("FInish Time =", current_time," ","Observations ",len(df))
