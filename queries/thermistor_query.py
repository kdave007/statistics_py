import mysql.connector
import datetime
import sys

from configs.db_config import DataBaseParams

class ThermistorQuery(object):
     @staticmethod
     def getByRange(device,dataRange):
          start = dataRange[0]
          end = dataRange[1]
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "SELECT timestamp as dateTime,temp1,temp2,temp3,temp4 FROM device_data_temp_volt WHERE deviceId = {} AND timestamp BETWEEN FROM_UNIXTIME({}) AND FROM_UNIXTIME({}) ".format(device,start,end)
               db_cursor.execute(mainQuery)
               myresult = db_cursor.fetchall()

               # for row in myresult:
               #      print(row)

               if len(myresult):
                    return {"err":False,"data":myresult,"references":db_cursor.column_names}

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True}


     @staticmethod
     def getSetPoints(device):
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "SELECT * FROM atechnik_hTelemetry.thermistor_setpoints WHERE device ={} order by thermistor".format(device)
               db_cursor.execute(mainQuery)
            
               myresult = db_cursor.fetchall()

               return {"err":False,"data":myresult,"references":db_cursor.column_names}                         

          except Exception as e:
               print(e)
               return {"err":True}    

     @staticmethod
     def setSetPoints(setpointId,device,thermistor,sampleValue,timestamp,inputValue):
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "INSERT INTO thermistor_setpoints SET setpointId={},device={},thermistor={},sampleValue={},timestamp='{}',inputValue={}".format(setpointId,device,thermistor,sampleValue,timestamp,inputValue)
               mainQuery +=" ON DUPLICATE KEY UPDATE sampleValue={},timestamp='{}',inputValue={};".format(sampleValue,timestamp,inputValue)
               print(mainQuery)
               db_cursor.execute(mainQuery)
               db.commit()#always end with this line when inserting/updating data

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True}                             

     @staticmethod
     def deleteAll(device):
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "DELETE FROM thermistor_setpoints WHERE device={}".format(device)
              
               print(mainQuery)
               db_cursor.execute(mainQuery)
               db.commit()#always end with this line when inserting/updating data

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True} 

     @staticmethod
     def getMktAConsts(device):
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "SELECT heat_activation, unv_gas_const FROM mkt_const_values WHERE DeviceID = {}".format(device)
               db_cursor.execute(mainQuery)
               myresult = db_cursor.fetchall()

               # for row in myresult:
               #      print(row)

               if len(myresult):
                    return {"err":False,"data":myresult,"references":db_cursor.column_names}

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True} 

     @staticmethod
     def setGraphFilterValues(device,configId,value):
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "INSERT INTO interface_device_config SET configId={},deviceId={},value={} ".format(configId,device,value)
               mainQuery +=" ON DUPLICATE KEY UPDATE value={};".format(value)
               print(mainQuery)
               db_cursor.execute(mainQuery)
               db.commit()#always end with this line when inserting/updating data

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True}


     @staticmethod
     def getGraphFilterValues(device):                         
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "SELECT * FROM interface_device_config WHERE deviceId = {}".format(device)
               db_cursor.execute(mainQuery)
               myresult = db_cursor.fetchall()

               # for row in myresult:
               #      print(row)

               if len(myresult):
                    return {"err":False,"data":myresult,"references":db_cursor.column_names}

               return {"err":False,"data":False}                         

          except Exception as e:
               print(e)
               return {"err":True}