import mysql.connector
import datetime
import sys

from configs.db_config import DataBaseParams

class CompressorQuery(object):
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

               mainQuery = "SELECT timestamp,sensorData FROM device_data_sensor WHERE DeviceID = {} AND timestamp BETWEEN FROM_UNIXTIME({}) AND FROM_UNIXTIME({}) ORDER BY timestamp ASC".format(device,start,end)
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
     def getAll(device):#ROWS LIMIT 900
          try:
               db = mysql.connector.connect(
                    host = DataBaseParams.host,
                    user = DataBaseParams.user,
                    password = DataBaseParams.password,
                    database = DataBaseParams.database
               )

               db_cursor = db.cursor()

               mainQuery = "SELECT timestamp,sensorData FROM device_data_sensor WHERE DeviceID = {} ORDER BY timestamp DESC LIMIT 900".format(device)
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