import json
import datetime
import pytz
from queries.thermistor_query import ThermistorQuery
from helpers.dates_formatter import datesFormatter
from helpers.query_json_sort import queryJsonSort
from scripts.meanKineticTemperature import MeanKineticTemperature as mkt
from scripts.ramerDouglas import ramerDouglas as rdp

class ThermistorDriver(object):
     #identifies the operation to do
     @classmethod
     def task(cls,bodyReq):
          print("bodd ",bodyReq)
          #identify params
          try:

               operationResult = cls.executeOp(bodyReq)
              # print(operationResult)
               return operationResult
          except Exception as e:
               print("data request params error",e)
               return {"error":True, "status":400}

     #executes the operation given
     @classmethod            
     def executeOp(cls,bodyReq):
          commands = {
               "RAW":"raw",
               "MEAN_KINETIC":"mean_kinetic",
               "RDP": "rdp",
               "SET_REF_POINTS":"set_ref_points",
               "GET_REF_POINTS":"get_ref_points",
               "LOW_PASS":"low_pass"
          }

          device = bodyReq["device"]
          cmd = bodyReq["cmd_op"]
          extra = bodyReq["extra"]

          if cmd==commands["RAW"]:
               dateRange = bodyReq["range"]
               filtered = False
               return cls.rawCommand(device,dateRange,filtered)

          if cmd==commands["LOW_PASS"]:
               dateRange = bodyReq["range"]
               filtered = True
               return cls.rawCommand(device,dateRange,filtered)               

          if cmd==commands["MEAN_KINETIC"]:
               dateRange = bodyReq["range"]   
               filtered = False  
               return mkt.mktvalues(cls.rawCommand(device,dateRange,filtered), device)

          if cmd==commands["RDP"]:
               dateRange = bodyReq["range"]
               filtered = False
               return rdp.rdpValues(cls.rawCommand(device,dateRange,filtered))

          if cmd==commands["SET_REF_POINTS"]:
               # 1 - insert/update setpoint values in table
               # 2 - calculate constant values and insert/update them in device config table (table must be updated, pending!)

               cls.setRefPoints(device,extra)
               return {"error":False,"data":True,"status": 200}

          if cmd==commands["GET_REF_POINTS"]: 
               return cls.getRefPoints(device)
                       

     #  ///////////////////////////////////////////////////////////////////////////////////
     # ///////////setpoints and constant values to calibrate thermistors methods here:////
     #///////////////////////////////////////////////////////////////////////////////////

     @classmethod     
     def getRefPoints(cls,device):
          result = ThermistorQuery.getSetPoints(device)
          
          if cls.gotValidResult(result):
               parsedData = cls.preProcessSetpoints(result["data"],result["references"])
               return {"error":False,"data":parsedData,"status": 200}
          else:
               #error ocurred
               return {"error":True,"status": cls.defineStatus(result)}

     @classmethod     
     def setRefPoints(cls,device,extra):
          ThermistorQuery.deleteAll(device)
          for index,value in enumerate(extra):
               cls.insertSetpointsParams(value,device)
 
     @classmethod #sort and parse only the setpoints           
     def preProcessSetpoints(cls,rawData,columnNames):
          parsed = []
          for row in rawData:
               parsedRow = cls.parseValuesToJsonSetpoints(row,columnNames)
               parsed.append(parsedRow)
          return parsed 

     @staticmethod # parse data in json (?) formatt           
     def parseValuesToJsonSetpoints(row,columnNames):  
          newRow = {}
          for i, key in enumerate(columnNames):
               if key=="timestamp":
                    newRow[key] = datesFormatter.basicFormat(row[i])
               else:
                    newRow[key] = row[i]        
          return newRow                                                        

     @staticmethod #         
     def insertSetpointsParams(rows,device):
          for index,value in enumerate(rows["samples"]):
               ThermistorQuery.setSetPoints(index+1,device,rows["id"]+1,value["temp"],value["timestamp"],value["value"])
               


     #  ///////////////////////////////////////////////////////////////////////////////
     # ////////////////////////thermistor samples methods here:///////////////////////
     #///////////////////////////////////////////////////////////////////////////////
     @classmethod     
     def rawCommand(cls,device,dateRange,filtered):
          rawSamples = cls.queryHandler(device,dateRange)

          if cls.gotValidResult(rawSamples):
               parsedData = cls.preProcessSamples(rawSamples["data"],rawSamples["references"])
               print("filtered ?? ",filtered)
               if filtered :
                    finalData = cls.processDataSamples(parsedData,device)      
               else :
                    finalData = parsedData

               return {"error":False,"data":finalData,"status": 200}
          else:
               #error ocurred
               return {"error":True,"status": cls.defineStatus(rawSamples)}

     #calls the thermistors queries driver
     @staticmethod          
     def queryHandler(device,dateRange):
          #convert the millis utc to datetime object
          utc_datetime_s = datetime.datetime.utcfromtimestamp(dateRange[0])
          utc_datetime_e = datetime.datetime.utcfromtimestamp(dateRange[1])

          #set the timezone of the new object datetime
          utc_datetime_s = utc_datetime_s.astimezone(pytz.timezone('America/Mexico_City'))
          utc_datetime_e = utc_datetime_e.astimezone(pytz.timezone('America/Mexico_City'))

          #get the millis back from the new datetime object
          millisec_start = utc_datetime_s.timestamp()
          millisec_end = utc_datetime_e.timestamp()

          # print("START MILLIS TZ ",millisec_start,millisec_end)
          # print("range request: start at",start," to ",end," in timezone ",datetime.datetime.fromtimestamp(dateRange[0]).tzinfo)
          # print("range request:by mex timezone --- start at",utc_datetime_s," to ",utc_datetime_e," in timezone ",utc_datetime_e.tzinfo)
          # print(pytz.all_timezones)
          return ThermistorQuery.getByRange(device,[millisec_start,millisec_end])  

     @staticmethod          
     def gotValidResult(queryResult): 
          if not queryResult["err"]:
               if queryResult["data"]:
                    return True
          return False

     @classmethod #sort and parse            
     def preProcessSamples(cls,rawData,columnNames):
          parsed = []
          for row in rawData:
               parsedRow = cls.parseValuesToJsonSamples(row,columnNames)
               parsed.append(parsedRow)
          return parsed     

     @staticmethod # parse data in json (?) formatt           
     def parseValuesToJsonSamples(row,columnNames):  
          newRow = {}
          for i, key in enumerate(columnNames):
               if key=="dateTime":
                    newRow[key] = datesFormatter.basicFormat(row[i])
               else:
                    newRow[key] = row[i]        
          return newRow     

     @staticmethod          
     def defineStatus(reference):
          if reference["err"]:
               #server failed to process query
               return 500
          else:
               if not reference["data"]:
                    #server couldn't find the requested data
                    return 204
          return 200

     #METHOD TO CALL CLASS TO PROCESS DATA IF NEEDED
     @classmethod            
     def processDataSamples(cls,parsedData,device):  
          #complementary filter
          ks = cls.fetchConstants(device)
          index = 0
          
          for row in parsedData:
               if index<1 :
                    f_t1 = row["temp1"]
                    f_t2 = row["temp2"]
                    f_t3 = row["temp3"]
                    f_t4 = row["temp4"]

               f_t1 = round((f_t1*(1-ks[0]))+( row["temp1"]*(ks[0])),2)
               f_t2 = round((f_t2*(1-ks[1]))+( row["temp2"]*(ks[1])),2)
               f_t3 = round((f_t3*(1-ks[2]))+( row["temp3"]*(ks[2])),2)
               f_t4 = round((f_t4*(1-ks[3]))+( row["temp4"]*(ks[3])),2)
               
               #print( "f_t1 ",f_t1," temp1 ",row["temp1"]," temp2 ",row["temp2"]," temp3 ",row["temp3"]," temp4 ",row["temp4"] )
               
               row["temp1"] = f_t1
               row["temp2"] = f_t2
               row["temp3"] = f_t3
               row["temp4"] = f_t4
               index+=1

          return parsedData
          
     @classmethod            
     def fetchConstants(cls,device):
          result = ThermistorQuery.getGraphFilterValues(device)
          print("fetchConstants: ",result)
          if(not result["err"]):
               if(not result["data"]):
                    pass
               else:
                    sortedjJson = queryJsonSort.sort(result["data"],result["references"])
                   # print("we got something!",sortedjJson)
                   # print("sorted",cls.sortKs(sortedjJson))
                    return cls.sortKs(sortedjJson)

          return [0,0,0,0]

     @staticmethod
     def sortKs(data):
          defaultKs = [0,0,0,0]
          index = 0
          for row in data:
               try :
                    #we convert the value from string to float
                    row["value"] = float(row["value"])
               except Exception as e:
                    #if the string wasnt a number, dont parse to float
                    pass

               if(row["value"] is not None and type(row["value"]) is not str):
                    #only save values in the array if they are numbers, else 0 is default
                    defaultKs[index]= float(row["value"])

               index+=1     

          return defaultKs

                   