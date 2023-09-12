import json

# from queries.thermistor_query import ThermistorQuery
from helpers.dates_formatter import datesFormatter
from queries.compressor_query import CompressorQuery
from scripts.meanTimes import MeanTimes as mT

class CompressorDriver(object):
     #identifies the operation to do
     @classmethod
     def task(cls,bodyReq):
          print("bodd ",bodyReq)
          #identify params
          try:
               device = bodyReq["device"]
               cmd = bodyReq["cmd_op"]
               dateRange = bodyReq["range"]

               operationResult = cls.executeOp(device,cmd,dateRange)
               # print(operationResult)
               return operationResult

          except Exception as e:
               print("data request params error",e)
               return {"error":True, "status":400}

     @classmethod            
     def executeOp(cls,device,cmd,dateRange):
          commands = {
               "RAW":"raw",
               "MEAN_T":"mean_times"
          }

          if cmd==commands["RAW"]:
               return cls.raw(device,dateRange)
          elif cmd==commands["MEAN_T"]:
               return mT.meanTimeValues(cls.raw(device,dateRange), dateRange)

     @staticmethod          
     def gotValidResult(queryResult): 
          if not queryResult["err"]:
               if queryResult["data"]:
                    return True
          return False              

     #calls the compressor activation states queries driver
     @staticmethod          
     def queryHandler(device,dateRange,queryType):  
          if queryType=="byRange":
               return CompressorQuery.getByRange(device,dateRange)
          if queryType=="all":#ROWS LIMIT 900
               return CompressorQuery.getAll(device)

     @classmethod #sort and parse data in json (?) formatt           
     def preProcess(cls,rawData,columnNames):
          parsed = []
          DATE_INDEX = 0
          STATES_INDEX = 1

          for row in rawData:
               gpioState = cls.retrieveGPIO12(row[STATES_INDEX],columnNames)
               parsed.append({"timestamp":datesFormatter.basicFormat(row[DATE_INDEX]),"gpio12":gpioState}) 
               print("\n raw value :----- ", {"timestamp":datesFormatter.basicFormat(row[DATE_INDEX]),"gpio12":gpioState})          
    
          
          # print(parsed)
          return parsed 

     @staticmethod #BIT POSITION 12 has the compressor activation status         
     def retrieveGPIO12(states,columnNames):  
          gpiosStatus = []
          num_bits = 16
          # bits = [(row[1] >> bit) & 1 for bit in range(num_bits - 1, -1, -1)]
          for bit in range(num_bits - 1, -1, -1):
               if (states >> bit) & 1:
                    gpiosStatus.append(0)#invert state
               else:
                    gpiosStatus.append(1)          
          return gpiosStatus[3]               

     @classmethod #method to query samples raw           
     def raw(cls,device,dateRange):
          rawSamples = cls.queryHandler(device,dateRange,"byRange")
          # print(rawSamples)

          if cls.gotValidResult(rawSamples):
               parsedData = cls.preProcess(rawSamples["data"],rawSamples["references"])
               finalData = cls.chartParse(parsedData)

               return {"error":False,"data":finalData,"status": 200}
          else:
               #error ocurred
               return {"error":True,"status": cls.defineStatus(rawSamples)}  

     @classmethod #method to parse data for the client chart component             
     def chartParse(cls,parsedData):  
          pastState = -1
          lastIndex = len(parsedData)-1
          print("last index ",lastIndex)
          for index,row in enumerate(parsedData):
               state = row["gpio12"]
               parsedData[index]["chartParsed"] = state 

               if pastState == state and index > 0:
                    if not index == lastIndex:
                         parsedData[index]["chartParsed"] = None
                      
               pastState = state          
               # print("\n", parsedData[index])          

          return parsedData    

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