import datetime

class queryJsonSort(object):
     @classmethod
     def sort(cls,rows,columnNames):
          parsed = []
          for row in rows:
               parsedRow = cls.parseRow(row,columnNames)
               parsed.append(parsedRow)
          return parsed 

          
     @classmethod     
     def parseRow(cls,row,columnNames):
          newRow = {}
          for i, key in enumerate(columnNames):
                    if key=="dateTime" or key=="timestamp" :
                         newRow[key] = cls.basicFormat(row[i])
                    else:
                         newRow[key] = row[i]   

          return newRow 

     @staticmethod
     def basicFormat(cls,dateObject):
          try:
               year = dateObject.strftime("%Y")
               month = dateObject.strftime("%m")
               day = dateObject.strftime("%d")
               time = dateObject.strftime("%H:%M:%S")
               stringDate = year+"-"+month+"-"+day+" "+time
              
               return stringDate

          except Exception as e:
               print("query json sort :: data time format params error",e)
               return None      