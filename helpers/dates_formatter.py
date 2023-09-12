import datetime

class datesFormatter(object):
     @classmethod
     def basicFormat(cls,dateObject):
          try:
               year = dateObject.strftime("%Y")
               month = dateObject.strftime("%m")
               day = dateObject.strftime("%d")
               time = dateObject.strftime("%H:%M:%S")
               stringDate = year+"-"+month+"-"+day+" "+time
              
               return stringDate

          except Exception as e:
               print("data request params error",e)
               return None     



