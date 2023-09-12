from http.server import BaseHTTPRequestHandler,HTTPServer
import mysql.connector
import json
import sys

from controllers.thermistor_driver import ThermistorDriver
from controllers.compressor_samples_driver import CompressorDriver
from configs.db_config import DataBaseParams


class requestHandler(BaseHTTPRequestHandler):
     #parse request body to a dictionary
     def parseBody(self):
          try:
               content_len = int(self.headers.get('Content-Length'))
               post_body = self.rfile.read(content_len)
               body_unicode = post_body.decode('utf-8')
               body_data = json.loads(body_unicode)
               return body_data

          except Exception as e:
               print("body parse exception",e)
               return None

     #identifies post method path
     def taskSelector(self,reqBody):
          pathDir = {
               'THERMISTOR' : '/thermistor',
               'L_SENSOR' : '/light',
               'COMPRESSOR' : '/comp',
               'MISC':'/misc'
          }

          #identify and handle by the correct driver
          if self.path.endswith(pathDir['THERMISTOR']):
               return ThermistorDriver.task(reqBody)

          elif self.path.endswith(pathDir['L_SENSOR']):
               print('POST to light',reqBody)

          elif self.path.endswith(pathDir['COMPRESSOR']):
               print('POST to comp',reqBody)
               return CompressorDriver.task(reqBody)

          elif self.path.endswith(pathDir['MISC']):
               print('POST to misc',reqBody)


     def do_POST(self):
          body_data = self.parseBody()

          if not body_data == None:
               response = self.taskSelector(body_data)

               if (not response==None) and (not response["error"]):
                    # print("got a response :",json.dumps(response["data"]))
                    responseSorted = json.dumps(response["data"])
                    self.send_response(response["status"])
                    self.send_header('content-type','application/json')
                    self.send_header('content-Length',len(responseSorted))
                   
                    self.end_headers()
                    self.wfile.write(bytes(responseSorted, 'utf-8'))
                    return

          if response==None:
               self.send_response(400)
          else:
               self.send_response(response["status"])     

          self.end_headers()


#main function
def main():
     #server address params
     PORT=9000
     server_address = ("localhost",PORT)

     #start server
     server = HTTPServer(server_address,requestHandler)
     print("python analytics server starting on port :",server_address[1])
     server.serve_forever()


#set this script as the main script
if __name__ == "__main__":
    main()