#Integration testing for watershed deliniation

#region "Imports"
import traceback
import datetime
import time
import os
import argparse
import fnmatch
import json
from WIMLib import WiMLogging
from WIMLib import Shared
from WIMLib.Config import Config
from ServiceAgents.StreamStatsServiceAgent import StreamStatsServiceAgent

from threading import Thread

import time
import random
from Queue import Queue

#endregion

##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+
#http://stackoverflow.com/questions/13653991/passing-quotes-in-process-start-arguments

class IntigrationTest(object):
    def __init__(self):
        self.maxThreads = 5
        try:
            self.config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
            self.workingDir = Shared.GetWorkspaceDirectory(self.config["workingdirectory"])
            
            parser = argparse.ArgumentParser()
            #Use the following LAT/LON pour point
            parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str,
                                default = 'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
            parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                                default = '4326')
            args = parser.parse_args()
            if not os.path.isfile(args.file): raise Exception("File does not exist")

            refDir = {"bdel":self.config["referenceFolderBasinDel"],"bchar":self.config["referenceFolderBasinChar"]}

            file = Shared.readCSVFile(args.file)
            headers = file[0]
            rcode = headers.index("State") if "State" in headers else 0
            x = headers.index("dec_long") if "dec_long" in headers else 1
            y = headers.index("dec_lat") if "dec_lat" in headers else 2
            uniqueID = headers.index("GageID") if "GageID" in headers else 3
            file.pop(0)#removes the header
            startTime = time.time()
            WiMLogging.init(os.path.join(self.workingDir,"Temp"),"Integration.log")
            self._sm("Starting routine")

            queue= Queue()  
            for thrd in range(self.maxThreads):
                worker = ThreadWorker(queue)
                worker.start()

            for row in file:
                queue.put((row[rcode],row[x],row[y],refDir,row[uniqueID], self.workingDir))


            self._sm('Finished.  Total time elapsed:', str(round((time.time()- startTime)/60, 2)), 'minutes')

        except:
            tb = traceback.format_exc()
            self._sm("Error executing delineation wrapper "+tb)


    def _sm(self,msg,type="INFO", errorID=0):        
        WiMLogging.sm(msg,type, errorID)
        #print type, msg


class ThreadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue
        
    def run(self):
        try:            
            while True:
                rcode,x,y,refdir,id,workspace = self.queue.get()
                try:
                    self._run(rcode,x,y,refdir,id,workspace)
                except:
                    tb = traceback.format_exc()
                    WiMLogging.sm("Error w/ run "+ tb)
                finally:
                    self.queue.task_done()
            #next
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error running "+tb)


    def _run(self,rcode, x,y, path,siteIdentifier,workingDir):   
        try:
            result = None
            
            with StreamStatsServiceAgent() as sa: 
                try:
                    response = sa.getBasin(rcode,x,y,4326) #Get feature collection
                    responseBChar = sa.getBChar(rcode,response['workspaceID'])
                    resultBChar = responseBChar['parameters']
                    result = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates']
                except:
                    pass                

            if result == None: raise Exception("{0} Failed to return from service".format(siteIdentifier))
            if resultBChar == None: raise Exception ("{0} Failed to return from service Bchar".format(siteIdentifier))
            self._compare(result, path.get("bdel"),siteIdentifier,workingDir)
            self._compare(resultBChar, path.get("bchar"),siteIdentifier,workingDir)
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error w/ station "+ tb)

    def _writeToJSONFile(self,path, fileName, data):  #Define function to write as json object
    #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
        try:
            filePathNameWExt = os.path.join(path,fileName+".json")
            with open(filePathNameWExt, 'w') as fp:
                json.dump(data, fp)
        except:
            tb=traceback.format_exc()
            WiMLogging.sm("Error writing json output "+tb)

    def _compare(self,inputObj,path,ID, workingDir):
        try:  
            refObj = None
            refFile = os.path.join(path, ID+".json") #Get the reference json file from existing root folder
            if os.path.isfile(refFile):
                with open (refFile) as f:
                    refObj = json.load(f)

                if inputObj!= refObj:
                    WiMLogging.sm("Not equal Json's"+" "+ID)
                    self._writeToJSONFile(workingDir,ID+"_"+str(path.rsplit('/', 1)[-1]),inputObj) #Store in log folder
                else:
                    tb = traceback.format_exc()
                    WiMLogging.sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
            else:
                #file not in reference folder, Create it
                WiMLogging.sm("file not in reference folder, Creating it"+" "+refFile)
                self._writeToJSONFile(path, ID,inputObj)
        except:
            tb=traceback.format_exc()
            WiMLogging.sm("Error Comparing "+tb)
            self._writeToJSONFile(workingDir, ID+"_viaError",{'error':tb})

    #Create function to compare basin char
if __name__ == '__main__':
    IntigrationTest()
      