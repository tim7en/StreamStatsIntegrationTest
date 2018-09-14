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

#endregion

##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+
#http://stackoverflow.com/questions/13653991/passing-quotes-in-process-start-arguments
class IntegrationWrapper(object):

    #region Constructor
    def __init__(self):

        try:
            self.config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
            self.workingDir = Shared.GetWorkspaceDirectory(self.config["workingdirectory"])
            existingFiles=dict([("StatID",self._listFiles(os.path.normpath (self.config["referenceFolderStationID"]))),
                            ("WorkID", self._listFiles(os.path.normpath (self.config["referenceFolderWorkspaceID"])))])
            parser = argparse.ArgumentParser()
            #Use the following LAT/LON pour point
            parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str,
                                default = 'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
            parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                                default = '4326')
            parser.add_argument("-checkParams", help="Bool indicating characteristic step", type=bool,
                                default = True)


            args = parser.parse_args()
            if not os.path.isfile(args.file): raise Exception("File does not exist")
            file = Shared.readCSVFile(args.file)
            headers = file[0]
            rcode = headers.index("State") if "State" in headers else 0
            x = headers.index("dec_long") if "dec_long" in headers else 1
            y = headers.index("dec_lat") if "dec_lat" in headers else 2
            uniqueID = headers.index("GageID") if "GageID" in headers else 3

            file.pop(0)#removes the header
            startTime = time.time()
            WiMLogging.init(os.path.join(self.workingDir,"Temp"),"Integration.log")
            WiMLogging.sm("Starting routine")

            file=sorted(file, key = lambda x: int(x[3]))    #Sort by 3rd element, siteID 3rd column
            i=1     #Loop, site-id should be sorted, otherwise overwrite will happen
            lastUnique = None
            for row in file:            
                if (row[uniqueID]!=lastUnique):
                    i=1
                    rowID=str (row[uniqueID])
                else:
                    rowID=str(row[uniqueID])+str("_test_")+str(i) #Create unique rowID for each discrete point location
                    i=i+1
                lastUnique=row[uniqueID]
                #self._run(row[id],row[x],row[y],rowID,existingFiles)
                thr=Thread(target=self._run, args=(row[rcode],row[x],row[y],rowID,existingFiles,args.checkParams))
                thr.start()
                
                

            WiMLogging.sm('Finished.  Total time elapsed:', str(round((time.time()- startTime)/60, 2)), 'minutes')

        except:
             tb = traceback.format_exc()
             WiMLogging.sm("Error executing delineation wrapper "+tb)

#Main function involving streamstats library
    def _run(self,rcode, x,y, rowID, existingFiles, doparams = False):
        try:
            print "Running "+rowID
            with StreamStatsServiceAgent() as sa:
                if (doparams):
                    folderPath = (os.path.normpath (self.config["referenceFolderStationID"])+"\\")
                    folderPath = folderPath.replace(os.sep, '/')
                    try:
                        sourceFilePath = folderPath+rowID+".json"
                        with open (sourceFilePath) as f:
                            sourceFile=json.load(f)
                            rowID = sourceFile['workspaceID'] #Get corresponding workspaceID
                    except:
                        sourceFile = sa.getBasin(rcode,x,y,4326) #Get feature collection if it does not exist
                        rowID = sourceFile['workspaceID'] #Get corresponding workspaceID
                        print "Warning: UNIQUE WORKSPACEID will be generated, duplicate station ID in csv input "
                    folderPoint = existingFiles["WorkID"] #Point to clean room
                    folderPath = (os.path.normpath (self.config["referenceFolderWorkspaceID"])+"\\")
                    folderPath = folderPath.replace(os.sep, '/')
                    inputJson = {"parameters":sa.getBChar(rcode,sourceFile['workspaceID'])['parameters']} #Get basin parameters
                else:
                    folderPoint = existingFiles["StatID"] #Point to clean room
                    folderPath = (os.path.normpath (self.config["referenceFolderStationID"])+"\\")
                    folderPath = folderPath.replace(os.sep, '/')
                    inputJson = sa.getBasin(rcode,x,y,4326) #Get feature collection
                self._compareJsons(inputJson,folderPoint,folderPath,rowID)         
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error w/ station "+ rcode +" "+ tb)

    def _listFiles(self,folder): #Function to list files in the directory
        try:
            for root, dirs, files in os.walk(folder, topdown=False):
                existingFiles=files
            return existingFiles
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Something wrong with folder path "+ folder +" "+ tb)

    def _writeToJSONFile(self,path, fileName, data):  #Define function to write as json object
    #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
        filePathNameWExt = path + '/' + fileName + '.json'
        try:
            with open(filePathNameWExt, 'w') as fp:
                json.dump(data, fp)
        except:
            tb=traceback.format_exc()
            WiMLogging.sm("Error writing json output "+tb)

    def _compareJsons (self,inputJson,folderPoint,folderPath,ID):

        rowID=ID
        refJson = folderPath+rowID+".json" #Get the reference json file from existing root folder
        if (rowID+".json" in folderPoint): #Check if the unique ID exists in the clean room
            with open (refJson) as f:
                existing_json = json.load(f)
                if "parameters" not in existing_json or inputJson["parameters"]!=existing_json["parameters"]:
                    tb = traceback.format_exc()
                    WiMLogging.sm("Not equal Json's"+" "+rowID+" "+ tb)
                    self._writeToJSONFile(self.workingDir,rowID,inputJson) #Store in log folder
                else:
                    tb = traceback.format_exc()
                    WiMLogging.sm("Equal Json's"+" "+rowID+" "+ tb) #Don't create file
        else:
            tb = traceback.format_exc()
            if "_test_" in rowID:
                WiMLogging.sm("New Json, duplicate. Specify unique property in addition to GageID "+" "+rowID + " "+ tb) #Store in log folder
                self._writeToJSONFile(self.workingDir,rowID+"_"+inputJson['workspaceID'],inputJson)
            else:
                WiMLogging.sm("New Json"+" "+rowID + " "+ tb)
                self._writeToJSONFile(folderPath,rowID,inputJson)
if __name__ == '__main__':
    IntegrationWrapper()


#https://test.streamstats.usgs.gov/streamstatsservices/download?workspaceID= WORKSPACEID &format=