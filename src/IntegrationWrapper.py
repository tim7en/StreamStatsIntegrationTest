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
from ServiceAgents.WIMServiceAgent import WIMServiceAgent

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

            existingFiles=self._listFiles(os.path.normpath (self.config["referenceFolder"])) #Store list of existing files

            parser = argparse.ArgumentParser()
            #Use the following LAT/LON pour point
            parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str,
                                default = 'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
            parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                                default = '4326'),
            parser.add_argument("-bcharacteristics", help="Comma separated list of region parameters to compute.", type=str,
                                default = None),
            parser.add_argument("-flowstatistics", help="Comma separated list of region flowstatistics to compute.", type=str,
                                default = "BNKF")

            args = parser.parse_args()
            if not os.path.isfile(args.file): raise Exception("File does not exist")
            file = Shared.readCSVFile(args.file)
            headers = file[0]
            id = headers.index("State") if "State" in headers else 0
            x = headers.index("dec_long") if "dec_long" in headers else 1
            y = headers.index("dec_lat") if "dec_lat" in headers else 2
            uniqueID = headers.index("GageID") if "GageID" in headers else 3

            file.pop(0)#removes the header
            startTime = time.time()
            WiMLogging.init(os.path.join(self.workingDir,"Temp"),"Integration.log")
            WiMLogging.sm("Starting routine")

            file=sorted(file, key = lambda x: int(x[3]))    #Sort by 3rd element, siteID 3rd column
            i=0     #Loop, site-id should be sorted, otherwise overwrite will happen
            lastUnique = None
            for row in file:
                if (row[uniqueID]!=lastUnique):
                    i=1
                rowID=str(row[uniqueID])+str("_test_")+str(i) #Create unique rowID for each discrete point location
                i=i+1
                self._run(row[id],row[x],row[y],rowID,existingFiles)
                lastUnique=row[uniqueID]

            WiMLogging.sm('Finished.  Total time elapsed:', str(round((time.time()- startTime)/60, 2)), 'minutes')

        except:
             tb = traceback.format_exc()
             WiMLogging.sm("Error executing delineation wrapper "+tb)

#Main function involving streamstats library
    def _run(self,stationid, x,y, rowID, existingFiles):

        new_json = None

        try:
            with StreamStatsServiceAgent() as sa:
                folderDir = (os.path.normpath (self.config["referenceFolder"])+"\\")
                folderDir = folderDir.replace(os.sep, '/')
                print rowID
                refJson = folderDir+rowID+".json"
                new_json = sa.getBasin(stationid,x,y,4326)
                if (rowID+".json" in existingFiles):
                    with open (refJson) as f:
                        existing_json = json.load(f)
                        if (new_json['featurecollection']!=existing_json['featurecollection']):
                            tb = traceback.format_exc()
                            WiMLogging.sm("Not equal Json's"+" "+rowID+" "+ tb)
                            self._writeToJSONFile(self.workingDir,rowID,new_json)
                        else:
                            tb = traceback.format_exc()
                            WiMLogging.sm("Equal Json's"+" "+rowID+" "+ tb)
                else:
                    tb = traceback.format_exc()
                    WiMLogging.sm("New Json"+" "+rowID + " "+ tb)
                    self._writeToJSONFile(folderDir,rowID,new_json) #Store in reference folder
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error w/ station "+ stationid +" "+ tb)

    def _listFiles(self,folder): #Function to list files in the directory
        try:
            for root, dirs, files in os.walk(folder, topdown=False):
                existingFiles=files
            return existingFiles
        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Something wrong with folder path "+ stationid +" "+ tb)

    def _writeToJSONFile(self,path, fileName, data):  #Define function to write as json object
    #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
        filePathNameWExt = path + '/' + fileName + '.json'
        try:
            with open(filePathNameWExt, 'w') as fp:
                json.dump(data, fp)
        except:
            tb=traceback.format_exc()
            WiMLogging.sm("Error writing json output "+tb)

if __name__ == '__main__':
    IntegrationWrapper()
