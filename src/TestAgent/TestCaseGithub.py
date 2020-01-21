#region "Imports"
import traceback
import json
from WIMLib.Config import Config
import os
import requests
import time
from  WIMLib.WiMLogging import WiMLogging
from WIMLib import Shared

#Open config file and define workspace
config = json.load(open(os.path.join(os.path.dirname(__file__), 'config.json')))
Config (config)

class testCase(object):
        #region Constructor
    def __init__(self):
        try:
            self.gitUrl = Config()["TestCase"]["gitUrl"];
        except:
            self.gitUrl = None

        #self._sm("initialized StreamStatsServiceAgent")
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.gitUrl=None
    
    def LoadJson (self, url):
        try:
            if self.gitUrl == None:
                self.gitUrl = "https://raw.githubusercontent.com/USGS-WiM/StreamStats-Setup/master/batchTester/testSites.geojson"
            else:
                self.gitUrl = url
            try:
                response = requests.get(self.gitUrl)
                return [response.json(), response.headers]
            except:
                self._sm("Error: file " + os.path.basename(self.gitUrl) + "doesn't exist", "ERROR")
                return ''
        except:
            tb = traceback.format_exc()
            self._sm("StreamstatsService getBChar Error "+tb, "ERROR")
            return json
            
    def FindString (self, name, userdict):
        for i in userdict:
            if (i.get("Label") == name):
                return (i.get("Value"))
            else:
                None

    def _sm(self,msg,type="INFO", errorID=0):        
        WiMLogging().sm(msg,type="INFO", errorID=0)
        
        
        

response =  (testCase().LoadJson(None))
result = response[0]["features"]
bcharpath = config["referenceFolderBasinChar"]


mismatch = []
workingDir = Shared.GetWorkspaceDirectory (config["workingdirectory"]) #initialize and create logging folder w file
sumPath = os.path.join (workingDir, 'TestCase.txt')
fSummary = open (sumPath, 'w+')
for i in result:
    try:
        siteid = int(i.get("properties").get("siteid"))  
        jsonpath = (bcharpath + "/" + str(siteid) + ".json")    
        bcharvalues = i.get("properties").get("testData")
    

        with open(jsonpath, 'r') as f:
            bchar = json.load(f)    
    
        
        for item in bchar:
            for j in item:
                if list (j.keys())[0] == "code":
                    varname = list(j.values())[0]
                    myval = testCase().FindString ( varname, bcharvalues)                   
                elif list (j.keys())[0] == "value":
                    if (myval is None):
                        None
                    else:                       
                        if (float(myval) == float(list(j.values())[0])):
                            None
                        else:
                            mismatch.append(
                                    {"Reference": float(myval)},
                                    {"Server": float (list(j.values())[0])}
                                    )
                else:
                    None
                    
        fSummary.write ("Results for site: " + str(siteid) + '\n')
        fSummary.write (str(mismatch) + '\n')

    except:
        None
fSummary.close ()

