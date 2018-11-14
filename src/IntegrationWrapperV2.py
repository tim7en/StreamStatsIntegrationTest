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

#Set globals
simulThreads = 5.0           #Timout interval. Timout threads every few intervals, to reduce load on server.
TS = 100
queue_list = Queue()
bdelMissing = [] #Global, missing basin delination calls
bcharMissing = []
bdelNoteq = []
bcharNoteq = []
config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
workingDir = Shared.GetWorkspaceDirectory(config["workingdirectory"])
parser = argparse.ArgumentParser()
#Use the following LAT/LON pour point
parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str,
                    default = 'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                    default = '4326')
args = parser.parse_args()
if not os.path.isfile(args.file): raise Exception("File does not exist")
refDir = {"bdel":config["referenceFolderBasinDel"],"bchar":config["referenceFolderBasinChar"]}
file = Shared.readCSVFile(args.file)
headers = file[0]
rcode = headers.index("State") if "State" in headers else 0
x = headers.index("dec_long") if "dec_long" in headers else 1
y = headers.index("dec_lat") if "dec_lat" in headers else 2
uniqueID = headers.index("GageID") if "GageID" in headers else 3
file.pop(0)#removes the header
startTime = time.time()
WiMLogging.init(os.path.join(workingDir,"Temp"),"Integration.log")


row_count = sum(1 for row in file)
maxThreads = row_count
wrapper = int(row_count / maxThreads) + (row_count % maxThreads > 0)
history_list = []

def run(i, q):
    rcode,x,y,refdir,id,workspace = q.get()
    try:
        run_func(rcode,x,y,refdir,id,workspace)
    except:
        tb = traceback.format_exc()
        WiMLogging.sm("Error w/ run "+ tb)
    finally:
        q.task_done()

def run_func(rcode, x,y, path,siteIdentifier,workingDir):   
    try:
        resultBChar = None
        resultBDel = None

        with StreamStatsServiceAgent() as sa: 
            try:
                response = sa.getBasin(rcode,x,y,4326) #Get feature collection
                responseBChar = sa.getBChar(rcode,response['workspaceID'])
                resultBChar = responseBChar['parameters'] #List of dictionaries
                resultBDel = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
            except:
                pass                

        if resultBDel == None:
            global bdelMissing
            bdelMissing.append(siteIdentifier)
            print "Finished: ", siteIdentifier
            raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))
        if resultBChar == None:
            global bcharMissing
            bcharMissing.append(siteIdentifier)
            print "Finished: ", siteIdentifier
            raise Exception ("{0} Failed to return from service Bchar".format(siteIdentifier))
        compare(resultBDel, path.get("bdel"),siteIdentifier,workingDir)
        compare(resultBChar, path.get("bchar"),siteIdentifier,workingDir)
        print "Finished: ", siteIdentifier
    
    except:
        tb = traceback.format_exc()
        WiMLogging.sm("Error w/ station "+ tb)

def writeToJSONFile(path, fileName, data):  #Define function to write as json object
#https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
    try:
        filePathNameWExt = os.path.join(path,fileName+".json")
        with open(filePathNameWExt, 'w') as fp:
            json.dump(data, fp)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error writing json output "+tb)

def compare(inputObj,path,ID, workingDir):
    try:  
        refObj = None
        refFile = os.path.join(path, ID+".json") #Get the reference json file from existing root folder
        inputObj.sort()

        if (type(inputObj[0])!=list): #Condition: If the inner object of the list is not a list then dictionary
            i = 0
            dictlist = [[] for _ in range (len(inputObj))] #Initialize list of lists

            while i < len(inputObj):
                dic = (inputObj[i]) #Extract dictionary object i
                for key in sorted(dic): #Sort it by keys and extract each key, next append to the dictionary
                    dictlist[i].append({key:str(dic[key])})
                i += 1
            inputObj = dictlist #Return sorted list of lists instead of list of dictionaries for basin characteristics
    
        if os.path.isfile(refFile):
            with open (refFile) as f:
                refObj = json.load(f)

            if inputObj!= refObj:
                if (path.find('Char')>0):
                    global bcharNoteq
                    bcharNoteq.append (ID)
                else:
                    global bdelNoteq
                    bdelNoteq.append (ID)            
                
                WiMLogging.sm("Not equal Json's"+" "+ID)
                writeToJSONFile(workingDir,ID+"_"+str(path.rsplit('/', 1)[-1]),inputObj) #Store in log folder
            else:
                tb = traceback.format_exc()
                WiMLogging.sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
        else:
            #file not in reference folder, Create it
            WiMLogging.sm("File not in reference folder"+" "+refFile)
            writeToJSONFile(path, ID,inputObj)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error Comparing "+tb)
        writeToJSONFile(workingDir, ID+"_viaError",{'error':tb})

for i in range(maxThreads):
    worker = Thread(target=run, args=(i, queue_list,))
    worker.setDaemon(True)
    time.sleep(0.1)
    worker.start()

j = 0
for row in file:
    queue_list.put((row[rcode],row[x],row[y],refDir,row[uniqueID], workingDir))
    if (float((j+1)/simulThreads).is_integer()):
        WiMLogging.sm('*** Main thread waiting: '+ str(TS)+ ' sec')
        time.sleep (TS)
    WiMLogging.sm('***Calling Input: '+ str(j))
    j = j+1
    

queue_list.join()

WiMLogging.sm('********************************************************')
WiMLogging.sm('***Testing Summary***')
WiMLogging.sm('Threads Invoked: '+ str(maxThreads))
WiMLogging.sm('User select wait time for server before next thread initialized: '+ str(TS))
WiMLogging.sm('********************************************************')
WiMLogging.sm('Basin Deliniation (Calls/Returned): '+ str(row_count) +'/'+str(row_count-len(bdelMissing)))
WiMLogging.sm('BDel Sites Missing: '+ str(bdelMissing))
WiMLogging.sm('I\O difference BDel of calls and lib found: '+ str(bdelNoteq))
WiMLogging.sm('********************************************************')
WiMLogging.sm('Basin Characteristics (Calls/Returned): '+ str(row_count) + '/'+ str(row_count-len(bcharMissing)))
WiMLogging.sm('BChar Sites Missing: '+ str(bcharMissing))
WiMLogging.sm('I\O difference BChar of calls and lib found: '+ str(bcharNoteq))
print '*** Done'