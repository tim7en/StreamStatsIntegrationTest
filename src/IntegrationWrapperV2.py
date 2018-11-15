#Integration testing for watershed deliniation

#region "Imports"
import traceback
import datetime
import time
import os
import argparse
import fnmatch
import json
import threading
from WIMLib import WiMLogging
from WIMLib import Shared
from WIMLib.Config import Config
from ServiceAgents.StreamStatsServiceAgent import StreamStatsServiceAgent
from threading import Thread
import time
import random
from Queue import Queue

#Set globals
simulThreads = 4           #Initial number of thread calls (N+1) because of 0
queue_list = Queue()
fSummary = open ('Summary.txt', 'w+') #Create summary file if it does not exist
fSummary.write ( 'Starting Summary'+ '\n')
fSummary.close ()
'''bdelMissing = []
bcharMissing = []
bdelNoteq = []
bcharNoteq = []
bcharNew = []
bdelNew = []
bdelHUCID = []
bcharHUCID = []
xy = []
bcharXY = []
bdelXY = []
bcharRegion = []
bdelRegion = []
bcharDif = []
bdelDif = []'''

config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
workingDir = Shared.GetWorkspaceDirectory(config["workingdirectory"])

parser = argparse.ArgumentParser()
parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str, #Use the following LAT/LON pour point
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

row_count = sum(1 for row in file) #Total number of sites
maxThreads = row_count #Equal to the number of sites


def run(i, q):
    rcode,x,y,refdir,id,workspace = q.get() #Target function for threads to run
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
                if response != None and len(response)>0:
                    responseBChar = sa.getBChar(rcode,response['workspaceID'])
                    resultBChar = responseBChar['parameters'] #List of dictionaries
                    try:
                        resultBDel = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
                        HUCID = response ['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
                        xy = [x,y]
                    except:
                        resultBDel = None
                        resultBChar = None
                
                else:
                    k = 0
                    while k < 4: #Try to call 4 times!
                        print 'Attempting Basin Del Call: ' + str(siteIdentifier)
                        time.sleep (1) #Wait seconds before next attempt
                        try :
                            response = sa.getBasin(rcode,x,y,4326) #Get feature collection
                            if response == None:
                                k = k+1
                            elif len(response) == 0:
                                k = k+1
                            else:
                                responseBChar = sa.getBChar(rcode,response['workspaceID'])
                                resultBChar = responseBChar['parameters'] #List of dictionaries
                                try:
                                    resultBDel = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
                                    HUCID = response ['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
                                    xy = [x,y]
                                    k = 4
                                except:
                                    k = k+1
                                    resultBDel = None
                                    resultBChar = None
                                    pass
                        except:
                            resultBDel = None
                            resultBChar = None
                            k = k+1
            except:
                pass
        
        def findStr (x,y):
            resultBChar = x
            ind = [] #Look for string (value) inside of the parameters. We should have 20 of them returned from the server.
            c = 0
            for i in range (0,len (resultBChar)):
                tempind = str(resultBChar).find(str(y),c)
                if tempind == -1:
                    break
                c = tempind+1
                ind.append (tempind)
            return (ind)
        
        if resultBChar !=None:
            ind = findStr (resultBChar, 'value')
            l = len (resultBChar)
        else:
            ind = 0
            l = 1
        
        if ind < l:
            k = 0
            while k < 4:
                try:
                    time.sleep (1) #Wait seconds before next attempt
                    print 'No value, Repeating Calls for BDel and BChar', siteIdentifier
                    response = sa.getBasin(rcode,x,y,4326) #Get feature collection
                    if len(response) == 0:
                        k=k+1
                        pass
                    elif (response == None):
                        k= k +1
                        pass
                    else:
                        print 'No val', (len(response))
                        responseBChar = sa.getBChar(rcode, response['workspaceID'])
                        resultBChar = responseBChar['parameters'] #List of dictionaries
                        resultBDel = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
                        HUCID = response['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
                        xy = [x,y]
                        ind = findStr (resultBChar, 'value')
                        if (len(ind)<len(resultBChar)):
                            k = k+1
                        else:
                            k = 4
                except:
                    resultBDel = None
                    resultBChar = None
                    k=k+1

            if (len(ind)<len(resultBChar)):
                resultBDel = None
                resultBChar = None

        if resultBDel == None:
            fSummary = open('Summary.txt', 'a') 
            #global bdelMissing
            #bdelMissing.append(siteIdentifier)
            fSummary.write (str(siteIdentifer)+ ':' + ' Missing Return for BDel'+ '\n')
            fSummary.close ()
            print "Finished: ", siteIdentifier
            raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))
        else:
            compare(resultBDel, path.get("bdel"),siteIdentifier,workingDir, HUCID, xy, rcode)
        if resultBChar == None:
            fSummary = open('Summary.txt', 'a') 
            #global bcharMissing
            #bcharMissing.append(siteIdentifier)
            fSummary.write (str(siteIdentifer)+ ':' + ' Missing Return for BChar'+ '\n')
            fSummary.close ()
            print "Finished: ", siteIdentifier
            raise Exception ("{0} Failed to return from service Bchar".format(siteIdentifier))
        else:
            compare(resultBChar, path.get("bchar"),siteIdentifier,workingDir, HUCID, xy, rcode)
        print "Finished: ", siteIdentifier
    
    except:
        tb = traceback.format_exc()
        WiMLogging.sm("Error w/ station "+ tb)

def writeToJSONFile(path, fileName, data):  #Define function to write as json object
#https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
    try:
        filePathNameWExt = os.path.join(path,fileName+".json")
        with open(filePathNameWExt, "w+") as fp:
            json.dump(data, fp)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error writing json output "+tb)

def compare(inputObj,path,ID, workingDir, HUCID, xy, rcode):
    try:  
        refObj = None
        refFile = os.path.join(path, ID+".json") #Get the reference json file from existing root folder
        inputObj.sort()

        if (type(inputObj[0])!=list): #Condition: If the inner object of the list is not a list then dictionary
            i = 0
            dictlist = [[] for _ in range (len(inputObj))] #Initialize list of lists

            while i < len(inputObj):
                dic = (inputObj[i]) #Extract dictionary object i
                for key in sorted(dic): #Sort it by keys and extract each key, next, append to the dictionary
                    dictlist[i].append({key:str(dic[key])})
                i += 1
            inputObj = dictlist #Return sorted list of lists instead of list of dictionaries for basin characteristics
    
        if os.path.isfile(refFile):
            with open (refFile) as f:
                refObj = json.load(f)
            if inputObj!= refObj:
                if (type(inputObj[0])!=list):
                    dif = []
                    for i in range (0, len(inputObj)):
                        list_1 = inputObj[i]
                        list_2 = refObj[i]
                        pairs = zip(list_1, list_2)
                        noteq = ([(x, y) for x, y in pairs if x != y])
                        dif.append (noteq)
                else:
                    dif = []
                    dif.append ([j for j in inputObj if not j in refObj])

                if (path.find('Char')>0):
                    #global bcharNoteq
                    #global bcharHUCID
                    #global bcharXY
                    #global bcharRegion
                    #global bcharDif
                    #bcharNoteq.append (ID)
                    #bcharHUCID.append (HUCID)
                    #bcharXY.append (xy)
                    #bcharRegion.append (rcode)
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + 'BChar not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +'Difference between NewCall and Ref'+ '\n')
                    fSummary.close ()
                    #f.write (str(ID)+ ':' + str(HUCID) +' Missing Return for BDel')
                    #bcharDif.append(first_set.symmetric_difference(secnd_set))
                else:
                    #global bdelNoteq
                    #global bdelHUCID
                    #global bdelXY
                    #global bdelRegion
                    #global bdelDif
                    #bdelNoteq.append (ID)            
                    #bdelHUCID.append (HUCID)
                    #bdelXY.append (xy)
                    #bdelRegion.append (rcode)
                    #bdelDif.append(first_set.symmetric_difference(secnd_set))
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + 'BDel not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +'Difference between Newcall and Ref'+ '\n')
                    fSummary.close ()
                WiMLogging.sm("Not equal Json's"+" "+ID)
                writeToJSONFile(workingDir,ID+"_"+str(path.rsplit('/', 1)[-1]),inputObj) #Store in log folder
            else:
                tb = traceback.format_exc()
                WiMLogging.sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
        else:
            #file not in reference folder, Create it
            if (path.find('Char')>0):
                fSummary = open('Summary.txt', 'a') 
                #global f
                #global bcharNew
                #bcharNew.append (ID)
                fSummary.write (str(ID)+ ':' + 'BChar New'+ '\n')
                fSummary.close ()
            else:
                fSummary = open('Summary.txt', 'a') 
                #global f
                #global bdelNew
                #bdelNew.append (ID)    
                fSummary.write (str(ID)+ ':' + 'BDel New'+ '\n')
                fSummary.close()
            WiMLogging.sm("File not in reference folder"+" "+refFile)
            writeToJSONFile(path, ID,inputObj)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error Comparing "+tb)
        writeToJSONFile(workingDir, ID+"_viaError",{'error':tb})

for i in range(maxThreads): #Run threads as daemon, so they close when finish
    worker = Thread(target=run, args=(i, queue_list,))
    worker.setDaemon(True)
    time.sleep(0.1)
    worker.start()


threadsINI = threading.active_count() 
f1 = 0
for row in file: #Query to invoke threads !
    queue_list.put((row[rcode],row[x],row[y],refDir,row[uniqueID], workingDir))
    print 'Calling Input: ', row[uniqueID]
    WiMLogging.sm('***Calling Input: '+ str(f1))
    if f1 == simulThreads:
        while f1 == simulThreads and threading.active_count() == threadsINI: #Listener
            pass            #Inifite loop waiting for thread to be done with work
            #time.sleep (0.1) #There is a chance to run into error if two threads finished simultaniously within 0.1 or higher interval
        if threading.active_count()<threadsINI:
            print ('Initialized') #Each initialized statement should follow Finished one
            f1 = simulThreads - (threadsINI-threading.active_count ())+1
            threadsINI = threading.active_count ()
    else:
        f1=f1+1

queue_list.join() #Close mainthread after child threads done working

"""WiMLogging.sm('********************************************************')
WiMLogging.sm('***Testing Summary***') #Grab global variables and write them into log file
WiMLogging.sm('Threads Invoked: '+ str(maxThreads))
#WiMLogging.sm('User select wait time for server before next thread initialized: '+ str(TS))
WiMLogging.sm('********************************************************')
WiMLogging.sm('Basin Deliniation (Calls/Returned): '+ str(row_count) +'/'+str(row_count-len(bdelMissing)))
WiMLogging.sm('Basin Characteristics (Calls/Returned): '+ str(row_count) + '/'+ str(row_count-len(bcharMissing)))
WiMLogging.sm('BDel Sites Missing: '+ str(bdelMissing))
WiMLogging.sm('BChar Sites Missing: '+ str(bcharMissing))
WiMLogging.sm('Added to the refrenece BDel: '+ str(bdelNew))
WiMLogging.sm('Added to the reference BChar: '+ str(bcharNew))
WiMLogging.sm('I\O difference BDel of calls and lib found: '+ str(bdelNoteq))
WiMLogging.sm('I\O difference BChar of calls and lib found: '+ str(bcharNoteq))
WiMLogging.sm('BDel x,y coordinates:'+ str(bdelXY))
WiMLogging.sm('BChar x,y coordinates:'+ str(bcharXY))
WiMLogging.sm('BDel Regions:'+ str(bdelRegion))
WiMLogging.sm('BChar Regions:'+ str(bcharRegion))
WiMLogging.sm('BDel HUCID:'+ str(bdelHUCID))
WiMLogging.sm('BChar HUCID:'+ str(bcharHUCID)) """
#WiMLogging.sm('BDel list dif:'+str(bdelDif))
#WiMLogging.sm('BChar list dif:'+str(bcharDif))
#WiMLogging.sm('********************************************************')

print '*** Done'