#Integration testing for watershed deliniation

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
import threading
import time
import random
from Queue import Queue


#Initial number of thread calls will be (N+1)
simulThreads = 3           
queue_list = Queue()


#Create Summary.txt in the root folder of streamstats (Need to change directory)
fSummary = open ('Summary.txt', 'w+')
fSummary.write ( 'Starting Summary'+ '\n')
fSummary.close ()


#Open config file and define workspace
config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
workingDir = Shared.GetWorkspaceDirectory(config["workingdirectory"])


#Used for command line
parser = argparse.ArgumentParser()
parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str, #Use the following LAT/LON pour point
                    default = 'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                    default = '4326')
args = parser.parse_args()


#Check if file (Input.csv) is in csv
if not os.path.isfile(args.file): raise Exception("File does not exist")
refDir = {"bdel":config["referenceFolderBasinDel"],"bchar":config["referenceFolderBasinChar"]}
file = Shared.readCSVFile(args.file)
headers = file[0]
rcode = headers.index("State") if "State" in headers else 0
x = headers.index("dec_long") if "dec_long" in headers else 1
y = headers.index("dec_lat") if "dec_lat" in headers else 2
uniqueID = headers.index("GageID") if "GageID" in headers else 3
file.pop(0)


#Start logging txt
startTime = time.time()
WiMLogging.init(os.path.join(workingDir,"Temp"),"Integration.log")


#Count total number of rows, and initiate equal number of threads
row_count = sum(1 for row in file) #Total number of sites
maxThreads = row_count #Equal to the number of sites


#Main function that run by thread
def run(i, q):
    rcode,x,y,refdir,iden,workspace = q.get() #Target function for threads to run
    try:
        run_func(rcode,x,y,refdir,iden,workspace)
    except:
        tb = traceback.format_exc()
        WiMLogging.sm("Error w/ run "+ tb)
    finally:
        q.task_done()


#Background function run for each site
def run_func(rcode, x,y, path, siteIdentifier, workingDir):
    response = None
    resultBChar = None
    resultBDel = None

    def findStr (x,y):
        resultBChar = x
        ind = [] #Look for string (value) inside of the parameters. We should have 20 of them returned from the server.
        c = 0
        for i in range (0,len (resultBChar)):
            tempind = str(resultBChar).find(str(y),c)
            if tempind == -1: #When there none left, break the loop
                break
            c = tempind+1
            ind.append (tempind)
        return (ind)

    with StreamStatsServiceAgent() as sa: 
        try:
            response = sa.getBasin(rcode,x,y,4326) #Get feature collection
            k = 0
            f1 = 0 #Flag 1  
            f2 = 0 #Flag 2 
            f3 = 0 #Flag 3


            #Catch if basin del returned error and flag it
            try:
                len(response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0])
            except:
                f1 = 1


            #Check the flag, if flag is active, attempt to get basin del one more time
            while ((response == None or f1 == 1) and f2 < 4):
                print 'New Attempt, while loop 1', siteIdentifier
                response = sa.getBasin(rcode,x,y,4326) #Get feature collection
                try:
                    len(response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0])
                    f1 = 0
                except:
                    f2 = f2+1

            
            if (response !=None and f1 == 0):
                while (f3 == 0 and (response!= None or len(response)>0)):
                    try:
                        print 'While loop 2, Outer', siteIdentifier
                        responseBChar = sa.getBChar(rcode,response['workspaceID'])
                        resultBChar = responseBChar['parameters'] #List of dictionaries
                        resultBDel = response['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
                        HUCID = response ['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
                        xy = [x,y]
                        ind = findStr(resultBChar,'value') 
                        if (len(ind)<len(resultBChar)): #Call basin characteristics if there any missing, pass parameter code
                            k=k+1
                        elif k == 4:
                            f3=1
                        else:
                            f3=1
                        if (k>0):
                            response = sa.getBasin(rcode,x,y,4326) #Recursive call  (A(5/5)|B(22/24) WID - 1 call again (WID =2) ; A(5/5)|B(24/24) WID -2)
                            print 'New Attempt, While loop 2, Inner', siteIdentifier
                    except:
                        resultBDel = None
                        resultBChar = None
                        f3 = 1
                if (resultBDel == None):
                    print "Finished: ", siteIdentifier
                    raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))
            else:
                print "Finished: ", siteIdentifier
                raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))   
        except:
            resultBChar = None
            resultBDel = None

        if resultBDel == None:
            fSummary = open('Summary.txt', 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BDel'+ '\n')
            fSummary.close ()
            print "Finished: ", siteIdentifier
            raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))
        else:
            compare(resultBDel, path.get("bdel"),siteIdentifier,workingDir, HUCID, xy, rcode)


        if resultBChar == None:
            fSummary = open('Summary.txt', 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BChar'+ '\n')
            fSummary.close ()
            print "Finished: ", siteIdentifier
            raise Exception ("{0} Failed to return from service Bchar".format(siteIdentifier))
        else:
            compare(resultBChar, path.get("bchar"),siteIdentifier,workingDir, HUCID, xy, rcode)

    print "Finished: ", siteIdentifier



def writeToJSONFile(path, fileName, data):  #Define function to write as json object #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
    try:
        filePathNameWExt = os.path.join(path,fileName+".json")
        with open(filePathNameWExt, "w+") as fp:
            json.dump(data, fp)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error writing json output "+tb)


def compare(inputObj,path,ID, workingDir, HUCID, xy, rcode): #Compare json txt files
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
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + ' BChar not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between NewCall and Ref'+ '\n')
                    fSummary.close ()
                else:
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + ' BDel not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between Newcall and Ref'+ '\n')
                    fSummary.close ()
                WiMLogging.sm("Not equal Json's"+" "+ID)
                writeToJSONFile(workingDir,ID+"_"+str(path.rsplit('/', 1)[-1]),inputObj) #Store in log folder
            else:
                if (path.find('Char')>0):
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + 'BChar Equal Jsons' + '\n')
                    fSummary.close ()
                else:
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + 'BDel Equal Jsons' + '\n')
                    fSummary.close ()
                tb = traceback.format_exc()
                WiMLogging.sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
        else:
            if (path.find('Char')>0):#file not in reference folder, Create it
                fSummary = open('Summary.txt', 'a') 
                fSummary.write (str(ID)+ ':' + 'BChar New'+ '\n')
                fSummary.close ()
            else:
                fSummary = open('Summary.txt', 'a') 
                fSummary.write (str(ID)+ ':' + ' BDel New'+ '\n')
                fSummary.close()
            WiMLogging.sm("File not in reference folder"+" "+refFile)
            writeToJSONFile(path, ID,inputObj)
    except:
        tb=traceback.format_exc()
        WiMLogging.sm("Error Comparing "+tb)
        writeToJSONFile(workingDir, ID+"_viaError",{'error':tb})

'''def getBasin2 (x,y,region,ID, f=0): #Recursive calling 
    try:
        if (f>4):
            return None
        else:
            #Do smth
    except:
        getBasin2 (x,y,region,ID, f+=1)'''


#Main thread
for i in range(maxThreads): #Run threads as daemon, so they close when finish
    worker = Thread(target=run, args=(i, queue_list,))
    worker.setDaemon(True)
    time.sleep(0.1)
    worker.start()



#Global var counting number of active threads
threadsINI = threading.active_count() 
f = 0
for row in file: #Query to invoke threads !
    print 'Calling Input: ', row[uniqueID]
    queue_list.put((row[rcode],row[x],row[y],refDir,row[uniqueID], workingDir))
    WiMLogging.sm('***Calling Input: '+ str(f))
    if f == simulThreads:
        while f == simulThreads and threading.active_count() == threadsINI: #Listener
            pass            #Inifite loop waiting for thread to be done with work
            time.sleep (1) #There is a chance to run into error if two threads finished simultaniously within 0.1 or higher interval
        if threading.active_count()<threadsINI:
            print ('Initialized') #Each initialized statement should follow Finished one
            f = simulThreads - (threadsINI-threading.active_count ())+1
            threadsINI = threading.active_count ()
    else:
        f=f+1

queue_list.join() #Close mainthread after child threads done working

print '*** Done'