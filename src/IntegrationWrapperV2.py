#Integration testing for watershed deliniation
import traceback
import datetime
import time
import os
import argparse
import fnmatch
import json
import threading
from WIMLib.WiMLogging import WiMLogging
from WIMLib import Shared
from WIMLib.Config import Config
from ServiceAgents.StreamStatsServiceAgent import StreamStatsServiceAgent
from threading import Thread
import random
import queue


#Initial number of thread calls will be (N+1)
simulThreads = 0
queue_list = queue.Queue()


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
                    default = r'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
parser.add_argument("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
                    default = '4326')
args = parser.parse_args()


#Check if file (Input.csv) is in csv
if not os.path.isfile(args.file): raise Exception ("File does not exist")
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
WiMLogging (os.path.join(workingDir,"Temp"),"Integration.log")


#Count total number of rows, and initiate equal number of threads
row_count = sum(1 for row in file) #Total number of sites
maxThreads = row_count #Equal to the number of sites

#timing decorator for functions
def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print('{:s} function took {:.3f} s'.format(f.__name__, (time2-time1)))
        return ret
    return wrap
  
def findStr (x,y):
    resultBChar = x
    ind = [] #Look for string (value) inside of the parameters. We should have 20 of them returned from the server.
    for i in range (0,len (resultBChar)):
        tempind = str(resultBChar[i]).find(str(y))
        ind.append (tempind)
    return (sum(i > 0 for i in ind))

#Main function that run by a thread
def run(i, q):
    rcode,x,y,refdir,iden,workspace = q.get() #Target function for threads to run to get sites
    try:
        run_func(rcode,x,y,refdir,iden,workspace) #main function for threads to run to get data from server
    except:
        tb = traceback.format_exc()
        WiMLogging().sm ("Error w/ run "+ tb)
    finally:
        q.task_done()


#Background function run for each site
def run_func(rcode, x,y, path, siteIdentifier, workingDir):
    response = None
    resultBChar = None
    resultBDel = None


    with StreamStatsServiceAgent() as sa: 
        try:
            #Recursive call with 1 second interval for basin deliniation, limit to 5 calls
            @timing
            def rcBDel (f = 0):
                try:
                    if (f >4):
                        return None
                    else:
                        response = sa.getBasin(rcode,x,y,4326)
                        #attempting to create a new, fake var. If successful, continue, else it will rise an error.
                        c = len(response[0]['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0])
                        return (response) #Get feature collection
                except:
                    print ('Attempting another try for bDel')
                    f +=1
                    time.sleep (1)
                    rcBDel (f)

            #Recursive call for basin characteristics, limit to 5 calls for basin characteristics and 5 calls for basin deliniation
            @timing
            def rcBChar ( rcode, response, j = 0):
                try:
                    if (j>4):
                        return None
                    else:
                        responseBChar = sa.getBChar (rcode,response['workspaceID']) #get basin characteristics from the server
                        X1 = findStr(list(responseBChar[0].values())[0], 'value')
                        X2 = len (list(responseBChar[0].values())[0])
                        if ( X1!=X2 ) : #if the number of returned values is not equal to the number of variables, rise an error
                            raise ValueError
                        return (responseBChar)
                except:
                    response = rcBDel (3)
                    j+=1
                    rcBChar (rcode, response[0], j)
                    

            #Get response of rcBDel
            response = rcBDel ()
            resultBDel = response[0]['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
            HUCID = response[0] ['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
            xy = [x,y]
            bdelServer = response[1]['Server']
            resultBChar = rcBChar (rcode, response[0], 0)
            bcharServer = resultBChar[1]['Server']

        except:
            if (response == None):
                resultBDel = None
                resultBChar = None

        if resultBDel == None:
            fSummary = open('Summary.txt', 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BDel'+ '\n')
            fSummary.close ()
            print ("Finished: ", siteIdentifier)
            raise Exception("{0} Failed to return from service BDel".format(siteIdentifier))
        else:
            compare(resultBDel, path.get("bdel"),siteIdentifier,workingDir, HUCID, xy, rcode,bdelServer )


        if resultBChar == None:
            fSummary = open('Summary.txt', 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BChar'+ '\n')
            fSummary.close ()
            print ("Finished with error: ", siteIdentifier)
            raise Exception ("{0} Failed to return from service Bchar".format(siteIdentifier))
        else:
            compare(resultBChar, path.get("bchar"),siteIdentifier,workingDir, HUCID, xy, rcode, bcharServer)

    print ("Finished: ", siteIdentifier)



def writeToJSONFile(path, fileName, data):  #Define function to write as json object #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
    try:
        filePathNameWExt = os.path.join(path,fileName+".json")
        with open(filePathNameWExt, "w+") as fp:
            json.dump(data, fp)
    except:
        tb=traceback.format_exc()
        WiMLogging().sm("Error writing json output "+tb)


def compare(inputObj,path,ID, workingDir, HUCID, xy, rcode, servName): #Compare json txt files
    try:  
        refObj = None
        refFile = os.path.join(path, ID+".json") #Get the reference json file from existing root folder
        
        if (type(inputObj[0])==dict): #Condition: If the inner object of the list is not a list then dictionary
            inputPars = inputObj[0]['parameters']
            i = 0
            dictlist = [[] for _ in range (len(inputPars))] #Initialize list of lists
            while i < len(inputPars):
                dic = (inputPars[i]) #Extract dictionary object i
                for key in sorted(dic): #Sort it by keys and extract each key, next, append to the dictionary
                    dictlist[i].append({key:str(dic[key])})
                i += 1
            inputObj = dictlist #Return sorted list of lists instead of list of dictionaries for basin characteristics
    
        if os.path.isfile(refFile):
            with open (refFile) as f:
                refObj = json.load(f)
            if inputObj!= refObj:
                dif = []
                dif.append ([j for j in inputObj if not j in refObj])

                if (path.find('Char')>0):
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + ' BChar not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(servName) +' Server '+ '\n') 
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between NewCall and Ref'+ '\n')
                    fSummary.close ()
                else:
                    fSummary = open('Summary.txt', 'a') 
                    fSummary.write (str(ID)+ ':' + ' BDel not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(servName) +' Server '+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between Newcall and Ref'+ '\n')
                    fSummary.close ()
                WiMLogging().sm("Not equal Json's"+" "+ID)
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
                WiMLogging().sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
        else:
            if (path.find('Char')>0):#file not in reference folder, Create it
                fSummary = open('Summary.txt', 'a') 
                fSummary.write (str(ID)+ ':' + 'BChar New'+ '\n')
                fSummary.close ()
            else:
                fSummary = open('Summary.txt', 'a') 
                fSummary.write (str(ID)+ ':' + ' BDel New'+ '\n')
                fSummary.close()
            WiMLogging().sm("File not in reference folder"+" "+refFile)
            writeToJSONFile(path, ID,inputObj)
    except:
        tb=traceback.format_exc()
        WiMLogging().sm("Error Comparing "+tb)
        writeToJSONFile(workingDir, ID+"_viaError",{'error':tb})

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
    queue_list.put((row[rcode],row[x],row[y],refDir,row[uniqueID], workingDir))
    WiMLogging().sm('***Calling Input: '+ row[uniqueID])

    if f == simulThreads:
        while f == simulThreads and threading.active_count() == threadsINI: #Listener
            pass            #Inifite loop waiting for thread to be done with work
            time.sleep (0.5) #There is a chance to run into error if two threads finished simultaniously within 0.1 or higher interval
        if threading.active_count()<threadsINI:
            print ('Initialized') #Each initialized statement should follow Finished one
            f = simulThreads - (threadsINI-threading.active_count ())+1
            threadsINI = threading.active_count ()
    else:
        f=f+1

queue_list.join() #Close mainthread after child threads done working

print ('*** Done')