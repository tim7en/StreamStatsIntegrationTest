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


#Open config file and define workspace
config = Config (json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))
workingDir = Shared.GetWorkspaceDirectory (config["workingdirectory"]) #initialize and create logging folder w file

#Create Summary.txt in the root folder of streamstats (Need to change directory)
sumPath = os.path.join (workingDir, 'Summary.txt')
fSummary = open (sumPath, 'w+')
fSummary.write ( 'Starting Summary'+ '\n')
fSummary.write ( 'Total all runs: 0'+ '\n')
fSummary.write ( 'Total bchar runs: 0'+ '\n')
fSummary.write ( 'Total bdel runs: 0'+ '\n')
fSummary.write ( 'Total bcharNoteq runs: 0'+ '\n')
fSummary.write ( 'Total bdelNoteq runs: 0'+ '\n')
fSummary.write ( 'Total bcharfail runs: 0'+ '\n')
fSummary.write ( 'Total bdelfail runs: 0'+ '\n')
fSummary.write ( 'Total bcharNew runs: 0'+ '\n')
fSummary.write ( 'Total bdelNew runs: 0'+ '\n')
fSummary.write ( 'Total bcharrep runs: 0'+ '\n')
fSummary.close ()
WiMLogging (workingDir)

#Used for command line
parser = argparse.ArgumentParser()
parser.add_argument ("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str, #Use the following LAT/LON pour point
                    default = r'D:\ClientData\InputCoordinates.csv') #Change to the location of the csv file
parser.add_argument ("-inputEPSG_Code", help="Default WGS 84 (4326),see http://spatialreference.org/ref/epsg/ ", type=int,
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
WiMLogging().sm ("Starting routine")

#Count total number of rows, and initiate equal number of threads
row_count = sum(1 for row in file) #Total number of sites
maxThreads = row_count #Equal to the number of sites

#timing decorator for functions
def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        WiMLogging().sm ('{:s} function took {:.3f} s'.format(f.__name__, (time2-time1))) #we can specify to append to a txt file
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
def run(i, q): #j is a mutable counter for a function (currently this will track number of sites executed)
    rcode,x,y,refdir,iden,workspace = q.get() #Target function for threads to run to get sites
    try:
        run_func (rcode, x, y,
                refdir,iden,workspace) #main function for threads to run to get data from server
    except:
        tb = traceback.format_exc()
        WiMLogging().sm ("Error w/ run "+ tb)
    finally:
        counterOverwrite (sumPath, 'all')
        q.task_done()


#Background function run for each site
def run_func(rcode, x, y, 
            path, siteIdentifier, workingDir):

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
                        #counterOverwrite (sumPath, 'bdel')
                        response = sa.getBasin(rcode,x,y,4326)
                        c = len(response[0]['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0])
                        return (response) #Get feature collection
                except:
                    WiMLogging().sm('Attempting rcBDel')
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
                        #counterOverwrite (sumPath, 'bchar')
                        responseBChar = sa.getBChar (rcode,response['workspaceID']) #get basin characteristics from the server
                        return (responseBChar)
                except:
                    response = rcBDel (3)
                    j+=1
                    WiMLogging().sm('Attempting rcBChar')
                    rcBChar (rcode, response[0], j)
                    

            #Get response of rcBDel
            response = rcBDel ()
            resultBDel = response[0]['featurecollection'][1]['feature']['features'][0]['geometry']['coordinates'][0] #List of lists
            HUCID = response[0] ['featurecollection'][1]['feature']['features'][0]['properties']['HUCID']
            xy = [x,y]
            bdelServer = response[1]['usgswim-hostname']
            resultBChar = rcBChar (rcode, response[0], 0)
            bcharServer = resultBChar[1]['usgswim-hostname']

        except:
            if response == None:
                resultBDel = None
                resultBChar = None

        if resultBDel == None:
            fSummary = open(sumPath, 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BDel'+ '\n')
            counterOverwrite (sumPath, 'bdelfail')
            fSummary.close ()
            print ("Finished: ", siteIdentifier)
            WiMLogging().sm ("{0} Failed to return from service BDel".format(siteIdentifier))
        else:
            compare(resultBDel, path.get("bdel"), siteIdentifier, 
                    workingDir, HUCID, xy, rcode,bdelServer)

        
        if resultBChar == None:
            fSummary = open(sumPath, 'a') 
            fSummary.write (str(siteIdentifier)+ ':' + ' Missing Return for BChar'+ '\n')
            counterOverwrite (sumPath, 'bcharfail')
            fSummary.close ()
            print ("Finished with error: ", siteIdentifier)
            WiMLogging().sm ("{0} Failed to return from service Bchar".format(siteIdentifier))
        else:
            compare(resultBChar, path.get("bchar"), siteIdentifier,
                    workingDir, HUCID, xy, rcode, bcharServer)

    print ("Finished: ", siteIdentifier)



def writeToJSONFile (path, fileName, data):  #Define function to write as json object #https://gist.github.com/keithweaver/ae3c96086d1c439a49896094b5a59ed0
    try:
        filePathNameWExt = os.path.join(path,fileName+".json")
        with open(filePathNameWExt, "w+") as fp:
            json.dump(data, fp)
    except:
        tb=traceback.format_exc()
        WiMLogging().sm("Error writing json output "+tb)


def counterOverwrite (input_txt, param_string):
    #Create Summary.txt in the root folder of streamstats (Need to change directory)
    with open(input_txt, 'r') as myfile:
        datas = myfile.read()
    myfile.close()
    
    datList = datas
    datas = datas.split('\n')
    del datas[len(datas)-1]
    
    for line in datas:
        vList = (line.split())
        if (vList[1] == param_string):
            v = vList [3] #value in the string is 4th after splitting (Timur gives high 5) 5 is the value
            v = int(v)+1
            vList[3] = str(v)
            seperator = ' '
            newline = seperator.join(vList)
            repline = line
    
    # Write the file out again
    with open(input_txt, 'w') as myfile:
        datList = datList.replace(repline, newline)
        myfile.write(datList)
    myfile.close ()


def compare (inputObj, path, ID, workingDir, 
            HUCID, xy, rcode, servName): #Compare json txt files
    try:  
        refObj = None
        refFile = os.path.join(path, ID+".json") #Get the reference json file from existing root folder
        
        if isinstance(inputObj[0], dict): #Condition: If the inner object of the list is dictionary
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
                    fSummary = open(sumPath, 'a') 
                    fSummary.write (str(ID)+ ':' + ' BChar not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(servName) +' Server '+ '\n') 
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between NewCall and Ref'+ '\n')
                    counterOverwrite (sumPath, 'bcharNoteq')
                    X1 = refObj
                    X2 = inputObj
                    #merge two dictionaries and add missing values if any
                    dictOutput = []
                    for i in range (0, len(X1)):
                        finalMap1 = {}
                        for d in X1[i]:
                            finalMap1.update(d)
                        finalMap2 = {}
                        for d in X2[i]:
                            finalMap2.update(d)
                            
                        union = dict(finalMap1.items() | finalMap2.items()) #get the union of dictionaries
                        
                        dictlist = []
                        for key in sorted(union): #Sort it by keys and extract each key, next append to the dictionary
                            dictlist.append({key:str(union[key])})
                        dictOutput.append(dictlist)

                    if (dictOutput != refObj): #if output dictionary is not equal to the reference one, replace it
                        WiMLogging().sm ('Updated Bchar in the reference folder : ', str(ID) )
                        refObj = dictOutput
                        fSummary.write (str(ID)+ ':' +'Bchar gets replaced'+ '\n')
                        counterOverwrite (sumPath, 'bcharrep')
                    counterOverwrite (sumPath, 'bchar')
                    fSummary.close ()
                else:
                    fSummary = open(sumPath, 'a') 
                    fSummary.write (str(ID)+ ':' + ' BDel not Equal'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(HUCID) + ' HUCID'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(xy) +' xy coordinates'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(rcode) +' State'+ '\n')
                    fSummary.write (str(ID)+ ':' + str(servName) +' Server '+ '\n')
                    fSummary.write (str(ID)+ ':' + str(dif) +' Difference between Newcall and Ref'+ '\n')
                    counterOverwrite (sumPath, 'bdelNoteq')
                    counterOverwrite (sumPath, 'bdel')
                    fSummary.close ()
                WiMLogging().sm("Not equal Json's"+" "+ID)
                writeToJSONFile(workingDir,ID+"_"+str(path.rsplit('/', 1)[-1]),inputObj) #Store in log folder
            else:
                if (path.find('Char')>0):
                    fSummary = open(sumPath, 'a') 
                    fSummary.write (str(ID)+ ':' + 'BChar Equal Jsons' + '\n')
                    counterOverwrite (sumPath, 'bchar')
                    fSummary.close ()
                else:
                    fSummary = open(sumPath, 'a') 
                    fSummary.write (str(ID)+ ':' + 'BDel Equal Jsons' + '\n')
                    counterOverwrite (sumPath, 'bdel')
                    fSummary.close ()
                tb = traceback.format_exc()
                WiMLogging().sm("Equal Json's"+" "+ID+" "+ tb) #Don't create file
        else:
            if (path.find('Char')>0):#file not in reference folder, Create it
                fSummary = open(sumPath, 'a') 
                fSummary.write (str(ID)+ ':' + 'BChar New'+ '\n')
                counterOverwrite (sumPath, 'bcharNew')
                counterOverwrite (sumPath, 'bchar')
                fSummary.close ()
            else:
                fSummary = open(sumPath, 'a') 
                fSummary.write (str(ID)+ ':' + ' BDel New'+ '\n')
                counterOverwrite (sumPath, 'bdelNew')
                counterOverwrite (sumPath, 'bdel')
                fSummary.close()
            WiMLogging().sm("File not in reference folder"+" "+refFile)
            writeToJSONFile(path, ID,inputObj)
    except:
        counterOverwrite (sumPath, 'bcharfail')
        counterOverwrite (sumPath, 'bdelfail')
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
    queue_list.put((row[rcode], row[x], row[y],
                    refDir, row[uniqueID], workingDir))

    WiMLogging().sm ('***Calling Input: '+ row[uniqueID])

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
