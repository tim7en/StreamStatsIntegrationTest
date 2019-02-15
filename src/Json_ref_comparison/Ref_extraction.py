# -*- coding: utf-8 -*-
"""
Created on Wed Feb 13 15:29:29 2019

@author: tsabitov
"""

import requests
import json
import os
import numpy as np
with open(r'C:\Users\tsabitov\Desktop\json_compare\input.json') as f:
    data = json.load(f)


dictOutput = []
for j in range (0, len (data['features'])):
    url = 'https://test.streamstats.usgs.gov/streamstatsservices/parameters.json?rcode='
    rcode =  data['features'][j]['properties']['state']
    vals = data['features'][j]['properties']['testData']
    response = requests.get(url+rcode)
    X1 = (response.json()['parameters'])
    
    #Replace key 'label' by 'code' in the data (j)
    for f in range (0, len(data['features'][j]['properties']['testData'])):
        d = data['features'][j]['properties']['testData'][f]
        d['code'] = d.pop('Label')
        d['value'] = d.pop('Value')
        data['features'][j]['properties']['testData'][f] = d
    
    X2 = data['features'][j]['properties']['testData']
              
    dictlist = []      
    for i in range (0, len(X1)):
        for i2 in range (0, len (X2)):
            if X1[i]['code'] == X2[i2]['code']:
                X2[i2]['value'] = float (X2[i2]['value']) #Important conversion, returns from the server are in decimals
                union = dict(X1[i].items() | X2[i2].items())
                dictsort = []
                for key in sorted(union): #Sort it by keys and extract each key, next append to the dictionary
                    dictsort.append({key:str(union[key])})
                dictlist.append(dictsort)
    dictOutput.append (dictlist)
    
    


#data is json file (reference file)
#dictOutput is the reference list
#inputList is the loaded list from the clean lib

dirBchar = r"D:\ClientData\Library\BasinChar"
dirFiles = os.listdir(dirBchar)

dictOut2 = []
for i in range (0, len (dirFiles)): #(2,3): 

    rcode = data['features'][i]['properties']['siteid']
    try:
        rcode = int(rcode)
    except ValueError:
        rcode = str(rcode)
    with open(dirBchar+r'/'+str(rcode)+'.json') as f:
        inputObj = json.load(f) #load file from the folder

    refObj = dictOutput[i] #load file from the dictionary
    
    inputList = []
    for k in range (0, len (inputObj)):
        x = [inputObj[k]]
        finalMap1 = []
        for i2 in range (0, len(x)):
            finalMap1 = {}
            for d in x[i2]:
                finalMap1.update(d)
            inputList.append (finalMap1)
            
            
    refList = []
    for k2 in range (0, len (refObj)):
        y = [refObj[k2]]
        
        finalMap2 = []
        for i3 in range (0, len(y)):
            finalMap2 = {}
            for d in y[i3]:
                finalMap2.update(d)
            refList.append (finalMap2)
            
    def numpy_flat(a):
        return list(np.array(a).flat)
    
    def numpy_concatenate(a):
        return list(np.concatenate(a))

    def compDList (x1,x2):
        listReturn = []
        for i in range (0, len (x1)):
            valsFlag = []
            for j in range (0, len (x2)):
                if (x1[i]['code'] == x2[j]['code']):
                    try:
                        valsFlag.append (float (x1[i]['value'])/float(x2[j]['value']))
                    except (ZeroDivisionError, KeyError):
                        valsFlag.append (float(1))
            if len(valsFlag) == 0:
                valsFlag = [float(1)]
            listReturn.append (valsFlag)
        return ((numpy_concatenate(listReturn))) #flattening list https://stackoverflow.com/questions/952914/how-to-make-a-flat-list-out-of-list-of-lists
    
                    
    
    def getDif (inputList,valsFlag):
        low = np.array (valsFlag) <0.99
        #print (low)
        up = np.array (valsFlag) >1.01
        #print (up)
        dif = []
        if len(np.where (low)[0])>0 or len(np.where(up)[0]>0):
            for i in (np.where (low)):
                if len(i)>0:
                    for inL in i:
                        dif.append (inputList[inL])

            for j in np.where (up):
                if len(j)>0:
                    for inL2 in j:
                        dif.append (inputList[inL2])
        return (dif)
    dictOut2.append(getDif (inputList, compDList(inputList, refList)))

with open('your_file.txt', 'w') as f:
    for item in dictOut2:
        f.write("%s\n" % item)
        
#result is a list with dictionaries of values that are different from the reference by at least 1%
