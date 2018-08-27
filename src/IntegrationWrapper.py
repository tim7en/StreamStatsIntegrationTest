
#------------------------------------------------------------------------------
#----- DelineateWrapper.py ----------------------------------------------------
#------------------------------------------------------------------------------

#-------1---------2---------3---------4---------5---------6---------7---------8
#       01234567890123456789012345678901234567890123456789012345678901234567890
#-------+---------+---------+---------+---------+---------+---------+---------+

# copyright:   2016 WiM - USGS

#    authors:  Jeremy K. Newson USGS Web Informatics and Mapping
# 
#   purpose:  Wrapper to delineate watershed using split catchement methods
#          
#discussion:  https://github.com/GeoJSON-Net/GeoJSON.Net/blob/master/src/GeoJSON.Net/Feature/Feature.cs
#             http://pro.arcgis.com/en/pro-app/tool-reference/spatial-analyst/watershed.htm
#             geojsonToShape: http://desktop.arcgis.com/en/arcmap/10.3/analyze/arcpy-functions/asshape.htm
#       

#region "Comments"
#08.19.2015 jkn - Created
#endregion

#region "Imports"
import traceback
import datetime
import time
import os
import argparse
import fnmatch
from WIMLib import WiMLogging
from WIMLib import Shared
from WIMLib.Config import Config
from ServiceAgents.StreamStatsServiceAgent import StreamStatsServiceAgent
from ServiceAgents.WIMServiceAgent import WIMServiceAgent
import json

#endregion

##-------1---------2---------3---------4---------5---------6---------7---------8
##       Main
##-------+---------+---------+---------+---------+---------+---------+---------+
#http://stackoverflow.com/questions/13653991/passing-quotes-in-process-start-arguments
class IntegrationWrapper(object):
    #region Constructor
    def __init__(self):
        
        try:
            parser = argparse.ArgumentParser()
            #Use the following LAT/LON pour point
            parser.add_argument("-file", help="specifies csv file location including gage lat/long and comid's to estimate", type=str, 
                                default = 'C:/Users/jknewson/Downloads/PA_Bankfull.csv')
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
            id = headers.index("site_no") if "site_no" in headers else 0
            x = headers.index("dec_long") if "dec_long" in headers else 1
            y = headers.index("dec_lat") if "dec_lat" in headers else 2

            file.pop(0)#removes the header

            startTime = time.time()
            
            self.config = Config(json.load(open(os.path.join(os.path.dirname(__file__), 'config.json'))))  
            self.workingDir = Shared.GetWorkspaceDirectory(self.config["workingdirectory"]) 
            
            WiMLogging.init(os.path.join(self.workingDir,"Temp"),"Integration.log")
            WiMLogging.sm("Starting routine")
            
            BCheaders = ['DRNAREA','CARBON']
            StatHeaders =['BFAREA','BFFLOW','BFWDTH','BFDPTH']
            headerline = ",".join(["site_no","dec_long","dec_lat"]+BCheaders+StatHeaders)
            Shared.appendLineToFile(os.path.join(self.workingDir,self.config["outputFile"].format("PA")),headerline)

            for row in file:  
                self._run(row[id],row[x],row[y],BCheaders,StatHeaders)
            #next station            
            
            WiMLogging.sm('Finished.  Total time elapsed:', str(round((time.time()- startTime)/60, 2)), 'minutes')

        except:
             tb = traceback.format_exc()
             WiMLogging.sm("Error executing delineation wrapper "+tb)
    def _run(self,stationid, x,y,BCheaders,StatHeaders):
        watershed = None
        bestCorrelatedGage=None
        FDCTMResult = None
        try:
            print stationid

            with StreamStatsServiceAgent() as sa:
                watershed = sa.getBasin('PA',x,y,4326,'drnarea;carbon')
                flowstats = sa.getFlowStats('PA',watershed['workspaceID'],'BNKF')
            #end with
            #with WIMServiceAgent() as wsa:
            #    bestCorrelatedGage = wsa.getKrigGages('IA',x,y, 4326)[0]                
            #    FDCTMResult = wsa.getFDCTMResults('IA',start, end, bestCorrelatedGage["ID"], self._cleanParams(watershed['parameters']))
            ##end with

            #do something with result
            print "writing to file"
            resultline = [stationid,x,y]
            bc = dict([bc['code'],bc['value']] for bc in watershed['parameters'])
            fs = dict([x['code'],x['Value']] for x in flowstats[0][u'RegressionRegions'][0][u'Results'])

            resultline=resultline+self._formatRow(bc,BCheaders)+self._formatRow(fs,StatHeaders)
  
            Shared.appendLineToFile(os.path.join(self.workingDir,self.config["outputFile"].format("PA")),",".join(resultline))

        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error w/ station "+ stationid +" "+ tb)            

    def _cleanParams(self, parameterlist):
        #ensures'drnarea;precip;rsd;hysep;stream_varg;SSURGOB;SSURGOC;SSURGOD' are in the correct format
        toLowerList =['drnarea','precip','rsd','hysep','stream_varg']
        returnlist =[]
        for p in parameterlist:
            code = p['code']
            if code.lower() in toLowerList:
                code = p['code'].lower()
            if p['value'] < 0:
                print "%s found to be less than 0 (%s) updated to 0"% (p['code'],p['value'])
                p[u'unit'] += " !WARNING value computed < 0, please verify correct."
                
                p['value']=0
            #endif
            returnlist.append({'code':code,'value':p['value']})   
        #next p
        return returnlist
    def _formatRow(self, params, definedkeys =None):        
            r = []
            keys = params if not definedkeys else definedkeys
            for k in keys:
                value = params[k]
                r.append(str(value))                
            #next p

            return r
    def _writeResultsToFile(self,result,id):
        try:
            file =[]
            file.append("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
            file.append("Execute Date: " + str(datetime.date.today()))
            file.append("Station Idenifier: " +id)
            file.append("Model: "+ result['Description'])
            file.append("output range: %s - %s" % (result['StartDate'],result['EndDate']))
            file.append(result[u'LINKS'][0][u'Href'])
            file.append("-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
            file.append("Best Correlated Gage: %s %s" % (result[u'ReferanceGage'][u'StationID'],
                                            result[u'ReferanceGage'][u'Name'].replace(',', '')))
            file.append('')
            file.append("Parameter,value,unit")
            for param in result[u'Parameters']:
                file.append(','.join([param[u'name'],str(param[u'value']),param[u'unit']]))
            file.append('')
            file.append('Estimated Flow Observations')
            file.append("Date,Value")
            for obs in result[u'EstimatedFlow'][u'Observations']:
                file.append(','.join([datetime.datetime.strptime(obs['Date'], 
                               '%Y-%m-%dT%H:%M:%S').strftime('%m/%d/%y'),str(obs['Value'])]))

            Shared.writeToFile(os.path.join(self.workingDir,self.config["outputFile"].format(id)),file)            

        except:
            tb = traceback.format_exc()
            WiMLogging.sm("Error w/ station "+ json.dumps(station)+" "+ tb) 

if __name__ == '__main__':
    IntegrationWrapper()