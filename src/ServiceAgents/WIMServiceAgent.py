#------------------------------------------------------------------------------
#----- StreamStatsServiceAgent.py ----------------------------------------------------
#------------------------------------------------------------------------------
#
#  copyright:  2018 WiM - USGS
#
#    authors:  Jeremy K. Newson - USGS Web Informatics and Mapping (WiM)
#              
#    purpose:  StreamStatsServiceAgent is a server class to provide hunting and gathering  
#                   methods for NLDI service
#
#      usage:  THIS SECTION NEEDS TO BE UPDATED
#
# discussion:  THIS SECTION NEEDS TO BE UPDATED
#
#
#      dates:   07 Jul 2018 jkn - Created
#
#------------------------------------------------------------------------------

#region "Imports"
import traceback
import json
from WIMLib.Config import Config
import os
import requests
from datetime import datetime


from  WIMLib.WiMLogging import WiMLogging as log


class ServiceAgentBase(object):
    """ """
    #region Constructor
    def __init__(self,baseurl):
        self.BaseUrl = baseurl
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.BaseUrl = None
    #endregion

    #region Methods
    def Execute(self, resource):
        try:
            url = self.BaseUrl + resource
            #below is temporary for batch jkn
            try:
                response = requests.get(url)
                return response.json()
            except:
                self._sm("Error: file " + os.path.basename(resource) + " does not exist within Gages iii", 1.62, 'ERROR')
                return ''
        except requests.exceptions.RequestException as e:
                self._sm("Error:, failed to reach a server " + e.strerror, 1.54, 'ERROR')
                return ""
        except:
            tb = traceback.format_exc()            
            self._sm("url exception failed " + resource + ' ' + tb, 1.60, 'ERROR')
            return ""    
    
    def indexMatching(self, seq, condition):
        for i,x in enumerate(seq):
            if condition(x):
                return i
        return -1

    def _sm(self,msg,type="INFO", errorID=0):        
        log().sm(msg,type="INFO", errorID=0)

#endregion

class WIMServiceAgent(ServiceAgentBase):
    #region Constructor
    def __init__(self):
        ServiceAgentBase.__init__(self, Config()["WIM"]["baseurl"])
        self.resources = Config()["WIM"]["resources"]

        self._sm("initialized WIMServiceAgent")
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        ServiceAgentBase.__exit__(self, exc_type, exc_value, traceback) 
    #endregion
    #region Methods
    def getKrigGages(self, region, xpoint , ypoint, crs = 4326):
        try:
            resource = self.resources["krig"].format(region, xpoint, ypoint,crs)          

            try:
                results = self.Execute(resource)
                return results
            except:
                tb = traceback.format_exc()
                self._sm("Exception raised for "+ os.path.basename(resource), "ERROR")
        except:
            tb = traceback.format_exc()
            self._sm("WIM Krig getkrig gages Error "+tb, "ERROR")
    def getFDCTMResults(self,region, startDate, endDate, nwisGageID, listParameters):
        results={}
        try: 
            url = self.BaseUrl+ self.resources["models"].format('FDCTM',region)          
            payload ={
                    'startdate':datetime.strptime(startDate, '%m/%d/%Y').isoformat() ,
                    'enddate':datetime.strptime(endDate, '%m/%d/%Y').isoformat() ,
                    'nwis_station_id':nwisGageID,
                    'parameters':listParameters
                }
            try:
                response = requests.post(url, json=payload)
                return response.json()
            except:
                tb = traceback.format_exc()
                self._sm("Exception raised for "+ os.path.basename(url) + tb, "ERROR")
            
            return results
        except:
            tb = traceback.format_exc()
            self._sm("WIM FDCTM fetFDCTM Error "+tb, "ERROR")

    #endregion
    #region Helper Methods
    #endregion
    