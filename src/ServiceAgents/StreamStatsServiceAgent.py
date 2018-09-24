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
import glob, sys, os
import requests
import certifi
import string
import traceback
from  WIMLib import WiMLogging
import re

from datetime import date, timedelta


class StreamStatsServiceAgent(object):
    #region Constructor
    def __init__(self):
        self.BaseUrl = Config()["StreamStats"]["baseurl"]
        self.resources = Config()["StreamStats"]["resources"]

        self._sm("initialized StreamStatsServiceAgent")
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.BaseUrl=None

    #region Methods
    def getBasin(self, region, xpoint , ypoint, crs = 4326,parameterlist=False):
        try:
            resource = self.resources["watershed"].format(region, xpoint, ypoint,crs,parameterlist)          

            try:
                results = self.Execute(resource)
                return results
            except:
                tb = traceback.format_exc()
                self._sm("Exception raised for "+ os.path.basename(resource), "ERROR")
        except:
            tb = traceback.format_exc()
            self._sm("StreamStatsService getBasin Error "+tb, "ERROR")

    def getBChar(self,region,workspaceID):
        try:
            resource = self.resources["basinChar"].format(region,workspaceID,True)

            try:
                results = self.Execute (resource)
                return results
            except:
                tb = traceback.format_exc()
                self._sm("Exception raised for " + os.path.basename(resource), "ERROR")
        except:
            tb = traceback.format_exc()
            self._sm("StreamstatsService getBChar Error "+tb, "ERROR")


    def getFlowStats(self,region,workspaceID,flowstats):
        results={}
        try:            
            resource = self.resources["flowStats"].format(region, workspaceID,flowstats)          
            
            try:
                results = self.Execute(resource)
                return results
            except:
                tb = traceback.format_exc()
                self._sm("Exception raised for "+ os.path.basename(resource), "ERROR")
            
            return results
        except:
            tb = traceback.format_exc()
            self._sm("StreamStatsService getBasinCharacteristics Error "+tb, "ERROR")

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
        except requests.exceptions as e:
            if hasattr(e, 'reason'):
                self._sm("Error:, failed to reach a server " + e.reason.strerror, 1.54, 'ERROR')
                return ""

            elif hasattr(e, 'code'):
                self._sm("Error: server couldn't fullfill request " + e.code, 1.58, 'ERROR')
                return ''
        except:
            tb = traceback.format_exc()            
            self._sm("url exception failed " + resource + ' ' + tb, 1.60, 'ERROR')
            return ""    

    def _sm(self,msg,type="INFO", errorID=0):        
        WiMLogging.sm(msg,type="INFO", errorID=0)
        print type, msg

    #endregion
    #region Helper Methods
    #endregion
    

