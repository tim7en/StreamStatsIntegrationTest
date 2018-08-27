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
from WIMLib.ServiceAgents import ServiceAgentBase
from WIMLib.Config import Config
import os

#endregion

class StreamStatsServiceAgent(ServiceAgentBase.ServiceAgentBase):
    #region Constructor
    def __init__(self):
        ServiceAgentBase.ServiceAgentBase.__init__(self, Config()["StreamStats"]["baseurl"])
        self.resources = Config()["StreamStats"]["resources"]

        self._sm("initialized StreamStatsServiceAgent")
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        ServiceAgentBase.ServiceAgentBase.__exit__(self, exc_type, exc_value, traceback) 
    #endregion
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

    #endregion
    #region Helper Methods
    #endregion
    