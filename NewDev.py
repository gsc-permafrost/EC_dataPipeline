import re
import os
import sys
import yaml
import copy
import numpy as np
import pandas as pd
import datetime
import siteCoordinates
import helperFunctions
import dateutil.parser as dateParse 
from dataclasses import dataclass, field

class _base(object):
    def __post_init__(self):
        # just intercept the __post_init__ calls so they
        # aren't relayed to `object`
        pass

@dataclass(kw_only=True)
class database:
    fillChar = '_'
    sepChar = '-'
    projectPath: str = field(repr=False)
    overwrite: bool = field(default=False,repr=False)
    verbose: bool = field(default=False,repr=False)
    metadataFile: dict = field(default_factory=lambda:helperFunctions.loadDict(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
        ,repr=False)
    newProject: bool = field(default=False,repr=False)
    validate: bool = field(default=False,repr=False)
    
    def __post_init__(self):
        if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
            self.metadataFile['.dateCreated'] = self.now()
            self.metadataFile['.dateModified'] = self.now()
            for d in self.metadataFile['directoryStructure']:
                os.makedirs(os.path.join(self.projectPath,d))
            self.newProject = True
            siteInventory(projectPath=self.projectPath,newProject=self.newProject)
            measurementInventory(projectPath=self.projectPath,newProject=self.newProject)
        elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')) and not self.newProject:
           sys.exit('Non-empty, non-project directory provided')
        else:
            self.metadataFile = helperFunctions.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
            self.metadataFile['.dateModified'] = self.now()
            if self.validate:
                for kw,obj in zip(['siteInventory.yml','measurementInventory.yml'],[siteInventory,measurementInventory]):
                    inventory = [helperFunctions.loadDict(os.path.join(dir,f))
                                for dir,_,file in os.walk(os.path.join(self.projectPath,'metadata'))
                                for f in file if f == kw]
                    for inv in inventory:
                        for key,values in inv.items():
                            obj(projectPath=self.projectPath,ID=key,overwrite=True,**values)
            # sI in [os.path.join(dir,f) for dir,_,file in os.walk(os.path.join(self.projectPath,'metadata')) for f in file if f == 'siteInventory.yml']:
            #     (helperFunctions.loadDict(sI))
            # print([os.path.join(dir,f) for dir,_,file in os.walk(os.path.join(self.projectPath,'metadata')) for f in file if f == 'measurementInventory.yml'])
        helperFunctions.saveDict(self.metadataFile,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)

    def now(self,fmt='%Y-%m-%d %H:%M:%S'):
        return(datetime.datetime.now().strftime(fmt))

@dataclass(kw_only=True)
class metadataRecord(database):
    # Formats a metadata entry for either a full site or a specific measurement
    siteID: str = 'siteID'
    measurementType: str = None
    loggerID: str = None
    ID: str = field(default=None,repr=False)
    safeName: bool = field(default=True,repr=False)
    inventory: dict = field(default=dict,repr=False)
    def __post_init__(self,inventoryFile=None):
        super().__post_init__()
        if inventoryFile is not None:
            self.inventory = helperFunctions.loadDict(inventoryFile)
            if self.ID is not None and not self.overwrite:
                self.record = self.inventory[self.ID]
                for f,v in self.__dataclass_fields__.items():
                    if f in self.record.keys():
                        self.__dict__[f] = self.record[f]
        if self.safeName == True:
            for k,v in self.__dataclass_fields__.items():
                if self.__dict__[k] is not None and v.kw_only and v.type == str:
                    if self.__dataclass_fields__[k].repr:
                        self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.fillChar, self.__dict__[k])
                    elif k.endswith('Date'):
                        self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.sepChar, self.__dict__[k])
        ID = [str(self.__dict__[k]) for k in metadataRecord.__dataclass_fields__.keys() 
                                    if self.__dataclass_fields__[k].repr and
                                    (self.__dict__[k] is not None)]
        self.nestDepth = len(ID)
        self.ID = self.sepChar.join(ID)
        self.record = {}
        for f,v in self.__dataclass_fields__.items():
            if f not in metadataRecord.__dataclass_fields__.keys():
                if self.safeName:
                    self.record.setdefault(self.ID,{}).setdefault(f,self.__dict__[f])
                else:
                    self.record.setdefault(f,self.__dict__[f])
        if inventoryFile is not None or self.overwrite:
            helperFunctions.updateDict(self.inventory,self.record,overwrite=self.overwrite)
            if 'siteID' in self.inventory.keys() and len(self.inventory.keys())>1:
                self.inventory.pop('siteID')
            helperFunctions.saveDict(self.inventory,inventoryFile)

    def parseID(self):
        i = 0
        ID = self.ID.split(self.sepChar)
        for k in metadataRecord.__dataclass_fields__.keys():
            if self.__dataclass_fields__[k].repr and i < len(ID):
                self.__dict__[k] = ID[i]
                i += 1
                
@dataclass(kw_only=True)
class siteInventory(metadataRecord):
    description: str = field(default=None,repr=False)
    name: str = field(default=None,repr=False)
    PI: str = field(default=None,repr=False)
    startDate: str = field(default=None,repr=False)
    endDate: str = field(default=None,repr=False)
    landCoverType: str = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)
    longitude: float = field(default=None,repr=False)
    def __post_init__(self):
        if self.siteID == 'siteID' and self.ID is not None:
            self.parseID()
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        if self.siteID == 'siteID':
            si = os.path.join(self.projectPath,'templates','siteInventory.yml')
        else:
            si = os.path.join(self.projectPath,'metadata','siteInventory.yml')
        super().__post_init__(inventoryFile=si)

@dataclass(kw_only=True)
class measurementInventory(metadataRecord):
    siteID: str = field(default='siteID',repr=False)
    description: str = field(default=None,repr=False)
    frequency: str = field(default=None,repr=False)
    fileType: str = field(default=None,repr=None)
    startDate: str = field(default=None,repr=False)
    endDate: str = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)
    longitude: float = field(default=None,repr=False)
    def __post_init__(self):
        if self.siteID == 'siteID' and self.ID is not None:
            self.parseID()
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        if self.ID is None and self.siteID != 'siteID':
            temp = siteInventory(projectPath = self.projectPath,siteID=self.siteID)
            self.latitude = temp.inventory[self.siteID]['latitude']
            self.longitude = temp.inventory[self.siteID]['longitude']
            self.startDate = temp.inventory[self.siteID]['startDate']
            self.endDate = temp.inventory[self.siteID]['endDate']
        if self.siteID == 'siteID':
            mI = os.path.join(self.projectPath,'templates',self.siteID,'measurementInventory.yml')
        else:
            mI = os.path.join(self.projectPath,'metadata',self.siteID,'measurementInventory.yml')
        super().__post_init__(mI)
        subsiteID = self.ID.lstrip(self.sepChar+self.siteID)
        os.makedirs(os.path.join(self.projectPath,'metadata',self.siteID,subsiteID),exist_ok=True)
        if self.startDate is not None and self.siteID != 'siteID':
            self.startYear = dateParse.parse(self.startDate).year
            if self.endDate is not None:
                self.endYear = dateParse.parse(self.endDate).year
            else: self.endYear = datetime.datetime.now().year
            for yr in range(self.startYear,self.endYear+1):
                db = os.path.join(self.projectPath,'database',str(yr),self.siteID,subsiteID)
                os.makedirs(db,exist_ok=True)


class databaseProject(database):
    def __init__(self,projectPath):
        super().__init__(projectPath = projectPath, validate=True)

    # def validate():

    #     # if os.path.isfile()
    #     self.createProject()
    # def createProject(self):
    #     siteInventory(projectPath=self.projectPath)
    #     measurementInventory(projectPath=self.projectPath)
    # def __init__(self,projectPath):
    #     os.path.join(self.projectPath,'projectInfo.yml')


# @dataclass(kw_only=True)
# class observation(metadataRecord):
#     # Metadata associated with a single trace
#     variableName: str = None
#     originalName: str = field(repr=False)
#     ignore: bool = field(default=False,repr=False)
#     unit: str = field(default=None,repr=False)
#     dtype: str = field(default=None,repr=False)
#     variableDescription: str = field(default=None,repr=False)
#     # positional variables (vertical, horizontal, replicate)
#     vertical: int = 1
#     horizontal: int = 1
#     replicate: int = 1
#     def __post_init__(self):
#         if self.variableName is None:
#             self.variableName = self.originalName
#         super().__post_init__()

