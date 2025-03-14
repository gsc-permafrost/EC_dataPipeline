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


import importlib
importlib.reload(helperFunctions)

@dataclass(kw_only=True)
class database:
    fillChar = '_'
    sepChar = '-'
    metadataFile = helperFunctions.loadDict(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
    # sessionMetadata: dict = field(default_factory=lambda:{},repr=False)
    projectPath: str = field(default=None,repr=False)
    overwrite: bool = field(default=False,repr=False)
    verbose: bool = field(default=True,repr=False)
    # newProject: bool = field(default=False,repr=False)
    # validate: bool = field(default=False,repr=False)
    lookup: bool = field(default=False,repr=False)
    
    def __post_init__(self):
        if self.projectPath:
            if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
                self.metadataFile['.dateCreated'] = self.now()
                self.metadataFile['.dateModified'] = self.now()
                for d in self.metadataFile['directoryStructure']:
                    os.makedirs(os.path.join(self.projectPath,d))
                # self.newProject = True
                # print('a')
                # siteInventory(projectPath=self.projectPath,newProject=self.newProject)
                # measurementInventory(projectPath=self.projectPath,newProject=self.newProject)
            elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')) and not self.newProject:
                sys.exit('Non-empty, non-project directory provided')
            else:
                self.metadataFile = helperFunctions.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
                self.metadataFile['.dateModified'] = self.now()
                # if self.validate:
                #     sI = os.path.join(self.projectPath,'metadata','siteInventory.yml')
                #     Sites = helperFunctions.loadDict(sI)
                #     for siteID,values in Sites.items():
                #         siteInventory(projectPath=self.projectPath,index=siteID,overwrite=True,**values)
                #         mI = os.path.join(self.projectPath,'metadata',siteID,'measurementInventory.yml')
                #         Measurements = helperFunctions.loadDict(mI)
                #         for subID,vals in Measurements.items():
                #             measurementInventory(projectPath=self.projectPath,siteID=siteID,index=subID,**vals)
            helperFunctions.saveDict(self.metadataFile,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)

    def now(self,fmt='%Y-%m-%d %H:%M:%S'):
        return(datetime.datetime.now().strftime(fmt))
    

@dataclass(kw_only=True)
class metadataRecord(database):

    def __post_init__(self):
        super().__post_init__()
        if self.projectPath:
            self.public = helperFunctions.baseFields(self,repr=True)
            self.index = self.public[0]
            self.__dict__[self.index] = self.safeFmt(self.__dict__[self.index])
            self.private = helperFunctions.baseFields(self,repr=False)
            src = os.path.join(self.projectPath,self.mapSubdir(self.subDir),type(self).__name__+'.'+self.ext)
            print('confirm validation and filling procedures')
            self.inventory = helperFunctions.loadDict(src)
            for mobject in [m for m in type(self).mro() if m not in type(metadataRecord()).mro() and m.__name__ != type(self).__name__][::-1]:
                src = os.path.join(self.projectPath,self.mapSubdir(mobject().subDir),mobject.__name__+'.'+mobject().ext)
                for v in self.mapSubdir(self.subDir,path=False).values():
                    tmp = helperFunctions.loadDict(src)
                    if v in tmp.keys():
                        for key,val in tmp[v].items():
                            if self.__dict__[key] is None:
                                self.__dict__[key] = val

            self.inventory[self.__dict__[self.index]] = {r:self.__dict__[r] for r in self.public}
            self.validateCoordinates()
            self.saveInventory()
    
    def safeFmt(self,sIn):
        safeName = sIn.replace(' ',self.sepChar)
        safeName = re.sub('[^0-9a-zA-Z_]+',self.fillChar, safeName)
        if sIn != safeName:
            if self.verbose:print('Unsafe name: ',sIn,' \nConverting to: ',safeName)
            sIn = safeName
        return(sIn)

    def mapSubdir(self,items,path=True):
        if path:
            return(os.path.sep.join([self.__dict__[s] if s in self.__dict__.keys() else s for s in items]))
        else:
            return({s:self.__dict__[s] for s in items if s in self.__dict__.keys() })
        
    def validateCoordinates(self):
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
    
    def saveInventory(self):
        src = os.path.join(self.projectPath,self.mapSubdir(self.subDir),type(self).__name__+'.'+self.ext)
        helperFunctions.saveDict(self.inventory,src)

@dataclass(kw_only=True)
class subSite(metadataRecord):
    subsiteID: str = None
    description: str = None
    latitude: float = None
    longitude: float = None

    def __post_init__(self):
        self.validateCoordinates()
        self.record = {k:v for k,v in self.__dict__.items() if self.__dataclass_fields__[k].repr}

@dataclass(kw_only=True)
class siteInventory(metadataRecord):
    ext = 'yml'
    subDir = ['metadata']
    siteID: str = None
    siteDescription: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    subSites: dict = field(default_factory=lambda:{})

    def __post_init__(self):
        if self.projectPath:
            if self.subSites != {}:
                self.subSites = self.validateSubSites()
            super().__post_init__()
    
    def validateSubSites(self):
        validated = {}
        for k,v in self.subSites.items():
            if 'subsiteID' not in v.keys():
                v['subsiteID'] = k
            tmp = subSite(**v)
            validated[tmp.subsiteID] = tmp.record
        return(validated)
    
    
@dataclass(kw_only=True)
class measurementInventory(siteInventory):
    ext = 'yml'
    subDir = ['metadata','siteID']
    measurementID: str = None
    measurementDescription: str = None
    subsiteID: str = None
    def __post_init__(self):
        if self.projectPath:
            super().__post_init__()

 
@dataclass(kw_only=True)
class fileInventory(measurementInventory):
    ext = 'json'
    subDir = ['metadata','siteID','measurementID']
    qID: str = '1'
    source: str = None
    inventory: dict = field(repr=False,default=None)
    def __post_init__(self):
        if self.projectPath:
            super().__post_init__()
        
               

#     # positionID: int = 1
#     fileType: str = field(default=None,repr=False)
#     description: str = field(default=None,repr=False)
#     frequency: str = field(default=None,repr=False)
#     startDate: str = field(default=None,repr=False)
#     stopDate: str = field(default=None,repr=False)
#     latitude: float = field(default=None,repr=False)
#     longitude: float = field(default=None,repr=False)
#     tag: str = field(default=None,repr=False)

# @dataclass(kw_only=True)
# class metadataRecord(database):
#     # Formats a metadata entry for either a full site or a specific measurement
#     siteID: str = field(default=None,repr=False)
#     measurementID: str = field(default=None,repr=False)
#     source: str = field(default=None,repr=False)
#     index: str = field(default=None,repr=False)
#     safeName: bool = field(default=True,repr=False)
#     inventory: dict = field(default=dict,repr=False)
#     def __post_init__(self,inventoryFile=None):
#         if not self.projectPath:
#             return
#         if type(self).__name__ != 'metadataRecord' and inventoryFile:
#             private = helperFunctions.baseFields(metadataRecord(projectPath=None),repr=False)
#             tmp = os.path.split(inventoryFile)[0].replace(self.projectPath+os.path.sep,'').split(os.path.sep)
#             if len(tmp)>1:
#                 for i,k in enumerate(tmp[1:]):
#                     print(private[i])
#                     print(self)
#                     print(self.__dict__[private[i]])
#         super().__post_init__()
#         self.attributes = helperFunctions.baseFields(self,repr=False)
#         self.ID = helperFunctions.baseFields(self,repr=True)
#         if inventoryFile is not None:
#             self.inventory = helperFunctions.loadDict(inventoryFile)
#             if self.index is not None and not self.overwrite:
#                 self.record = self.inventory[self.index]
#                 for f,v in self.__dataclass_fields__.items():
#                     if f in self.record.keys():
#                         self.__dict__[f] = self.record[f]
#         if self.safeName == True:
#             for k,v in self.__dataclass_fields__.items():
#                 if self.__dict__[k] is not None and v.kw_only and v.type == str:
#                     if self.__dataclass_fields__[k].repr and k != 'source':
#                         self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.fillChar, self.__dict__[k])
#                     elif k.endswith('Date'):
#                         self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.sepChar, self.__dict__[k])
#         self.makeID()
#         self.record = {}
#         for f in self.attributes:
#             if self.safeName:
#                 self.record.setdefault(self.index,{}).setdefault(f,self.__dict__[f])
#             else:
#                 self.record.setdefault(f,self.__dict__[f])
#         if not self.lookup and not self.overwrite and self.index in self.inventory.keys():
#             print('Duplicate Warning, do something here')
#         #     while self.index in self.inventory.keys() and self.record[self.index] != self.inventory[self.index]:
#         #         temp = self.record.pop(self.index)
#         #         # self.positionID+=1
#         #         self.makeID()
#         #         self.record[self.index] = temp
#         if inventoryFile is not None or self.overwrite:
#             self.saveInventory(inventoryFile)
    
#     def makeID(self):
#         ID = [str(self.__dict__[k]) if self.__dict__[k] is not None else k for k in self.ID]
#         self.nestDepth = len(ID)
#         self.index = self.sepChar.join(ID)

#     def parseID(self):
#         i = 0
#         ID = self.index.split(self.sepChar)
#         for k,v in self.__dataclass_fields__.items():
#             if v.repr and i < len(ID):
#                 self.__dict__[k] = ID[i]
#                 i += 1
#         self.index = self.__dataclass_fields__['index'].default
        
#     def saveInventory(self,fileName):
#         helperFunctions.updateDict(self.inventory,self.record,overwrite=self.overwrite)
#         helperFunctions.saveDict(self.inventory,fileName,sort_keys=True)

# @dataclass(kw_only=True)
# class siteInventory(metadataRecord):
#     siteID: str = None
#     description: str = field(default=None,repr=False)
#     Name: str = field(default=None,repr=False)
#     PI: str = field(default=None,repr=False)
#     startDate: str = field(default=None,repr=False)
#     stopDate: str = field(default=None,repr=False)
#     landCoverType: str = field(default=None,repr=False)
#     latitude: float = field(default=None,repr=False)
#     longitude: float = field(default=None,repr=False)
#     def __post_init__(self):
#         # if self.siteID is None and self.index is not None:
#         #     self.parseID()
#         if self.latitude is not None and self.longitude is not None:
#             coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
#             self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
#         if self.siteID is None:
#             si = os.path.join(self.projectPath,'templates','siteInventory.yml')
#         else:
#             si = os.path.join(self.projectPath,'metadata','siteInventory.yml')
#         super().__post_init__(inventoryFile=si)

# @dataclass(kw_only=True)
# class measurementInventory(metadataRecord):
#     measurementID: str = None
#     # positionID: int = 1
#     fileType: str = field(default=None,repr=False)
#     description: str = field(default=None,repr=False)
#     frequency: str = field(default=None,repr=False)
#     startDate: str = field(default=None,repr=False)
#     stopDate: str = field(default=None,repr=False)
#     latitude: float = field(default=None,repr=False)
#     longitude: float = field(default=None,repr=False)
#     tag: str = field(default=None,repr=False)
#     def __post_init__(self):
#         # if self.measurementID is None and self.index is not None:
#         #     self.parseID()
#         if self.latitude is not None and self.longitude is not None:
#             coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
#             self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
#         if self.index is None and self.siteID is not None:
#             temp = siteInventory(projectPath = self.projectPath,siteID=self.siteID,lookup=True)
#             self.latitude = temp.inventory[self.siteID]['latitude']
#             self.longitude = temp.inventory[self.siteID]['longitude']
#             self.startDate = temp.inventory[self.siteID]['startDate']
#             self.stopDate = temp.inventory[self.siteID]['stopDate']
#         if self.siteID is None:
#             mI = os.path.join(self.projectPath,'templates','siteID','measurementInventory.yml')
#         else:
#             mI = os.path.join(self.projectPath,'metadata',self.siteID,'measurementInventory.yml')
#         super().__post_init__(mI)
#         if self.siteID is not None and self.measurementID is not None:
#             subsiteID = self.index.lstrip(self.sepChar+self.siteID)
#             os.makedirs(os.path.join(self.projectPath,'metadata',self.siteID,subsiteID),exist_ok=True)
#             if self.startDate is not None:
#                 self.startYear = dateParse.parse(self.startDate).year
#                 if self.stopDate is not None:
#                     self.endYear = dateParse.parse(self.stopDate).year
#                 else: self.endYear = datetime.datetime.now().year
#                 for yr in range(self.startYear,self.endYear+1):
#                     db = os.path.join(self.projectPath,'database',str(yr),self.siteID,subsiteID)
#                     os.makedirs(db,exist_ok=True)

# @dataclass(kw_only=True)
# class fileInventory(metadataRecord):
#     source: str = None
#     ext: str = field(default='',repr=False)
#     matchPattern: list = field(default_factory=lambda:[],repr=False)
#     excludePattern: list = field(default_factory=lambda:[],repr=False)
#     fileList: list = field(default_factory=lambda:[],repr=False)

#     def __post_init__(self):
#         if self.source:
#             self.source = os.path.abspath(self.source)
#         for f,v in self.__dataclass_fields__.items():
#             if type(self.__dict__[f]) is not list and v.type is list:
#                 self.__dict__[f] = [self.__dict__[f]]
                
#         fI = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,'fileInventory.json')
        
#         # temp = measurementInventory(projectPath = self.projectPath,siteID=self.siteID,lookup=True)


#         super().__post_init__(inventoryFile=fI)
#         if not self.lookup:
#             for dir,_,files in os.walk(self.source):
#                 subDir = os.path.relpath(dir,self.source)
#                 self.fileList += [[os.path.join(subDir,f),False] for f in files 
#                     if f.endswith(self.ext)
#                     and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
#                     and sum([e in f for e in self.excludePattern]) == 0
#                     and [os.path.join(subDir,f),False] not in self.fileList
#                     and [os.path.join(subDir,f),True] not in self.fileList
#                     ]
#         self.saveInventory(fI)













    # def fileSearch(self,source,ext,matchPattern=[],excludePattern=[]):
    #     self.source = source
    #     self.ext = ext
    #     self.matchPattern = matchPattern
    #     self.excludePattern = excludePattern
    #     self.source = os.path.abspath(self.source)
    #     fI = os.path.join(self.projectPath,'metadata',self.siteID,self.index,'fileInventory.yml')
    #     self.fileInventory = {}
    #     self.fileInventory.setdefault(self.siteID,{}).setdefault(self.index,{}).setdefault(self.source,[])
    #     for dir,_,files in os.walk(self.source):
    #         subDir = os.path.relpath(dir,self.source)
    #         fileList = self.fileInventory[self.siteID][self.index][self.source]
    #         fileList += [[os.path.join(subDir,f),False] for f in files 
    #             if f.endswith(self.ext)
    #             and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
    #             and sum([e in f for e in self.excludePattern]) == 0
    #             and [os.path.join(subDir,f),False] not in fileList
    #             and [os.path.join(subDir,f),True] not in fileList]

# class databaseProject(database):
#     def __init__(self,projectPath):
#         super().__init__(projectPath = projectPath, validate=True)


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

