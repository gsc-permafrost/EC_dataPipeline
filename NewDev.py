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

class database:
    # Base class containing common functionality across the database
    fillChar = '_'
    sepChar = '-'
    projectPath = os.getcwd()
    verbose = False
    metadataFile = helperFunctions.loadDict(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
    
    def __init__(self,projectPath=None,verbose=None):
        for arg in self.__init__.__code__.co_varnames[1:self.__init__.__code__.co_argcount]:
            if locals()[arg]:
                setattr(self,arg,locals()[arg])
        if self.projectPath:
            if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
                self.make()
            elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')):
                sys.exit('Non-empty, non-project directory provided')
            else:
                self.metadataFile = helperFunctions.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
                print('Add Validation?')
                self.save()
    
    def make(self):
        self.metadataFile['dateCreated'] = self.now()
        self.metadataFile['dateModified'] = self.now()
        for d in self.metadataFile['directoryStructure']:
            os.makedirs(os.path.join(self.projectPath,d))
        self.save()

    def now(self,fmt='%Y-%m-%d %H:%M:%S'):
        return(datetime.datetime.now().strftime(fmt))
    
    def save(self,inventory=False):
        self.metadataFile['dateModified'] = self.now()
        helperFunctions.saveDict(self.metadataFile,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)
        if inventory:
            helperFunctions.saveDict(self.inventory,self.src)

@dataclass(kw_only=True)
class metadataRecord(database):
    index: str = None
    subIndex: list = field(default=None)
    projectPath: str = field(default=None,repr=False)
    overwrite: bool = field(default=False,repr=False)
    verbose: bool = field(default=False,repr=False)

    def __post_init__(self):
        super().__init__()
        if not self.projectPath:
            return
        # Define the index for new records 
        if not self.index:
            self.index=self.sepChar.join([self.safeFmt(k) for k,v in self.__dataclass_fields__.items() if v.metadata == 'ID' and self.__dict__[k]])
        
        # lookup the parents attributes
        parents = [p for p in type(self).mro() if p not in type(metadataRecord()).mro() and p.__name__ != type(self).__name__]
        for parent in parents:
            src = os.path.join(self.projectPath,self.mapSubdir(parent().subDir),parent.__name__+'.'+parent().ext)
            tmp = helperFunctions.loadDict(src)
            if self.index and self.index in tmp.keys():
                self.load(tmp[self.index])
            else:
                for v in self.mapSubdir(self.subDir,path=False).values():
                    if v in tmp.keys():
                        self.load(tmp[v])
    
        # load the inventory for the child
        self.src = os.path.join(self.projectPath,self.mapSubdir(self.subDir),type(self).__name__+'.'+self.ext)
        self.inventory = helperFunctions.loadDict(self.src)
        # get relevant attributes for the specific record
        self.public = helperFunctions.baseFields(self,repr=True)
        rec = {r:self.__dict__[r] for r in self.public}
        self.private = helperFunctions.baseFields(self,repr=False)
        if len(self.private):
            self.subIndex = [self.__dict__[p] for p in self.private if self.__dataclass_fields__[p].metadata == 'subID']
            rec = helperFunctions.defaultNest(self.subIndex[::-1],rec)
        print('confirm validation and filling procedures')

        # self.inventory[self.index] = rec
        helperFunctions.updateDict(self.inventory,{self.index:rec})

        self.validateCoordinates()
        self.save(True)
    
    def load(self,tmp):
        for key,val in tmp.items():
            if self.__dict__[key] is None and not self.overwrite:
                self.__dict__[key] = val

    def safeFmt(self,k):
        safeName = self.__dict__[k].replace(' ',self.sepChar)
        safeName = re.sub('[^0-9a-zA-Z_]+',self.fillChar, safeName)
        if self.__dict__[k] != safeName:
            if self.verbose:print('Unsafe name: ',self.__dict__[k],' \nConverting to: ',safeName)
            self.__dict__[k] = safeName
        return(self.__dict__[k])

    def mapSubdir(self,items,path=True):
        if path:
            return(os.path.sep.join([self.__dict__[s] if s in self.__dict__.keys() else s for s in items]))
        else:
            return({s:self.__dict__[s] for s in items if s in self.__dict__.keys() })
        
    def validateCoordinates(self):
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']

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
    siteID: str = field(default=None,metadata='ID')
    siteDescription: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    subSites: dict = None#field(default_factory=lambda:{})

    def __post_init__(self):
        if not self.projectPath:
            return
        if self.subSites:
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
    siteID: str = None
    measurementID: str = field(default=None,metadata='ID')
    subsiteID: str = field(default=None,metadata='ID')
    fileType: str = field(default=None,metadata='ID')
    baseFrequency: str = None
    measurementDescription: str = None

    def __post_init__(self):
        if not self.projectPath:
            return
        super().__post_init__()

 
@dataclass(kw_only=True)
class sourcefileInventory(measurementInventory):
    ext = 'json'
    subDir = ['metadata','siteID']
    sourcePath: str = field(default=None,repr=False,metadata='subID')
    fileExt: str = None
    # fileExt: str = field(default=None,repr=False,metadata='subID')
    matchPattern: list = field(default_factory=lambda:[])
    excludePattern: list = field(default_factory=lambda:[])
    fileList: list = field(default_factory=lambda:[])
    lookup: bool = field(default=False,repr=False)
    read: bool = field(default=True,repr=False)

    def __post_init__(self):
        if self.sourcePath:
            self.sourcePath = os.path.abspath(self.sourcePath)
        if not self.projectPath:
            return
        super().__post_init__()
        self.fileList = self.inventory[self.index][self.sourcePath]['fileList']
        if not self.lookup:
            for dir,_,files in os.walk(self.sourcePath):
                subDir = os.path.relpath(dir,self.sourcePath)
                self.fileList += [[os.path.join(subDir,f),False] for f in files 
                    if f.endswith(self.fileExt)
                    and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
                    and sum([e in f for e in self.excludePattern]) == 0
                    and [os.path.join(subDir,f),False] not in self.fileList
                    and [os.path.join(subDir,f),True] not in self.fileList
                    ]
                
            self.inventory[self.index][self.sourcePath]['fileList'] = self.fileList
            self.save(True)
        # if self.read:
            print(self.inventory)
               
