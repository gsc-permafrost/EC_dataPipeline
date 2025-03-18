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
    sepChar = '/'*2
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
                self.save()
    
    def make(self):
        self.metadataFile['dateCreated'] = self.now()
        self.metadataFile['dateModified'] = self.now()
        for d in self.metadataFile['directoryStructure']:
            os.makedirs(os.path.join(self.projectPath,d))
        self.save()

    def now(self,fmt='%Y-%m-%d %H:%M:%S'):
        return(datetime.datetime.now().strftime(fmt))
    
    def save(self,inventory=None,filename=None):

        self.metadataFile['dateModified'] = self.now()
        helperFunctions.saveDict(self.metadataFile,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)
        if filename:
            print('Saving: ',filename)
            helperFunctions.saveDict(inventory,os.path.join(self.projectPath,self.mapSubdir(self.subDir),filename))
        
    def validate(self):
        if self.verbose: print(f'Validating metadta in: ',self.projectPath)
        for key,values in helperFunctions.loadDict(os.path.join(self.projectPath,'metadata','siteInventory.yml')).items():
            siteInventory(projectPath=self.projectPath,overwrite=True,verbose=self.verbose,**values)
            mI = helperFunctions.loadDict(os.path.join(self.projectPath,'metadata',values['siteID'],'measurementInventory.yml'))
            mI = helperFunctions.packDict(helperFunctions.unpackDict(mI,format=self.sepChar),limit=1,format=self.sepChar)
            sI = helperFunctions.loadDict(os.path.join(self.projectPath,'metadata',values['siteID'],'searchInventory.yml'))
            for ke,vals in mI.items():
                measurementInventory(projectPath=self.projectPath,overwrite=True,verbose=self.verbose,siteID=values['siteID'],**vals)
                s = helperFunctions.findNestedValue(ke,sI,delimiter=self.sepChar)
                if s:
                    s = helperFunctions.packDict(helperFunctions.unpackDict(s,format=self.sepChar),limit=1,format=self.sepChar)
                    kl = sorted(list(s.keys()))
                    for k,v in s.items():
                        v = vals|v
                        s = searchInventory(projectPath=self.projectPath,overwrite=True,verbose=self.verbose,siteID=values['siteID'],**v)

@dataclass(kw_only=True)
class metadataRecord(database):
    index: str = field(default=None,repr=False)
    subIndex: list = field(default=None,repr=False)
    projectPath: str = field(default=None,repr=False)
    overwrite: bool = field(default=False,repr=False)
    verbose: bool = field(default=False,repr=False)

    def __post_init__(self):
        super().__init__()
        if not self.projectPath:
            return
        # Define the index for new records 
        if not self.index:
            self.index=(self.sepChar).join([self.safeFmt(k) for k,v in self.__dataclass_fields__.items() if v.metadata == 'ID' and self.__dict__[k]])

        self.validateCoordinates()
        # load the parent elements
        parents = [p for p in type(self).mro() if p not in type(metadataRecord()).mro() and p.__name__ != type(self).__name__]
        for parent in parents:
            print('chou Pi')
            src = os.path.join(self.projectPath,self.mapSubdir(parent().subDir),parent.__name__+'.'+parent().ext)
            tmp = helperFunctions.loadDict(src)
            tmpIx = self.sepChar.join([self.safeFmt(k) for k,v in parent().__dataclass_fields__.items() if v.metadata == 'ID' and self.__dict__[k]])
            if tmpIx:
                self.load(helperFunctions.findNestedValue(tmpIx,tmp,self.sepChar))
            else:
                for v in self.mapSubdir(self.subDir,path=False).values():
                    if v in tmp.keys():
                        self.load(tmp[v])
        # load the inventory for the child
        src = os.path.join(self.projectPath,self.mapSubdir(self.subDir),type(self).__name__+'.'+self.ext)
        self.inventory = helperFunctions.loadDict(src)

    
        # get relevant "user-facing" attributes for the specific record
        self.public = helperFunctions.baseFields(self,repr=True)
        rec = {r:self.__dict__[r] for r in self.public}
        helperFunctions.updateDict(self.inventory,helperFunctions.defaultNest(self.index.split(self.sepChar)[::-1],rec),self.overwrite,self.verbose)

    def load(self,tmp=None):
        if tmp:
            for key,val in tmp.items():
                if self.__dict__[key] is None and not self.overwrite:
                    self.__dict__[key] = val

    def safeFmt(self,k):
        if type(self.__dict__[k]) != list and not k.endswith('Path'):
            safeName = re.sub('[^0-9a-zA-Z_]+',self.fillChar, str(self.__dict__[k]))
            if self.__dict__[k] != safeName:
                if self.verbose:print('Unsafe name: ',self.__dict__[k],' \nConverting to: ',safeName)
                self.__dict__[k] = safeName
            return(self.__dict__[k])
        elif type(self.__dict__[k]) == list:
            return(self.fillChar.join([j for j in self.__dict__[k]]))            
        else:
            return(self.__dict__[k])

    def mapSubdir(self,items,path=True):
        if path:
            return(os.path.sep.join([self.__dict__[s] if s in self.__dict__.keys() else s for s in items]))
        else:
            return({s:self.__dict__[s] for s in items if s in self.__dict__.keys() })
        
    def validateCoordinates(self):
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            # print(coordinates)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']

    def popSubsets(self,fn):
        self.inventory = helperFunctions.unpackDict(self.inventory,self.sepChar,limit=len(self.index.split(self.sepChar))-1)
        print(self.index)
        keys = list(self.inventory[self.index].keys())
        print(keys)
        for key in keys:
            if type(self.inventory[self.index][key]) is dict:
                subSet = self.inventory[self.index].pop(key)
                fn = os.path.join(self.projectPath,self.mapSubdir(self.subDir),os.path.sep.join(self.index.split(self.sepChar)),fn)
                self.save(subSet,fn)

@dataclass(kw_only=True)
class test:
    A: int = 1

@dataclass(kw_only=True)
class samplePoint(metadataRecord):
    pointID: str = None
    description: str = None
    latitude: float = None
    longitude: float = None
    measurements: dict = field(default_factory=lambda:test().__dict__)
    fileName: str = field(default='samplePoints.yml',repr=False)

    def __post_init__(self):
        self.validateCoordinates()
        self.record = {k:v for k,v in self.__dict__.items() if self.__dataclass_fields__[k].repr}

@dataclass(kw_only=True)
class siteInventory(metadataRecord):
    ext = 'yml'
    subDir = ['metadata']
    siteID: str = field(default=None,metadata='ID')
    description: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    samplePoints: dict = field(default_factory=lambda:{'None':samplePoint(pointID='None').record})

    def __post_init__(self,main=True):
        if not self.projectPath:
            return
        self.samplePoints = self.validatesamplePoints()
        super().__post_init__()
        if main:
            self.popSubsets(samplePoint().fileName)
            self.save(self.inventory,type(self).__name__+'.'+self.ext)
    
    def validatesamplePoints(self):
        validated = {}
        if 'pointID' in self.samplePoints:
            tmp = samplePoint(**self.samplePoints)
            validated[tmp.pointID] = tmp.record
        else:
            for k,v in self.samplePoints.items():
                tmp = samplePoint(**v)
                validated[tmp.pointID] = tmp.record
        return(validated)
    
@dataclass(kw_only=True)
class sourceInventory(metadataRecord):
    sourceID: int = None
    sourcePath: str = None
    fileExt: str = None
    matchPattern: list = field(default_factory=lambda:[])
    excludePattern: list = field(default_factory=lambda:[])
    fileName: str = field(default='sourceFileList.json',repr=False)

    def __post_init__(self):
        if not self.sourceID:
            self.sourceID = self.now()
        for f,v in self.__dataclass_fields__.items():
            if type(self.__dict__[f]) is not list and v.type is list:
                self.__dict__[f] = [self.__dict__[f]]
        self.record = {k:v for k,v in self.__dict__.items() if self.__dataclass_fields__[k].repr}
        
@dataclass(kw_only=True)
class measurementInventory(siteInventory):
    ext = 'yml'
    subDir = ['metadata','siteID']
    siteID: str = None
    pointID: str = field(default=None,metadata='ID')
    measurementID: str = field(default=None,metadata='ID')
    fileType: str = field(default=None,metadata='ID')
    baseFrequency: str = None
    description: str = None
    sourceInventory: str = field(default_factory=lambda:lambda:{'None':samplePoint(sourceInventory='None').record})

    def __post_init__(self,main=True):
        print('Mi')
        if not self.projectPath:
            return
        self.sourceInventory = self.validateSources()
        super().__post_init__(main=False)
        if main:
            self.popSubsets(sourceInventory().fileName)
            self.save(self.inventory,type(self).__name__+'.'+self.ext)
    
    def validateSources(self):
        validated = {}
        if 'sourcePath' in self.sourceInventory:
            tmp = sourceInventory(**self.sourceInventory)
            validated[tmp.sourceID] = tmp.record
        else:
            for k,v in self.sourceInventory.items():
                tmp = sourceInventory(**v)
                validated[tmp.sourceID] = tmp.record
        self.findNewSourceFiles()
        return(validated)

    def findNewSourceFiles(self):
        print('doSomething')
        # filename = 'sourceFiles.json'
        # self.sourceFiles = helperFunctions.loadDict(os.path.join(self.projectPath,'metadata',self.siteID,filename))
        # if not helperFunctions.findNestedValue(self.index,self.sourceFiles,self.sepChar):
        #     self.sourceFiles = helperFunctions.defaultNest(self.index.split(self.sepChar)[::-1],seed = [[]])
        # fileList = helperFunctions.findNestedValue(self.index,self.sourceFiles,self.sepChar)
        # for dir,_,files in os.walk(self.sourcePath):
        #     subDir = os.path.relpath(dir,self.sourcePath)
        #     fileList += [[os.path.join(subDir,f),False] for f in files 
        #         if f.endswith(self.fileExt)
        #         and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
        #         and sum([e in f for e in self.excludePattern]) == 0
        #         and [os.path.join(subDir,f),False] not in fileList
        #         and [os.path.join(subDir,f),True] not in fileList
        #         ]
        # self.save(self.inventory,filename)  

@dataclass(kw_only=True)
class searchInventory(measurementInventory):
    ext = 'yml'
    subDir = ['metadata','siteID']
    sourcePath: str = field(default=None,metadata='ID')
    fileExt: str = field(default=None,metadata='ID')
    matchPattern: list = field(default_factory=lambda:[],metadata='ID')
    excludePattern: list = field(default_factory=lambda:[],metadata='ID')

    def __post_init__(self):
        if not self.projectPath:
            return
        if self.sourcePath:
            self.sourcePath = os.path.abspath(self.sourcePath)
        for f,v in self.__dataclass_fields__.items():
            if type(self.__dict__[f]) is not list and v.type is list:
                self.__dict__[f] = [self.__dict__[f]]
        super().__post_init__()

    def findNewSourceFiles(self):
        filename = 'sourceFiles.json'
        self.sourceFiles = helperFunctions.loadDict(os.path.join(self.projectPath,'metadata',self.siteID,filename))
        if not helperFunctions.findNestedValue(self.index,self.sourceFiles,self.sepChar):
            self.sourceFiles = helperFunctions.defaultNest(self.index.split(self.sepChar)[::-1],seed = [[]])
        fileList = helperFunctions.findNestedValue(self.index,self.sourceFiles,self.sepChar)
        for dir,_,files in os.walk(self.sourcePath):
            subDir = os.path.relpath(dir,self.sourcePath)
            fileList += [[os.path.join(subDir,f),False] for f in files 
                if f.endswith(self.fileExt)
                and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
                and sum([e in f for e in self.excludePattern]) == 0
                and [os.path.join(subDir,f),False] not in fileList
                and [os.path.join(subDir,f),True] not in fileList
                ]
        self.save(self.inventory,filename)


        # print(self.sourceFiles)
        # print()
        
# @dataclass(kw_only=True)
# class sourcefileInventory(measurementInventory):
#     ext = 'yml'
#     subDir = ['metadata','siteID']
#     sourcePath: str = field(default=None,repr=False,metadata='subID')
#     fileExt: str = field(default=None,repr=False,metadata='subID')
#     matchPattern: tuple = field(default_factory=lambda:(),repr=False,metadata='subID')
#     excludePattern: tuple = field(default_factory=lambda:(),repr=False,metadata='subID')
#     fileList: list = field(default_factory=lambda:[])
#     lookup: bool = field(default=False,repr=False)
#     read: bool = field(default=True,repr=False)

#     def __post_init__(self):
#         if not self.projectPath:
#             return
#         if self.sourcePath:
#             self.sourcePath = os.path.abspath(self.sourcePath)
#         for f,v in self.__dataclass_fields__.items():
#             if type(self.__dict__[f]) is not tuple and v.type is tuple:
#                 if type(self.__dict__[f]) is list:
#                     self.__dict__[f] = tuple(self.__dict__[f])
#                 else:
#                     self.__dict__[f] = tuple([self.__dict__[f]])
#         super().__post_init__()


        # self.fileList = self.inventory[self.index][self.sourcePath]['fileList']
        # if not self.lookup:
        #     for dir,_,files in os.walk(self.sourcePath):
        #         subDir = os.path.relpath(dir,self.sourcePath)
        #         self.fileList += [[os.path.join(subDir,f),False] for f in files 
        #             if f.endswith(self.fileExt)
        #             and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
        #             and sum([e in f for e in self.excludePattern]) == 0
        #             and [os.path.join(subDir,f),False] not in self.fileList
        #             and [os.path.join(subDir,f),True] not in self.fileList
        #             ]
                
        #     self.inventory[self.index][self.sourcePath]['fileList'] = self.fileList
        #     self.save(self.inventory,type(self).__name__+'.'+self.ext)
        # # if self.read:
               
