import re
import os
import yaml
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field
import helperFunctions
import NewDev as ND
import importlib
importlib.reload(ND)

@dataclass(kw_only=True)
class fileInventory(ND.database,ND.metadataRecord):
    siteID: str = field(repr=False)
    measurementType: str
    loggerID: str
    source: str = field(repr=False)
    ext: str = field(default='',repr=False)
    matchPattern: list = field(default_factory=lambda:[],repr=False)
    excludePattern: list = field(default_factory=lambda:[],repr=False)

    def __post_init__(self):
        # super(ND.metadataRecord, self).__post_init__()
        print(type(self).mro())
        super().__post_init__()
        # super().
        print(self.ID)
        print(self.siteInventory.siteInventory)
        # sFI = os.path.join(self.projectPath,'sites',self.siteID,'sourceFileInventory.yml')
        # fileInventory = helperFunctions.loadDict(sFI)
        # self.measurementID = self.measurementType.replace(' ','_')+'-'+self.loggerID.replace(' ','_')
        # self.source = os.path.abspath(self.source)
        # fileInventory.setdefault(self.siteID,{}).setdefault(self.measurementID,{}).setdefault(self.source,{'sourceFiles':[]})
        # for f,v in self.__dataclass_fields__.items():
        #     if type(self.__dict__[f]) is not list and v.type is list:
        #         self.__dict__[f] = [self.__dict__[f]]
        # for dir,_,files in os.walk(self.source):
        #     subDir = os.path.relpath(dir,self.source)
        #     fileInventory[self.siteID][self.measurementID][self.source]['sourceFiles'] += [os.path.join(subDir,f) for f in files 
        #         if f.endswith(self.ext)
        #         and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
        #         and sum([e in f for e in self.excludePattern]) == 0]
        
        # with open(sFI,'w+') as file:
        #     yaml.safe_dump(fileInventory,file,sort_keys=False)



@dataclass(kw_only=True)
class genericLoggerFile:
    varDeffs = 'variableDefinitions'
    source: str = field(repr=False)
    siteID: str
    measurementType: str
    loggerID: str
    Metadata: dict = field(default_factory=lambda:{},repr=False)
    verbose: bool = field(default=False,repr=False)
    loggerName: str = None
    timeZone: str = 'UTC'
    frequency: str = '30min'
    Data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def __post_init__(self):
        if self.Metadata != {}:
            if type(self.Metadata) == str and os.path.isfile(self.Metadata):
                self.Metadata = helperFunctions.loadDict(self.Metadata)
            elif type(self.Metadata) != dict:
                return
            self.updateMetadata()
        else:
            self.newMetadata()
        super().__post_init__()
    
    def updateMetadata(self):
        print(self.Metadata[self.varDeffs])
        keys = list(self.Metadata[self.varDeffs].keys())
        for k in keys:
            args = self.Metadata[self.varDeffs].pop(k)
            obs = ND.observation(**args)
            helperFunctions.updateDict(self.Metadata[self.varDeffs],obs.record)

    def newMetadata(self):
        for k,v in self.__dataclass_fields__.items():
            if v.repr and v.type != type(pd.DataFrame()) and k not in ND.database.__dataclass_fields__.keys():
                self.Metadata[v.name] = self.__dict__[v.name]     
        self.Metadata[self.varDeffs] = {}
        for col in self.Data.columns:
            obs = ND.observation(originalName=col,dtype=self.Data[col].dtype.str,safeName=False)
            self.Metadata[self.varDeffs][col] = obs.record

@dataclass(kw_only=True)
class hoboCSV(genericLoggerFile,ND.database):
    fileType = "HoboCSV"
    timestamp: str = field(default="Date Time",repr=False)
    yearfirst: bool = field(default=True,repr=False)
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'], repr=False)
    dropCols: list = field(default_factory=lambda:['#'],repr=False)

    def __post_init__(self):
        rawFile = open(self.source,'r',encoding='utf-8-sig')
        T = rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        self.Data = pd.read_csv(rawFile)
        #Parse the datetime index
        Timestamp = self.Data.columns[self.Data.columns.str.contains(self.timestamp)].values
        self.Data.index = self.Data[Timestamp].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
        self.Data = self.Data.drop(columns=Timestamp)
        #Parse the status variables
        self.statusCols = self.Data.columns[self.Data.columns.str.contains('|'.join(self.statusCols))].values
        self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
        self.Data = self.Data.loc[keep].copy()
        self.Data = self.Data.drop(columns=self.statusCols)
        #Remove any other undesirable data
        self.Data = self.Data.drop(columns=self.dropCols)

        super().__post_init__()