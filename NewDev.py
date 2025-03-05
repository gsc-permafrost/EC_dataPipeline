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
from typing import Literal
from dataclasses import dataclass, field

@dataclass(kw_only=True)
class metadataRecord:
    fillChar = '_'
    sepChar = '-'
    safeName: bool = field(default=True,repr=False)
    # Formats a metadata entry for either a full site or a specific measurement
    def __post_init__(self):
        if self.safeName == True:
            for k,v in self.__dataclass_fields__.items():
                if self.__dict__[k] is not None and v.kw_only and v.type == str:
                    if self.__dataclass_fields__[k].repr:
                        self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.fillChar, self.__dict__[k])
                    elif 'date' in k:
                        self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.sepChar, self.__dict__[k])
        self.ID = self.sepChar.join([str(self.__dict__[k]) for k in self.__dataclass_fields__.keys() 
                                       if self.__dataclass_fields__[k].repr])
        self.nestDepth = len([1 for k in self.__dataclass_fields__.keys() 
                                       if self.__dataclass_fields__[k].repr])

        self.record = {}
        for f,v in self.__dataclass_fields__.items():
            if f not in metadataRecord.__dataclass_fields__.keys():
                if self.safeName:
                    self.record.setdefault(self.ID,{}).setdefault(f,self.__dict__[f])
                else:
                    self.record.setdefault(f,self.__dict__[f])

@dataclass(kw_only=True)
class observation(metadataRecord):
    fillChar = '_'
    sepChar = '_'
    # Metadata associated with a single trace
    variableName: str = None
    originalName: str = field(repr=False)
    ignore: bool = field(default=False,repr=False)
    unit: str = field(default=None,repr=False)
    dtype: str = field(default=None,repr=False)
    variableDescription: str = field(default=None,repr=False)
    # positional variables (vertical, horizontal, replicate)
    vertical: int = 1
    horizontal: int = 1
    replicate: int = 1
    def __post_init__(self):
        if self.variableName is None:
            self.variableName = self.originalName
        super().__post_init__()


@dataclass(kw_only=True)
class siteRecord(metadataRecord):
    fillChar = '_'
    sepChar = '-'
    siteID: str = '.example'
    description: str = field(default=None,repr=False)
    name: str = field(default=None,repr=False)
    PI: str = field(default=None,repr=False)
    dateEstablished: str = field(default=None,repr=False)
    landCoverType: str = field(default=None,repr=False)
    longitude: float = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)
    def __post_init__(self):
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        super().__post_init__()

class siteInventory(siteRecord):
    def __init__(self,source,overwrite=False,**kwargs):
        si = siteRecord()
        self.nestDepth = si.nestDepth
        self.siteInventory = si.record
        self.overwrite = overwrite
        self.load(source)
        if kwargs != {}:
            if 'siteID' in kwargs and kwargs['siteID'] is not None:
                self.updateInventory(kwargs)
        self.save(source)

    def load(self,source):
        if os.path.isfile(os.path.join(source,'siteInventory.yml')):
            si = helperFunctions.loadDict(os.path.join(source,'siteInventory.yml'))
            if si is not None:
                self.siteInventory = helperFunctions.unpackDict(si,'-',limit=self.nestDepth-1)
        if os.path.isfile(os.path.join(source,'siteInventory.csv')):
            df = pd.read_csv(os.path.join(source,'siteInventory.csv'),index_col=[0])
            df = df.fillna(np.nan).replace([np.nan], [None])
            for ix,row in df.iterrows():
                if ix not in self.siteInventory.keys():
                    self.updateInventory(row.to_dict())
    
    def updateInventory(self,kwargs):
        args = {}
        for k,v in kwargs.items():
            if k in self.__dataclass_fields__.keys():
                args[k] = v
        si = siteRecord(**args)
        self.nestDepth = si.nestDepth
        while si.ID in self.siteInventory.keys() and self.overwrite == True:
            si.replicateID+=1
            args['replicateID'] = self.record.replicateID
            si = siteRecord(**args)
        if si.ID not in self.siteInventory.keys() or self.overwrite:
            self.siteInventory = helperFunctions.updateDict(self.siteInventory,si.record,overwrite=self.overwrite)
        if list(siteRecord().record.keys())[0] in self.siteInventory.keys():
            self.siteInventory.pop(list(siteRecord().record.keys())[0])

    def save(self,source):
        with open(os.path.join(source,'siteInventory.yml'),'w+') as f:
            # Sort alphabetically by ID, maintaining order of metadata
            # Hacked up to maintain desirable sort order regardless of case or pattern used to represent "blank" values
            self.siteInventory = {key.replace(self.fillChar,' '):value for key,value in self.siteInventory.items()}
            self.siteInventory = {key.replace(' ',self.fillChar):value for key,value in dict(sorted(self.siteInventory.items())).items()}
            self.siteInventory = helperFunctions.packDict(copy.deepcopy(self.siteInventory),'-',limit=self.nestDepth-1,order=1)
            yaml.safe_dump(self.siteInventory,f,sort_keys=False)
        self.siteInventory = helperFunctions.unpackDict(self.siteInventory,'-',limit=self.nestDepth-1)
        index,data = [k for k in self.siteInventory.keys()],[v for v in self.siteInventory.values()]
        df = pd.DataFrame(data = self.siteInventory.values(), index = index)
        df.to_csv(os.path.join(source,'siteInventory.csv'))

@dataclass
class database:
    projectPath: str = field(repr=False)
    siteID: str = field(default=None,repr=False)
    overwrite: bool = field(default=False,repr=False)
    verbose: bool = field(default=True,repr=False)
    metadataFile: dict = field(default_factory=lambda:helperFunctions.loadDict(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
        ,repr=False)
    
    def __post_init__(self):
        try:
            super().__post_init__()
        except:
            pass
        if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
            self.metadataFile['.dateCreated'] = self.now()
            self.metadataFile['.dateModified'] = self.now()
            for d in self.metadataFile['directoryStructure']:
                os.makedirs(os.path.join(self.projectPath,d))
        elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')):
           sys.exit('Non-empty, non-project directory provided')
        else:
            self.metadataFile = helperFunctions.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
            self.metadataFile['.dateModified'] = self.now()

        self._siteInventory(overwrite=self.overwrite)
        with open(os.path.join(self.projectPath,'projectInfo.yml'),'w+') as file:
            yaml.safe_dump(self.metadataFile,file)

    def _siteInventory(self,**kwargs):
        self.siteInventory = siteInventory(os.path.join(self.projectPath,'metadata'),**kwargs)
        for siteID in self.siteInventory.siteInventory:
            site = os.path.join(self.projectPath,'sites',siteID)
            if not os.path.isdir(site) and not siteID.startswith('.'):
                os.makedirs(site)
                with open(os.path.join(site,'sourceFileInventory.yml'),'w+') as file:
                    yaml.safe_dump({},file)

            
    # def makeDatabase(self):
    #     if self.verbose:print('Initializing empty database')
    #     if not os.path.isdir(self.projectPath):
    #         os.makedirs(self.projectPath)
    #         if self.verbose:print('Creating: ',self.mdPath)
    #     if not os.path.isdir(self.mdPath):
    #         os.mkdir(self.mdPath)
    #         if self.verbose:print('Creating: ',self.mdPath)
    #     self.metadataFile['Date_created'] = now
    #     self.metadataFile['Last_modified'] = now
    #     self.logFile = 'Database Created: ' + now + '\n'
    #     with open(self._metadataFile,'w+') as file:
    #         if self.verbose:print('Creating: ',self._metadataFile)
    #         yaml.safe_dump(self.metadataFile,file,sort_keys=False)
    #     with open(self._logFile,'w+') as file:
    #         if self.verbose:print('Creating: ',self._logFile)
    #         file.write(self.logFile)

    def now(self):
        return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    # def openDatabase(self):
    #     metadata = helperFunctions.loadDict(self._metadataFile,self.verbose)
    #     if sum(k not in metadata.keys() for k in self.metadataFile.keys()):
    #         sys.exit('Database metadata are corrupted')
    #     self.metadataFile = metadata
    #     with open(self._logFile) as file:
    #         self.logFile = file.read()

@dataclass(kw_only=True)
class rawDatabaseImport(database):
    stage = 'raw'
    measurementID: str
    dataIn: pd.DataFrame
    metadataIn: dict
    mode: Literal['fill','overwrite'] = 'fill'
    
    def __post_init__(self):
        super().__post_init__()
        keep = []
        for trace,details in self.metadataIn['Variables'].items():
            print(trace,details)
        #     if not details['ignore']: keep.append(trace)
        # self.dataIn = self.dataIn[keep].copy()
        # for y in self.dataIn.index.year.unique():
        #     if '-' in self.measurementID:

        #         siteID,subsiteID = self.measurementID.split('-',1)
        #         dbpth = os.path.join(self.projectPath,str(y),siteID,self.stage,subsiteID)
        #     else:
        #         dbpth = os.path.join(self.projectPath,str(y),self.measurementID,self.stage)
        #     print(dbpth)
