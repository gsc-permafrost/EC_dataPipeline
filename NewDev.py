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


fillChar = '_'
sepChar = '-'

def loadYAML(fpath,verbose=False):
    try:
        with open(fpath,'r') as f:
            return(yaml.safe_load(f))
    except:
        if not os.path.isfile(fpath):
            if verbose:print('Does not exist: ',fpath)
            return({})
        else:
            if os.path.isfile(fpath):print('Could not load: ',fpath)
            return(None)

@dataclass(kw_only=True)
class site:
    siteID: str = '.example'
    measurementID: str = '.site'
    replicateID: int = 1
    description: str = field(default=None,repr=False)
    name: str = field(default=None,repr=False)
    PI: str = field(default=None,repr=False)
    dateEstablished: str = field(default=None,repr=False)
    landCoverType: str = field(default=None,repr=False)
    longitude: float = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)

    def __post_init__(self):
        for k,v in self.__dataclass_fields__.items():
            if self.__dict__[k] is not None and v.kw_only and v.type == str:
                if self.__dataclass_fields__[k].repr:
                    self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',fillChar, self.__dict__[k])
                elif 'date' in k:
                    self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',sepChar, self.__dict__[k])
        if self.latitude is not None and self.longitude is not None:
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        self.ID = sepChar.join([str(self.__dict__[k]) for k in self.__dataclass_fields__.keys() 
                                       if self.__dataclass_fields__[k].repr])
        self.entry = {}
        for f,v in self.__dataclass_fields__.items():
            self.entry.setdefault(self.ID,{}).setdefault(f,self.__dict__[f])

@dataclass
class metadataRecord(site):
    # Formats a metadata entry for either a full site or a specific measurement
    def __post_init__(self):
        super().__post_init__()
        self.entry = {}
        for f,v in self.__dataclass_fields__.items():
            self.entry.setdefault(self.ID,{}).setdefault(f,self.__dict__[f])

class siteInventory(metadataRecord):
    def __init__(self,source,overwrite=False,**kwargs):
        self.siteInventory = metadataRecord().entry
        self.overwrite = overwrite
        self.load(source)
        if kwargs != {}:
            self.updateInventory(kwargs)
        self.save(source)

    def load(self,source):
        if os.path.isfile(os.path.join(source,'siteInventory.yml')):
            si = loadYAML(os.path.join(source,'siteInventory.yml'))
            if si is not None:
                self.siteInventory = helperFunctions.unpackDict(si,'-',limit=2)
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
        self.record = metadataRecord(**args)
        while self.record.ID in self.siteInventory.keys():
            self.record.replicateID+=1
            args['replicateID'] = self.record.replicateID
            self.record = metadataRecord(**args)
        self.siteInventory = helperFunctions.updateDict(self.siteInventory,self.record.entry,overwrite=self.overwrite)
        if list(metadataRecord().entry.keys())[0] in self.siteInventory.keys():
            self.siteInventory.pop(list(metadataRecord().entry.keys())[0])

    def save(self,source):
        with open(os.path.join(source,'siteInventory.yml'),'w+') as f:
            # Sort alphabetically by ID, maintaining order of metadata
            # Hacked up to maintain desirable sort order regardless of case or pattern used to represent "blank" values
            self.siteInventory = {key.replace(fillChar,' '):value for key,value in self.siteInventory.items()}
            self.siteInventory = {key.replace(' ',fillChar):value for key,value in dict(sorted(self.siteInventory.items())).items()}
            self.siteInventory = helperFunctions.packDict(copy.deepcopy(self.siteInventory),'-',limit=2,order=1)
            yaml.safe_dump(self.siteInventory,f,sort_keys=False)
        self.siteInventory = helperFunctions.unpackDict(self.siteInventory,'-',limit=2)
        index,data = [k for k in self.siteInventory.keys()],[v for v in self.siteInventory.values()]
        df = pd.DataFrame(data = self.siteInventory.values(), index = index)
        df.to_csv(os.path.join(source,'siteInventory.csv'))

@dataclass
class database:
    projectPath: str
    overwrite: bool = False
    verbose: bool = True
    logFile: str = ''
    metadataFile: dict = field(default_factory=lambda:loadYAML(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
        )
    
    def __post_init__(self):
        self.mdPath = os.path.join(self.projectPath,'metadata')
        self._metadataFile = os.path.join(self.mdPath,"_metadataFile.yml")
        self._logFile = os.path.join(self.mdPath,'_logFile.txt')
        if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
            self.makeDatabase()
        elif not os.path.isfile(self._metadataFile) and os.listdir(self.projectPath):
           sys.exit('Non-empty, non-project directory provided')
        else:
            self.openDatabase()
        self._siteInventory(overwrite=self.overwrite)

    def _siteInventory(self,**kwargs):
        self.siteInventory = siteInventory(self.mdPath,**kwargs)
            
    def makeDatabase(self):
        if self.verbose:print('Initializing empty database')
        if not os.path.isdir(self.projectPath):
            os.makedirs(self.projectPath)
            if self.verbose:print('Creating: ',self.mdPath)
        if not os.path.isdir(self.mdPath):
            os.mkdir(self.mdPath)
            if self.verbose:print('Creating: ',self.mdPath)
        now = self.now()
        self.metadataFile['Date_created'] = now
        self.metadataFile['Last_modified'] = now
        self.logFile = 'Creating Database: ' + now + '\n'
        print(self._metadataFile)
        with open(self._metadataFile,'w+') as file:
            if self.verbose:print('Creating: ',self._metadataFile)
            yaml.safe_dump(self.metadataFile,file,sort_keys=False)
        with open(self._logFile,'w+') as file:
            if self.verbose:print('Creating: ',self._logFile)
            file.write(self.logFile)

    def now(self):
        return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def openDatabase(self):
        metadata = loadYAML(self._metadataFile,self.verbose)
        if sum(k not in metadata.keys() for k in self.metadataFile.keys()):
            sys.exit('Database metadata are corrupted')
        self.metadataFile = metadata
        with open(self._logFile) as file:
            self.logFile = file.read()

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
