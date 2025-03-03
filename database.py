import os
import sys
import yaml
import shutil
import deepdiff
import datetime
import numpy as np
import pandas as pd
from typing import Literal
from dataclasses import dataclass,field

from rawDataFile import genericLoggerFile


def loadYAML(fpath,verbose=False):
    try:
        with open(fpath,'r') as f:
            return(yaml.safe_load(f))
    except:
        if os.path.isfile:print('Could not load: ',fpath)
        elif verbose:print('Does not exist: ',fpath)
        return(None)

@dataclass(kw_only=True)
class database:
    projectPath: str
    verbose: bool = True
    logFile: str = ''
    metadataFile: dict = field(default_factory=lambda:loadYAML(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml'))
        )
    
    def __post_init__(self):
        self._metadataFile = os.path.join(self.projectPath,"_metadataFile.yml")
        self._logFile = os.path.join(self.projectPath,'_logFile.txt')
        if not os.path.isdir(self.projectPath):
            self.makeDatabase()
        elif not os.path.isfile(self._metadataFile) and os.listdir(self.projectPath):
           sys.exit('Non-empty, non-project directory provided')
        else:
            self.openDatabase()
            
    def makeDatabase(self):
        if self.verbose:print('Initializing empty database')
        os.makedirs(self.projectPath)
        now = self.now()
        self.metadataFile['Date_created'] = now
        self.metadataFile['Last_modified'] = now
        self.logFile = 'Creating Database: ' + now + '\n'
        with open(self._metadataFile,'w+') as file:
            if self.verbose:print('Creating folder: ',self._metadataFile)
            yaml.safe_dump(self.metadataFile,file,sort_keys=False)
        with open(self._logFile,'w+') as file:
            if self.verbose:print('Creating file: ',self._logFile)
            file.write(self.logFile)

    def now(self):
        return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


    def openDatabase(self):
        metadata = loadYAML(self._metadataFile)
        if sum(k not in metadata.keys() for k in self.metadataFile.keys()):
            sys.exit('Database metadata are corrupted')
        self.metadataFile = metadata
        with open(self._logFile) as file:
            self.logFile = file.read()

# @dataclass
# class database:
#     # Declare the defaults for a database
#     verbose: bool = True
#     isDatabase: bool = False
#     projectPath: str = None
#     logFile: str = ''
#     siteID: list = field(default_factory=list)
#     level: list = field(default_factory=lambda:['raw','clean'])
#     Years: list = field(default_factory=lambda:[datetime.datetime.now().year])
#     metadata: dict = field(default_factory=load)

#     def __post_init__(self):
#         # self.Years = [str(Y) for Y in self.Years]
#         self._metadataFile = os.path.join(self.projectPath,"_metadata.yml")
#         self._logFile = os.path.join(self.projectPath,'_logFile.txt')
#         # self._map = {os.path.join(Y,siteID,level):[]
#         #             for Y in self.Years
#         #             for siteID in self.siteID
#         #             for level in self.level}
#         if not os.path.isdir(self.projectPath):
#             self.makeDatabase()
#         elif not os.path.isfile(self._metadataFile) and os.listdir(self.projectPath):
#            sys.exit('Non-empty, non-project directory provided')
#         else:
#             self.openDatabase()
#         self.isDatabase = True
        
#     def now(self):
#         return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#     def makeDatabase(self):
#         if self.verbose:print('Initializing empty database')
#         os.makedirs(self.projectPath)
#         now = self.now()
#         self.metadataFile['Date_created'] = now
#         self.metadataFile['Last_modified'] = now
#         self.logFile = 'Creating Database: ' + now + '\n'
#         # for dbpath in self._map:
#         #     newFolder = os.path.join(self.projectPath,dbpath)
#         #     os.makedirs(newFolder)
#         #     if self.verbose:print('Creating folder: ',newFolder)
#         with open(self._metadataFile,'w+') as file:
#             if self.verbose:print('Creating folder: ',self._metadataFile)
#             yaml.safe_dump(self.metadataFile,file,sort_keys=False)
#         with open(self._logFile,'w+') as file:
#             if self.verbose:print('Creating file: ',self._logFile)
#             file.write(self.logFile)

#     def openDatabase(self):
#         with open(self._metadataFile) as file:
#             metadata = yaml.safe_load(file)
#             if sum(k not in metadata.keys() for k in self.metadataFile.keys()):
#                 sys.exit('Database metadata are corrupted')
#             self.metadataFile = metadata
#         with open(self._logFile) as file:
#             self.logFile = file.read()
    
#     def readDatabaseFolder(self,dbpth,filter = None):
#         files = [f for f in os.listdir(dbpth) if '.' not in f and (filter is None or f in filter and f)]
#         data = {f:np.fromfile(os.path.join(dbpth,f),dtype=self.metadataFile['defaultFormat'][f]['dtype']) if f in self.metadataFile['defaultFormat'].keys() 
#                 else np.fromfile(os.path.join(dbpth,f),dtype=self.metadataFile['defaultFormat']['data']['dtype'])
#                 for f in files}
#         df = pd.DataFrame(data = {f:np.fromfile(os.path.join(dbpth,f),dtype=self.metadataFile['defaultFormat'][f]['dtype']) if f in self.metadataFile['defaultFormat'].keys() 
#                 else np.fromfile(os.path.join(dbpth,f),dtype=self.metadataFile['defaultFormat']['data']['dtype'])
#                 for f in files}
#                 )
#         if not df.empty:
#             df.index=pd.to_datetime(df['POSIX_timestamp'],unit='s')
#         metadataFile = os.path.join(dbpth,'_metadata.yml')            
#         if os.path.isfile(metadataFile):
#             with open(metadataFile,'r') as file:
#                 metadataFile = yaml.safe_load(file)
#         else:
#             metadataFile = None
#         return(df,metadataFile)

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
            if not details['ignore']: keep.append(trace)
        self.dataIn = self.dataIn[keep].copy()
        for y in self.dataIn.index.year.unique():
            if '-' in self.measurementID:
                siteID,subsiteID = self.measurementID.split('-',1)
                dbpth = os.path.join(self.projectPath,str(y),siteID,self.stage,subsiteID)
            else:
                dbpth = os.path.join(self.projectPath,str(y),self.measurementID,self.stage)
            print(dbpth)

        
#     def rawDatabaseImport(self,dataIn,metadataIn = None,stage='raw',mode='fill'):
#         if metadataIn is None:
#             print('Warning! No metadata provided, using default template dataset')
#             genericData = genericLoggerFile(data=dataIn)
#             dataIn = genericData.Data
#             metadataIn = genericData.Metadata
#         if self.verbose: print('raw file Metadata: \n\n',yaml.dump(metadataIn,sort_keys=False),'\n')
#         if self.verbose: print('Dropping non-numeric data')
#         keep = []
#         for trace,detals in metadataIn['Variables'].items():
#             if not detals['ignore']: keep.append(trace)
#         dataIn = dataIn[keep].copy()
#         for y in dataIn.index.year.unique():
#             dbpth = os.path.join(self.projectPath,str(y),metadataIn['siteID'],stage,metadataIn['subSiteID'])
#             if not os.path.isdir(dbpth):
#                 if self.verbose: print('Creating folder: ',dbpth)
#                 os.makedirs(dbpth)
#             fullYearData,fullYearMetadata = self.readDatabaseFolder(dbpth)
#             if fullYearData.empty:
#                 fullYearData = pd.DataFrame(index=pd.date_range(str(y),str(y+1),freq=metadataIn['frequency'],inclusive='right'))
#                 fullYearData['POSIX_timestamp'] = (fullYearData.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
#                 fullYearData = fullYearData.join(dataIn)
#             else:
#                 if mode == 'fill':
#                     fill_cols = [c for c in dataIn.columns if c in fullYearData.columns]
#                     fullYearData = fullYearData.fillna(dataIn[fill_cols])
#                 else: fill_cols = []
#                 append_cols = [c for c in dataIn.columns if c not in fill_cols]
#                 fullYearData = fullYearData.join(dataIn[append_cols])
#             self.writeDatabaseArrays(dbpth,fullYearData)
#             if fullYearMetadata is not None:
#                 dd = deepdiff.DeepDiff(fullYearMetadata,metadataIn,ignore_order=True)
#             with open(os.path.join(dbpth,'_metadata.yml'),'w+') as file:
#                 yaml.safe_dump(metadataIn,file,sort_keys=False)
        
#     def writeDatabaseArrays(self,dbpth,Data):
#         for col in Data.columns:
#             fname = os.path.join(dbpth,col)
#             if col in self.metadataFile['defaultFormat'].keys():
#                 dtype = self.metadataFile['defaultFormat'][col]['dtype']
#             else:
#                 dtype = self.metadataFile['defaultFormat']['data']['dtype']
#             if self.verbose: print('Writing: ',fname)
#             Data[col].astype(dtype).values.tofile(fname)