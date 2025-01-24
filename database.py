import os
import sys
import yaml
import shutil
import deepdiff
import datetime
import numpy as np
import pandas as pd
from dataclasses import dataclass,field

from rawDataFile import genericLoggerFile


def load():
    # read yml template(s)
    c = os.path.dirname(os.path.abspath(__file__))
    pth = os.path.join(c,'config_files','databaseMetadata.yml')
    with open(pth,'r') as f:
        defaults = yaml.safe_load(f)
    return(defaults)

@dataclass
class database:
    # Declare the defaults for a database
    verbose: bool = True
    isDatabase: bool = False
    projectPath: str = None
    logfile: str = ''
    siteID: list = field(default_factory=list)
    level: list = field(default_factory=lambda:['raw','clean'])
    Years: list = field(default_factory=lambda:[datetime.datetime.now().year])
    metadata: dict = field(default_factory=load)

    def __post_init__(self):
        # self.Years = [str(Y) for Y in self.Years]
        self._metadata = os.path.join(self.projectPath,"_metadata.yml")
        self._logfile = os.path.join(self.projectPath,'_logfile.txt')
        # self._map = {os.path.join(Y,siteID,level):[]
        #             for Y in self.Years
        #             for siteID in self.siteID
        #             for level in self.level}
        if not os.path.isdir(self.projectPath):
            self.makeDatabase()
        elif not os.path.isfile(self._metadata) and os.listdir(self.projectPath):
           sys.exit('Non-empty, non-project directory provided')
        else:
            self.openDatabase()
        self.isDatabase = True
        
    def now(self):
        return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def makeDatabase(self):
        if self.verbose:print('Initializing empty database')
        os.makedirs(self.projectPath)
        now = self.now()
        self.metadata['Date_created'] = now
        self.metadata['Last_modified'] = now
        self.logfile = 'Creating Database: ' + now + '\n'
        # for dbpath in self._map:
        #     newFolder = os.path.join(self.projectPath,dbpath)
        #     os.makedirs(newFolder)
        #     if self.verbose:print('Creating folder: ',newFolder)
        with open(self._metadata,'w+') as file:
            if self.verbose:print('Creating folder: ',self._metadata)
            yaml.safe_dump(self.metadata,file,sort_keys=False)
        with open(self._logfile,'w+') as file:
            if self.verbose:print('Creating file: ',self._logfile)
            file.write(self.logfile)

    def openDatabase(self):
        with open(self._metadata) as file:
            metadata = yaml.safe_load(file)
            if sum(k not in metadata.keys() for k in self.metadata.keys()):
                sys.exit('Database metadata are corrupted')
            self.metadata = metadata
        with open(self._logfile) as file:
            self.logfile = file.read()

    # def readDatabase(self,siteID=[],Years=[],stage=[],subSiteID=[]):

    
    def readDatabaseFolder(self,dbpth,filter = None):
        files = [f for f in os.listdir(dbpth) if '.' not in f and (filter is None or f in filter and f)]
        data = {f:np.fromfile(os.path.join(dbpth,f),dtype=self.metadata['defaultFormat'][f]['dtype']) if f in self.metadata['defaultFormat'].keys() 
                else np.fromfile(os.path.join(dbpth,f),dtype=self.metadata['defaultFormat']['data']['dtype'])
                for f in files}
        df = pd.DataFrame(data = {f:np.fromfile(os.path.join(dbpth,f),dtype=self.metadata['defaultFormat'][f]['dtype']) if f in self.metadata['defaultFormat'].keys() 
                else np.fromfile(os.path.join(dbpth,f),dtype=self.metadata['defaultFormat']['data']['dtype'])
                for f in files}
                )
        if not df.empty:
            df.index=pd.to_datetime(df['POSIX_timestamp'],unit='s')
        metadataFile = os.path.join(dbpth,'_metadata.yml')            
        if os.path.isfile(metadataFile):
            with open(metadataFile,'r') as file:
                metadataFile = yaml.safe_load(file)
        else:
            metadataFile = None
        return(df,metadataFile)
        
    def rawDatabaseImport(self,dataIn,metadataIn = None,stage='raw',mode='fill'):
        if metadataIn is None:
            print('Warning! No metadata provided, using default template dataset')
            genericData = genericLoggerFile(data=dataIn)
            dataIn = genericData.Data
            metadataIn = genericData.Metadata
        if self.verbose: print('raw file Metadata: \n\n',yaml.dump(metadataIn,sort_keys=False),'\n')
        if self.verbose: print('Dropping non-numeric data')
        keep = []
        for trace,detals in metadataIn['Variables'].items():
            if not detals['ignore']: keep.append(trace)
        dataIn = dataIn[keep].copy()
        for y in dataIn.index.year.unique():
            dbpth = os.path.join(self.projectPath,str(y),metadataIn['siteID'],stage,metadataIn['subSiteID'])
            if not os.path.isdir(dbpth):
                if self.verbose: print('Creating folder: ',dbpth)
                os.makedirs(dbpth)
            fullYearData,fullYearMetadata = self.readDatabaseFolder(dbpth)
            if fullYearData.empty:
                fullYearData = pd.DataFrame(index=pd.date_range(str(y),str(y+1),freq=metadataIn['frequency'],inclusive='right'))
                fullYearData['POSIX_timestamp'] = (fullYearData.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
                fullYearData = fullYearData.join(dataIn)
            else:
                if mode == 'fill':
                    fill_cols = [c for c in dataIn.columns if c in fullYearData.columns]
                    fullYearData = fullYearData.fillna(dataIn[fill_cols])
                else: fill_cols = []
                append_cols = [c for c in dataIn.columns if c not in fill_cols]
                fullYearData = fullYearData.join(dataIn[append_cols])
            self.writeDatabaseArrays(dbpth,fullYearData)
            if fullYearMetadata is not None:
                dd = deepdiff.DeepDiff(fullYearMetadata,metadataIn,ignore_order=True)
            with open(os.path.join(dbpth,'_metadata.yml'),'w+') as file:
                yaml.safe_dump(metadataIn,file,sort_keys=False)
        
    def writeDatabaseArrays(self,dbpth,Data):
        for col in Data.columns:
            fname = os.path.join(dbpth,col)
            if col in self.metadata['defaultFormat'].keys():
                dtype = self.metadata['defaultFormat'][col]['dtype']
            else:
                dtype = self.metadata['defaultFormat']['data']['dtype']
            if self.verbose: print('Writing: ',fname)
            Data[col].astype(dtype).values.tofile(fname)