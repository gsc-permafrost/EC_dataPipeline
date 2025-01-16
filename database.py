import os
import sys
import yaml
import shutil
import datetime
import numpy as np
import pandas as pd
from dataclasses import dataclass,field


def load():
    c = os.path.dirname(os.path.abspath(__file__))
    pth = os.path.join(c,'config_files','databaseMetadata.yml')
    with open(pth,'r') as f:
        defaults = yaml.safe_load(f)
    return(defaults)

@dataclass
class database:
    # Declare the defaults for a database
    isDatabase: bool = False
    projectPath: str = None
    logfile: str = ''
    siteID: list = field(default_factory=list)
    level: list = field(default_factory=lambda:['raw','Processed'])
    Years: list = field(default_factory=lambda:[datetime.datetime.now().year])
    metadata: dict = field(default_factory=load)


    def __post_init__(self):
        self.Years = [str(Y) for Y in self.Years]
        self._metadata = os.path.join(self.projectPath,"_metadata.yml")
        self._logfile = os.path.join(self.projectPath,'_logfile.txt')
        self._map = {os.path.join(Y,siteID,level):[]
                           for Y in self.Years
                           for siteID in self.siteID
                           for level in self.level}
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
        now = self.now()
        self.metadata['Date_created'] = now
        self.metadata['Last_modified'] = now
        self.logfile = 'Creating Database: ' + now + '\n'
        for dbpath in self._map:
            os.makedirs(os.path.join(self.projectPath,dbpath))
        with open(self._metadata,'w+') as file:
            yaml.safe_dump(self.metadata,file,sort_keys=False)
        with open(self._logfile,'w+') as file:
            file.write(self.logfile)

    def openDatabase(self):
        with open(self._metadata) as file:
            metadata = yaml.safe_load(file)
            if sum(k not in metadata.keys() for k in self.metadata.keys()):
                sys.exit('Database metadata are corrupted')
            self.metadata = metadata
        with open(self._logfile) as file:
            self.logfile = file.read()
    
    def readDatabaseFolder(self,dbpth,filter = None):
        files = [f for f in os.listdir(dbpth) if filter is None or f in filter]
        df = pd.DataFrame(data = {f:np.fromfile(os.path.join(dbpth,f),dtype=self.metadata[f]['dtype']) if f in self.metadata.keys() 
                else np.fromfile(os.path.join(dbpth,f),dtype=self.metadata['default']['dtype'])
                for f in files})
        if not df.empty:
            df.index=pd.to_datetime(df['POSIX_timestamp'],unit='s')
        return(df)
        
    def writeDatabase(self,dataIn,siteID,stage='raw',mode='fill',freq='30min'):
        for y in dataIn.index.year.unique():
            dbpth = os.path.join(self.projectPath,str(y),siteID,stage)
            if not os.path.isdir(dbpth):
                os.makedirs(dbpth)
            fullYear = self.readDatabaseFolder(dbpth)
            if fullYear.empty:
                fullYear = pd.DataFrame(index=pd.date_range(str(y),str(y+1),freq=freq,inclusive='right'))
                fullYear['POSIX_timestamp'] = (fullYear.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
                fullYear = fullYear.join(dataIn)
                for col in fullYear.columns:
                    if col in self.metadata.keys():
                        fullYear[col].astype(self.metadata[col]['dtype']).values.tofile(os.path.join(dbpth,col))
                    else:
                        fullYear[col].astype(self.metadata['default']['dtype']).values.tofile(os.path.join(dbpth,col))
            else:
                if mode == 'fill':
                    fill_cols = [c for c in dataIn.columns if c in fullYear.columns]
                    fullYear = fullYear.fillna(dataIn[fill_cols])
                else: fill_cols = []
                append_cols = [c for c in dataIn.columns if c not in fill_cols]
                fullYear = fullYear.join(dataIn[append_cols])
            print(fullYear.head())
