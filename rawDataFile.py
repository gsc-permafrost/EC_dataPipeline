import re
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields
import NewDev as ND

fillChar = '_'
sepChar = '-'
        
@dataclass(kw_only=True)
class observation(ND.metadataRecord):
    ignore: bool = False
    originalName: str = None
    unit: str = None
    dtype: str = None
    variableDescription: str = None
    # positional variables (vertical, horizontal, repetition)
    V: int = None
    H: int = None
    R: int = None

    def __post_init__(self):
        if self.originalName is not None:
            self.variableID = re.sub('[^0-9a-zA-Z]+',fillChar,self.originalName)
            if len(self.variableID)>1:
                self.variableID = self.variableID.rstrip(fillChar)
        self.ignore = not np.issubdtype(self.dtype,np.number) or self.variableID == fillChar
        self.dtype = self.dtype.str

@dataclass(kw_only=True)
class genericLoggerFile:
    source: str = field(repr=False)
    verbose: bool = True
    loggerName: str = None
    timeZone: str = 'UTC'
    frequency: str = '30min'
    Data: pd.DataFrame = field(default_factory=pd.DataFrame)

    def __post_init__(self):
        self.Metadata = {}
        for k,v in self.__dataclass_fields__.items():
            if v.repr and not v.type == type(pd.DataFrame()):
                self.Metadata[v.name] = self.__dict__[v.name]     
        self.Metadata['Variables'] = {}
        newNames = []
        if self.verbose: print('Standardizing and documenting traces')
        for col in self.Data.columns:
            obs = observation(originalName=col,dtype=self.Data[col].dtype)
            if obs.variableID != obs.originalName and self.verbose:
                print('Re-named: ',obs.originalName,' to: ',obs.variableID)
            self.Metadata['Variables'][obs.variableID] = obs.__dict__
            newNames.append(obs.variableID)
        self.Data.columns = newNames


@dataclass(kw_only=True)
class hoboCSV(genericLoggerFile):
    fileType = "HoboCSV"
    timestamp: str = field(default="Date Time",repr=False)
    yearfirst: bool = True
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'], repr=False)
    excludePattern: list = field(default_factory=lambda:{r"LGR(.*?)LBL: ":''},repr=False)

    def __post_init__(self):
        rawFile = open(self.source,'r',encoding='utf-8-sig')
        T = rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        self.Data = pd.read_csv(rawFile)
        # ID the timestamp and parse to datetime index
        self.Data.index = self.Data[
            self.Data.columns[self.Data.columns.str.contains(self.timestamp)].values
            ].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
        self.statusCols = self.Data.columns[self.Data.columns.str.contains('|'.join(self.statusCols))].values
        self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
        self.Data = self.Data.loc[keep].copy()
        self.Data = self.Data.drop(columns=self.statusCols)
        
        shortNames = []
        for i,c in enumerate(self.Data.columns):
            for ex,rep in self.excludePattern.items():
                match = re.search(ex, c)
                if match is not None:
                    c = c.replace(match[0],rep)
            shortNames.append(c)
        self.Data.columns = shortNames
        super().__post_init__()