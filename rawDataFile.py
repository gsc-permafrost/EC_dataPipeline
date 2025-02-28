import re
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields

        
@dataclass(kw_only=True)
class observation:
    ignore: bool = False
    name_in: str = None
    unit_in: str = None
    # unit_out: str = None
    # safeName: str = None
    safe_name: str = None
    dtype: str = None
    variableDescription: str = None
    # positional variables (vertical, horizontal, repetition)
    # V: int = None
    # H: int = None
    # R: int = None

    def __post_init__(self):
        if self.name_in is not None:
            self.safe_name = re.sub('[^0-9a-zA-Z]+','_',self.name_in)
            if len(self.safe_name)>1:
                self.safe_name = self.safe_name.rstrip('_')
        self.ignore = not np.issubdtype(self.dtype,np.number) or self.safe_name == '_'
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
            obs = observation(name_in=col,dtype=self.Data[col].dtype)
            if obs.safe_name != obs.name_in and self.verbose:
                print('Re-named: ',obs.name_in,' to: ',obs.safe_name)
            print(obs)
            self.Metadata['Variables'][obs.safe_name] = obs.__dict__
            newNames.append(obs.safe_name)
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