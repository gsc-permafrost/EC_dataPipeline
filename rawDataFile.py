import re
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields
import helperFunctions as helper

@dataclass
class observation:
    fillChar: str = '_'
    ignore: bool = False
    name_in: str = None
    unit_in: str = None
    safe_name: str = None
    dtype: str = None
    variableDescription: str = None

    def __post_init__(self):
        if self.name_in is not None:
            self.safe_name = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.name_in)
        self.ignore = not np.issubdtype(self.dtype,np.number)
        if self.safe_name == self.fillChar*len(self.safe_name):
            self.ignore = True
        self.dtype = self.dtype.str
        
@dataclass(kw_only=True)
class genericLoggerFile:
    # Important attributes to be associated with a generic logger file
    verbose: bool = field(default=True,repr=False)
    siteID: str = None
    measurementID: str = None
    fileType: str = None
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    def __post_init__(self):
        if self.fileType is None:
            self.fileType = type(self).__name__
        self.Metadata = {k:self.__dict__[k] for k,v in genericLoggerFile.__dataclass_fields__.items() if v.repr}
        # for field_value in type(self).__mro__[-2].__dataclass_fields__.values():
        #     # print(field_value)
        #     # exclude a subset of items from the attributes
        #     if not (field_value.type == type(pd.DataFrame()) or field_value.name=='verbose'):
        #         self.Metadata[field_value.name] = self.__dict__[field_value.name]     

        self.Metadata['Variables'] = {}
        newNames = []
        if self.verbose: helper.log('Standardizing and documenting traces')
        for col in self.Data.columns:
            # Generate Metadata for each observation
            obs = observation(name_in=col,dtype=self.Data[col].dtype)
            if obs.safe_name != obs.name_in and self.verbose:
                helper.log(['Re-named: ',obs.name_in,' to: ',obs.safe_name])
            self.Metadata['Variables'][obs.safe_name] = obs.__dict__
            newNames.append(obs.safe_name)
        self.Data.columns = newNames

@dataclass(kw_only=True)
class hoboCSV(genericLoggerFile):
    # fileType = 'HOBOcsv'
    sourceFile: str
    verbose: bool = True
    timestamp: str = "Date Time"
    varRepMap: dict = field(default_factory=dict)
    yearfirst: bool = True
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'])

    def __post_init__(self):
        rawFile = open(self.sourceFile,'r',encoding='utf-8-sig')
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

        super().__post_init__()

