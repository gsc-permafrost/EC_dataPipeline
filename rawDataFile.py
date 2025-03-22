import re
import os
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields
import helperFunctions as helper

@dataclass
class observation:
    fillChar = '_'
    safe_name: str = None
    ignore: bool = False
    name_in: str = None
    unit_in: str = None
    dtype: str = None
    variableDescription: str = None
    verbose: bool = field(default=False,repr=False)
    def __post_init__(self):
        if self.name_in is not None:
            if self.safe_name is None:
                self.safe_name = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.name_in)
            if self.safe_name == self.fillChar*len(self.safe_name):
                self.ignore = True
            else:
                self.safe_name = self.safe_name.rstrip('_')
            self.ignore = not np.issubdtype(self.dtype,np.number)
            self.dtype = self.dtype.str
            if self.safe_name != self.name_in and self.verbose:
                helper.log(['Re-named: ',self.name_in,' to: ',self.safe_name])
        
@dataclass(kw_only=True)
class genericLoggerFile:
    # Important attributes to be associated with a generic logger file
    projectPath: str = None
    siteID: str = None
    measurementID: str = None
    fileType: str = None
    Variables: dict = field(default_factory=lambda:{})
    colMap: dict = field(default_factory=lambda:{},repr=False)
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    verbose: bool = field(default=True,repr=False)
    def __post_init__(self):
        if self.fileType is None:
            self.fileType = type(self).__name__
        self.Metadata = {k:self.__dict__[k] for k,v in genericLoggerFile.__dataclass_fields__.items() if v.repr}
        safe_name = {col:None if col not in self.colMap else self.colMap[col] for col in self.Data.columns}
        self.Metadata['Variables'] = helper.dictToDataclass(
            observation,{col:{'name_in':col,'dtype':self.Data[col].dtype,'safe_name':safe_name[col]} for col in self.Data.columns},['name_in'],pop=True,constants={'verbose':self.verbose})
        if self.projectPath and self.siteID and self.measurementID:
            print(os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID))


        # print()
        # newNames = []
        # if self.verbose: helper.log('Standardizing and documenting traces')
        # for col in self.Data.columns:
        #     # Generate Metadata for each observation
        #     obs = observation(name_in=col,dtype=self.Data[col].dtype)
        #     if obs.safe_name != obs.name_in and self.verbose:
        #         helper.log(['Re-named: ',obs.name_in,' to: ',obs.safe_name])
        #     self.Metadata['Variables'][obs.safe_name] = obs.__dict__
        #     newNames.append(obs.safe_name)
        # self.Data.columns = newNames

@dataclass(kw_only=True)
class HOBOcsv(genericLoggerFile):
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

