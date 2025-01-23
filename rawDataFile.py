import re
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields

@dataclass
class observation:
    ignore: bool = True
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
            # if min([self.V,self.H,self.R])>0:
            #     self.safe_name = '_'.join([str(i) for i in [self.safeName,self.V,self.H,self.R]])
        self.unit_out = self.unit_in

@dataclass
class genericLoggerFile:
    # Important attributes to be associated with a generic logger file
    verbose: bool = True
    siteID: str = 'siteID'
    fileType: str = None
    loggerName: str = 'loggerName'
    subSiteID: str = None
    siteDescription: str = None
    # firstRecord: str = None
    # lastRecord: str = None
    replicateID: int = 1
    frequency: str = '30min'
    timeZone: str = 'UTC'
    lat: float = 0.0
    lon: float = 0.0
    Data: pd.DataFrame = field(default_factory=pd.DataFrame)
    def __post_init__(self):
        # fill subSiteID from logger name if alternate not provided
        if self.subSiteID is None and self.loggerName is not None:
            self.subSiteID = self.loggerName
        self.Metadata = {}
        for field_value in type(self).__mro__[-2].__dataclass_fields__.values():
            # exclude a subset of items from the attributes
            if not (field_value.type == type(pd.DataFrame()) or field_value.name=='verbose'):
                self.Metadata[field_value.name] = self.__dict__[field_value.name]     

            # self.firstRecord = self.Data.index.min().strftime('%Y-%m-%d %H:%M:%S')
            # self.lastRecord = self.Data.index.max().strftime('%Y-%m-%d %H:%M:%S')
        self.Metadata['Variables'] = {}
        newNames = []
        if self.verbose: print('Standardizing and documenting traces')
        for col in self.Data.columns:
            obs = observation(name_in=col,dtype=self.Data[col].dtype.str)
            if obs.safe_name != obs.name_in and self.verbose:
                print('Re-named: ',obs.name_in,' to: ',obs.safe_name)
            self.Metadata['Variables'][obs.safe_name] = obs.__dict__
            newNames.append(obs.safe_name)
        self.Data.columns = newNames

@dataclass
class hoboCSV(genericLoggerFile):
    verbose: bool = True
    fileType: str = "HoboCSV"
    sourcePath: str = None
    timestamp: str = "Date Time"
    varRepMap: dict = field(default_factory=dict)
    yearfirst: bool = True
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'])

    def __post_init__(self):
        rawFile = open(self.sourcePath,'r',encoding='utf-8-sig')
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
        # if self.varRepMap == {}:

        # super().__post_init__()





















# @dataclass
# class CSV(observationClasses.Logger):
#     fileType: str = "HoboCSV"
#     sourcePath: str = None
#     verbose: bool = True
#     timeZone: str = 'UTC'
#     measurementInterval: str = None
#     dropNonNumeric: bool = True
#     duplicateAxis: str = 'V'
#     variableMap: dict = field(default_factory=dict)
#     Data: pd.DataFrame = field(default_factory=pd.DataFrame)
#     timestamp_cols: str = field(default_factory=lambda:['Date Time'])
#     # Assume year is first in the date, dateparser (should) correct if not the case
#     yearfirst: bool = True
#     safe_name: dict = field(default_factory=dict)
#     # names of header metadata
#     header_index: list = field(default_factory=lambda:['name_in','unit_in','logger_sn','sensor_sn','label'])
#     # non-data traces
#     statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'])

#     def __post_init__(self):
#         if self.sourcePath is not None:
#             self.readFile()
#             # self.documentData()

#     def readFile(self):
#         # Determine if the file is a hobo file, then read header and data
#         rawFile = open(self.sourcePath,'r',encoding='utf-8-sig')
#         T = rawFile.readline().rstrip('\n')
#         if not T.startswith('"Plot Title: '):
#             self.fileType = False
#         column_names = rawFile.readline().lstrip('"').rstrip('"\n').split('","')
#         print(column_names)
        
#         # else:
#         #     self.readHeader()
#             # self.readData()
#         # if self.dropNonNumeric:
#         #     if self.verbose: print('Dropping non-numeric data')
#         #     self.Data = self.Data._get_numeric_data().copy()
        
#     # def readHeader(self):
#     #     # Parse the header information form the second line of the file
#     #     self.colnames = rawFile.readline().lstrip('"').rstrip('"\n').split('","')
#     #     print(self.colnames)

#         # H = H.replace('#','RecordNumber').lstrip('"').rstrip('"\n').split('","')
#         # H = [h.lstrip().rstrip(')').replace('(',',').split(',') for h in H]
#         # print(H)
#         # L = [len(h) for h in H]
#         # headerData = [h if l == max(L) else [sh for sh in h] + ['' for i in range(max(L)-l)] for h,l in zip(H,L)]

#         # # Create a header dataframe
#         # self.Header = pd.DataFrame(data = headerData).T.map(lambda x: x.strip() if isinstance(x, str) else x)
#         # self.Header.index = self.header_index
#         # self.Header = self.Header.map(lambda x: x.split(': ')[-1])

#         # self.parseDuplicateHeaders()


#         # # self.Header.columns = self.Header.loc['name_in'].values

#         # # self.Header = self.Header[1:-1].copy()
#         # # self.Header.index.name=''
#         # # # Identify status cols present in file
#         # print(self.statusCols)
#         # print(self.Header.loc['name_in'].isin(self.statusCols))
#         # self.statusCols = self.Header.columns[self.Header.loc['name_in'].isin(self.statusCols)]
#         # print(self.statusCols)
#         # # Parse timezone if not provided
#         # if self.timeZone is None:
#         #     self.timeZone = self.Header[self.timestamp_cols[0]]['unit_in'].lstrip()

#         # observationClasses.observation()

#     def parseDuplicateHeaders(self):
#         # Find replicated variable names
#         # Assume they represent vertical replicates if numeric label, horizontal replicates if text label
#         reps = self.Header[self.Header.columns[self.Header.loc['name_in'].duplicated(keep=False)]]
#         try:
#             order = reps.loc['label'].astype('float')
#         except:
#             order = reps.loc['label'].sort_values()
#         order = order.reset_index()
#         self.Header.loc[self.duplicateAxis] = 1
#         self.Header.loc[self.duplicateAxis,order['index'].values]=order.index.values+1
        
#     def readData(self):
#         # Read the data from the csv
#         # Copy over the header information
#         self.Data = pd.read_csv(rawFile,header=None)
#         self.Data.columns = self.colnames
#         # Drop records with a status flag
#         self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
#         keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
#         self.Data = self.Data.loc[keep].copy()
#         # Convert default 64-bit values to 32-bit
#         self.Data = self.Data.astype({col:'float32' for col in self.Data.select_dtypes('float64').columns})
#         self.Data = self.Data.astype({col:'int32' for col in self.Data.select_dtypes('int64').columns})
#         # Assign datatypes to header values
#         self.Header.loc['dataType'] = [str(v) for v in self.Data.dtypes.values]
#         self.Data.index = self.Data[self.timestamp_cols].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
#         self.POSIX_timestamp = (self.Data.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
#         if self.measurementInterval is None:
#             self.measurementInterval = str(int(np.median(np.diff(self.POSIX_timestamp))))+'s'
#         self.Data.index = pd.to_datetime(self.POSIX_timestamp,unit='s')

#     def documentData(self):
#         field_names = set(f.name for f in fields(observationClasses.observation))
#         for var,info in self.Header.to_dict().items():
#             if var in self.Data.columns:
#                 obs = observationClasses.observation(name_in = var,
#                     **{k:v for k,v in info.items() if k in field_names}
#                 )
#                 print(obs)

