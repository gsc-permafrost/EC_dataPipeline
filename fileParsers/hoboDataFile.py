import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field
from . import loggerClass


@dataclass
class CSV(loggerClass.Logger):
    fileType: str = "HoboCSV"
    sourcePath: str = None
    verbose: bool = True
    timeZone: str = 'UTC'
    measurementInterval: str = None
    dropNonNumeric: bool = True
    Data: pd.DataFrame = field(default_factory=pd.DataFrame)
    timestamp_cols: str = field(default_factory=lambda:['Date_Time'])
    # Assume year is first in the date, dateparser (should) correct if not the case
    yearfirst: bool = True
    name_out: dict = field(default_factory=dict)
    # names of header metadata
    header_index: list = field(default_factory=lambda:['unit_in','logger_sn','sensor_sn'])
    # non-data traces
    statusCols: list = field(default_factory=lambda:['Host_Connected', 'Stopped', 'End_Of_File'])

    def __post_init__(self):
        if self.sourcePath is not None:
            self.readFile()

    def readFile(self):
        # Determine if the file is a hobo file, then read header and data
        self.rawFile = open(self.sourcePath,'r',encoding='utf-8-sig')
        T = self.rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        else:
            self.readHeader()
            self.readData()
        if self.dropNonNumeric:
            if self.verbose: print('Dropping non-numeric data')
            self.Data = self.Data._get_numeric_data().copy()
        
    def readHeader(self):
        # Parse the header information form the second line of the file
        H = self.rawFile.readline()
        H = H.replace('#','RecordNumber').lstrip('"').rstrip('"\n').split('","')
        H = [h.lstrip().rstrip(')').replace('(',',').split(',') for h in H]
        L = [len(h) for h in H]
        headerData = [h if l == max(L) else [sh for sh in h] + ['' for i in range(max(L)-l)] for h,l in zip(H,L)]

        # Create a header dataframe
        self.Header = pd.DataFrame(data = headerData).T
        self.Header.columns = self.Header.iloc[0].values+self.Header.iloc[-1].values
        self.Header.columns = self.Header.columns.str.replace(' ','_').str.replace(':','').str.rstrip('_')
        self.Header = self.Header[1:-1].copy()
        self.Header.index.name=''
        self.Header.index = self.header_index
        # Identify status cols present in file
        self.statusCols = self.Header.columns[self.Header.columns.isin(self.statusCols)]
        # Parse timezone if not provided
        if self.timeZone is None:
            self.timeZone = self.Header['Date_Time']['unit_in'].lstrip()
        
    def readData(self):
        # Read the data from the csv
        # Copy over the header information
        self.Data = pd.read_csv(self.rawFile,header=None)
        self.Data.columns = self.Header.columns
        # Drop records with a status flag
        self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
        self.Data = self.Data.loc[keep].copy()
        # Convert default 64-bit values to 32-bit
        self.Data = self.Data.astype({col:'float32' for col in self.Data.select_dtypes('float64').columns})
        self.Data = self.Data.astype({col:'int32' for col in self.Data.select_dtypes('int64').columns})
        # Assign datatypes to header values
        self.Header.loc['dataType'] = [str(v) for v in self.Data.dtypes.values]
        self.Data.index = self.Data[self.timestamp_cols].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
        self.POSIX_timestamp = (self.Data.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        if self.measurementInterval is None:
            self.measurementInterval = str(int(np.median(np.diff(self.POSIX_timestamp))))+'s'
        self.Data.index = pd.to_datetime(self.POSIX_timestamp,unit='s')

