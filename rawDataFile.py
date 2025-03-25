import re
import os
import struct
import datetime
import numpy as np
import pandas as pd
import dateutil.parser as dateParse 
from dataclasses import dataclass,field,fields
import helperFunctions as helper

log = helper.log

@dataclass(kw_only=True)
class columnMap:
    dtype_map_numpy = {"IEEE4B": "float32","IEEE8B": "float64","FP2": "float16"}
    fillChar = '_'
    inputName: str = field(repr=False)
    safeName: str = None
    ignore: bool = True
    unit: str = None
    dtype: str = None
    frequency: str = None
    variableDescription: str = None
    verbose: bool = field(default=False,repr=False)
    def __post_init__(self):
        if self.inputName is not None:
            if self.safeName is None:
                self.safeName = re.sub('[^0-9a-zA-Z]+',self.fillChar,self.inputName)
            else:
                self.safeName = self.safeName.rstrip('_')
            if self.dtype is not None:
                if self.dtype in self.dtype_map_numpy:
                    self.dtype = np.dtype(self.dtype_map_numpy[self.dtype])
                else:
                    self.dtype = np.dtype(self.dtype)
                self.ignore = not np.issubdtype(self.dtype,np.number)
            if self.dtype:
                self.dtype = self.dtype.str
            if self.safeName == self.fillChar*len(self.safeName):
                self.ignore = True
            if self.safeName != self.inputName and self.verbose:
                log(['Re-named: ',self.inputName,' to: ',self.safeName])
        
@dataclass(kw_only=True)
class genericLoggerFile:
    # Important attributes to be associated with a generic logger file
    siteID: str = None
    measurementID: str = None
    timezone: str = None
    fileType: str = None
    frequency: str = None
    variableMap: dict = field(default_factory=lambda:{})
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    verbose: bool = field(default=True,repr=False)

    def __post_init__(self):
        # Create the template column map, fill column dtype where not present 
        self.variableMap = {key:{'dtype':self.Data[key].dtype}|self.variableMap[key] if key in self.variableMap else {'dtype':self.Data[key].dtype} for key in self.Data.columns}
        self.variableMap = {var.inputName:helper.reprToDict(var) for var in map(lambda name: columnMap(inputName = name, **self.variableMap[name]),self.variableMap.keys())}

    def applySafeNames(self):
        self.safeMap = {col:val['safeName'] for col,val in self.variableMap.items()}
        self.backMap = {safeName:col for col,safeName in self.safeMap.items()}
        self.Data = self.Data.rename(columns=self.safeMap)

@dataclass(kw_only=True)
class asciiHeader(genericLoggerFile):
    fileObject: object = field(default=None,repr=False)
    header: list = field(default_factory=lambda:[],repr=False)
    fileType: str = None
    StationName: str = None
    LoggerModel: str = None
    SerialNo: str = None
    program: str = None
    fileTimestamp: str = None
    frequency: str = None
    Table: str = None
    variableMap: dict = field(default_factory=lambda:{})
    byteMap: str = None

    def __post_init__(self):
        header = self.parseLine(self.fileObject.readline())
        self.fileType = header[0]
        self.Type=header[0]
        self.StationName=header[1]
        self.LoggerModel=header[2]
        self.SerialNo=header[3]
        if self.fileType == 'TOB3':
            self.fileTimestamp = pd.to_datetime(header[-1])
            header = self.parseLine(self.fileObject.readline())
            self.Table = header[0]
            self.frequency = f"{pd.to_timedelta(self.parseFreq(header[1])).total_seconds()}s"
            self.frameSize = int(header[2])
            self.val_stamp = int(header[4])        
            self.frameTime = pd.to_timedelta(self.parseFreq(header[5])).total_seconds()
            self.variableMap = {column:{'unit':unit,
                                        'variableDescription':aggregation,
                                        'dtype':dtype
                                        } for column,unit,aggregation,dtype in zip(
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            )}
            dtype_map_struct = {"IEEE4B": "f","IEEE8B": "d","FP2": "H"}
            self.byteMap = ''.join([dtype_map_struct[var['dtype']] for var in self.variableMap.values()])
            self.variableMap = {var.inputName:helper.reprToDict(var) for var in map(
                                    lambda name: columnMap(inputName = name, **self.variableMap[name]),self.variableMap.keys()
                                    )}

        elif self.fileType == 'TOA5':
            self.Table = header[-1]
            self.variableMap = {column:{'unit':unit,
                                        'variableDescription':aggregation
                                        } for column,unit,aggregation in zip(
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            self.parseLine(self.fileObject.readline()),
                                            )}
            self.fileTimestamp = pd.to_datetime(datetime.datetime.strptime(re.search(r'([0-9]{4}\_[0-9]{2}\_[0-9]{2}\_[0-9]{4})', f.rsplit('.',1)[0]).group(0),'%Y_%m_%d_%H%M'))
        self.fileTimestamp = self.fileTimestamp.strftime('%Y-%m-%dT%H:%M:%S')
        super().__post_init__()
                
    def parseLine(self,line):
        return(line.decode('ascii').strip().replace('"','').split(','))

    def parseFreq(self,text):
        #Parse a measurement frequency from a TOB3 string input to a format compatible with pandas datetime
        def split_digit(s):
            match = re.search(r"\d", s)
            if match:
                s = s[match.start():]
            return s 
        freqDict = {'MSEC':'ms','Usec':'us','Sec':'s','HR':'h','MIN':'min'}
        freq = split_digit(text)
        for key,value in freqDict.items():
            freq = re.sub(key.lower(), value, freq, flags=re.IGNORECASE)
        freq = freq.replace(' ','')
        return(freq)

@dataclass(kw_only=True)
class TOB3(asciiHeader):
    sourceFile: str
    header: list = field(default_factory=lambda:{})
    readData: bool = field(default=False,repr=False)
    Data: pd.DataFrame = field(default_factory=pd.DataFrame,repr=False)
    calcStats: bool = field(default_factory=False,repr=False)

    def __post_init__(self):
        with open(self.sourceFile,'rb') as self.fileObject:
            super().__post_init__()
            if self.readData:
                self.Data, Timestamp = self.readFrames()
                self.Data = pd.DataFrame(self.Data)
                self.Data.columns = [v['safeName'] for v in self.variableMap.values()]
                Timestamp = pd.to_datetime(Timestamp,unit='s')
                self.Data.index=Timestamp.round(self.frequency)
                self.Data.index.name = 'TIMESTAMP'
                if self.calcStats:
                    Agg = {}
                    for column in self.variableMap:
                        Agg[column] = self.Data[column].agg(['mean','std','count'])
                    self.Data = pd.DataFrame(
                        index=[Timestamp[-1]],
                        data = {f"{col}_{agg}":val 
                                for col in Agg.keys() 
                                for agg,val in Agg[col].items()})
            
    def readFrames(self):
        Header_size = 12
        Footer_size = 4
        record_size = struct.calcsize('>'+self.byteMap)
        records_per_frame = int((self.frameSize-Header_size-Footer_size)/record_size)
        self.byteMap_Body = '>'+''.join([self.byteMap for r in range(records_per_frame)])
        i = 0
        Timestamp = []
        campbellBaseTime = pd.to_datetime('1990-01-01').timestamp()
        readFrame = True
        frequency = float(self.frequency.rstrip('s'))
        while readFrame:         
            sb = self.fileObject.read(self.frameSize)
            if len(sb)!=0:
                Header = sb[:Header_size]
                Header = np.array(struct.unpack('LLL', Header))
                Footer = sb[-Footer_size:]
                Footer = struct.unpack('L', Footer)[0]
                flag_e = (0x00002000 & Footer) >> 14
                flag_m = (0x00004000 & Footer) >> 15
                footer_validation = (0xFFFF0000 & Footer) >> 16

                time_1 = (Header[0]+Header[1]*self.frameTime+campbellBaseTime)
                if footer_validation == self.val_stamp and flag_e != 1 and flag_m != 1:
                    Timestamp.append([time_1+i*frequency for i in range(records_per_frame)])
                    Body = sb[Header_size:-Footer_size]
                    Body = struct.unpack(self.byteMap_Body, Body)
                    if 'H' in self.byteMap_Body:
                        Body = self.decode_fp2(Body)
                    if i == 0:
                        data = np.array(Body).reshape(-1,len(self.byteMap))
                    else:
                        data = np.concatenate((data,np.array(Body).reshape(-1,len(self.byteMap))),axis=0)
                    i += 1
                else:
                    readFrame = False
            else:
                readFrame = False
        print('Frames ',i)
        if i > 0:
            return (data,np.array(Timestamp).flatten())
        else:
            return (None,None)

    def decode_fp2(self,Body):
        # adapted from: https://github.com/ansell/camp2ascii/tree/cea750fb721df3d3ccc69fe7780b372d20a8160d
        def FP2_map(int):
            sign = (0x8000 & int) >> 15
            exponent =  (0x6000 & int) >> 13 
            mantissa = (0x1FFF & int)       
            if exponent == 0: 
                Fresult=mantissa
            elif exponent == 1:
                Fresult=mantissa*1e-1
            elif exponent == 2:
                Fresult=mantissa*1e-2
            else:
                Fresult=mantissa*1e-3

            if sign != 0:
                Fresult*=-1
            return Fresult
        FP2_ix = [m.start() for m in re.finditer('H', self.byteMap_Body.replace('>','').replace('<',''))]
        Body = list(Body)
        for ix in FP2_ix:
            Body[ix] = FP2_map(Body[ix])
        return(Body)

@dataclass(kw_only=True)
class HOBOcsv(genericLoggerFile):
    sourceFile: str = field(repr=False)
    verbose: bool = field(default=True,repr=False)
    timestampName: str = field(default="Date Time",repr=False)
    fileTimestamp: str = field(default=None,repr=False)
    varRepMap: dict = field(default_factory=dict)
    yearfirst: bool = field(default=True,repr=False)
    statusCols: list = field(default_factory=lambda:['Host Connected', 'Stopped', 'End Of File'],repr=False)

    def __post_init__(self):
        rawFile = open(self.sourceFile,'r',encoding='utf-8-sig')
        T = rawFile.readline().rstrip('\n')
        if not T.startswith('"Plot Title: '):
            self.fileType = False
        self.Data = pd.read_csv(rawFile)
        # ID the timestamp and parse to datetime index
        self.Data.index = self.Data[
            self.Data.columns[self.Data.columns.str.contains(self.timestampName)].values
            ].apply(' '.join, axis=1).apply(dateParse.parse,yearfirst=self.yearfirst)
        self.statusCols = self.Data.columns[self.Data.columns.str.contains('|'.join(self.statusCols))].values
        self.Data[self.statusCols] = self.Data[self.statusCols].ffill(limit=1)
        keep = pd.isna(self.Data[self.statusCols]).all(axis=1)
        self.fileTimestamp = (self.Data.index[(self.Data[self.statusCols].isna()==False).any(axis=1).values].strftime('%Y-%m-%dT%H:%M:%S')).values[-1]
        self.Data = self.Data.loc[keep].copy()
        super().__post_init__()
        self.applySafeNames()

