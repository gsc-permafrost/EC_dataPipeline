# Given lat/lon input, script will:
# 
# 1) Autodetect input format
# 2) convert lat/lon to standardized formats
# 3) get UTM zone and coordinates

import re
import utm
from pyproj import CRS
from typing import Literal
from dataclasses import dataclass,field
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic import ConfigDict
import helperFunctions as helper

@pydantic_dataclass(config=ConfigDict(coerce_numbers_to_str=True))
class UTM:
    EPSG: str
    name: str
    x: float
    y: float
    UTM_sig = 3

    def __post_init__(self):
        self.x = round(self.x,self.UTM_sig)
        self.y = round(self.y,self.UTM_sig)
        
@dataclass
class GCS:
    x: float = None
    y: float = None
    datum: Literal['WGS84','NAD83'] = 'WGS84'
    EPSG: str = None

    def __post_init__(self):
        codes = {'WGS84':'4326','NAD83':'4269'}
        self.EPSG = codes[self.datum]

@dataclass
class coordinates:
    latitude: str = None
    longitude: str = None 
    datum: Literal['WGS84','NAD83'] = 'WGS84'
    formatted: dict = field(default_factory=lambda:{'DMS':None,'DDM':None})
    # GCS: dict = field(default_factory=lambda:{'EPSG':None,'latitude':None,'longitude':None})
    degreeString = '°'
    DD_sig = 7
    DDM_sig = 5
    DMS_sig = 1

    def __post_init__(self):
        if self.latitude is None or self.longitude is None:
            self.GCS = GCS(datum=self.datum,x=self.longitude,y=self.latitude).__dict__
            return()
        self.latitude,latDDM,latDMS = self.getDD(str(self.latitude),'NS')
        self.longitude,lonDDM,lonDMS = self.getDD(str(self.longitude),'EW')
        self.formatted['DDM'] = [latDDM,lonDDM]
        self.formatted['DMS'] = [latDMS,lonDMS]
        self.getUTM()
        self.GCS = GCS(datum=self.datum,x=self.longitude,y=self.latitude).__dict__

    def getDD(self,value,hemisphere):
        value = re.sub(r'\b(\d+)S\b|\bS\b|\bS(\d+)\b', r'-\1\2', value)
        value = re.sub(r'\b(\d+)W\b|\bW\b|\bW(\d+)\b', r'-\1\2', value)
        value = re.sub(r'[^0-9,.-]+',',',value)
        value = value.replace(',,',',')
        if '-' in value: 
            self.sign = -1
            hemisphere = hemisphere[1]
        else: 
            self.sign = 1
            hemisphere = hemisphere[0]
        value = value.replace('-','')
        value = [self.sign*float(v) for v in value.split(',') if len(v)>0]
        DD = round(sum([l*m for l,m in zip(value,[1,1/60,1/3600])]),self.DD_sig)
        DDM = hemisphere+str(int(abs(DD)))+self.degreeString+' '+str(round((DD%1)*60,self.DDM_sig))+"`"
        DMS = hemisphere+str(int(abs(DD)))+self.degreeString+' '+str(int((DD%1)*60))+"' "+str(round((DD%1*60)%1*60,self.DMS_sig))+'"'
        return(DD,DDM,DMS)

    def getUTM(self):
        UTM_coords = utm.from_latlon(self.latitude,self.longitude)
        crs = CRS.from_dict({'proj': 'utm', 'zone': UTM_coords[2], 'south': UTM_coords[3]<'N', 'datum': self.datum})
        self.UTM = UTM(EPSG=crs.to_epsg(),
                       name = crs.coordinate_operation.name,
                       x = UTM_coords[0],
                       y = UTM_coords[1]).__dict__
        
class pie:
    def __init__(self):
        pass