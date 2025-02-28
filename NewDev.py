import re
import os
import yaml
import copy
import numpy as np
import pandas as pd
from typing import Literal
import siteCoordinates
import helperFunctions
from dataclasses import dataclass, field

@dataclass(kw_only=True)
class site:
    siteID: str = None
    description: str = field(default=None,repr=False)
    name: str = field(default=None,repr=False)
    PI: str = field(default=None,repr=False)
    dateEstablished: str = field(default=None,repr=False)
    landCoverType: str = field(default=None,repr=False)
    longitude: float = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)

@dataclass(kw_only=True)
class measurement:
    measurementType: str = '.site'
    replicateID: str = '.base'
    longitude: float = field(default=None,repr=False)
    latitude: float = field(default=None,repr=False)

@dataclass
class metadataRecord(measurement,site):
    # Formats a metadata entry for either a full site or a specific measurement
    fillChar = '_'
    sepChar = '-'
    entry: dict = field(default_factory=dict,repr=False)
    def __post_init__(self):
        if self.siteID is None:
            return
        for k,v in self.__dataclass_fields__.items():
            if self.__dict__[k] is not None and v.kw_only:
                if self.__dataclass_fields__[k].repr:
                    self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.fillChar, self.__dict__[k])
                elif 'date' in k:
                    self.__dict__[k] = re.sub('[^0-9a-zA-Z.]+',self.sepChar, self.__dict__[k])
        coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
        self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        self.ID = self.sepChar.join([str(self.__dict__[k]) for k in self.__dataclass_fields__.keys() 
                                       if self.__dataclass_fields__[k].repr])
        for f,v in self.__dataclass_fields__.items():
            if v.kw_only: 
                self.entry.setdefault(self.ID,{}).setdefault(f,self.__dict__[f])

class siteInventory(metadataRecord):
    def __init__(self,source,overwrite=False,**kwargs):
        self.siteInventory = {}
        self.overwrite = overwrite
        print(source)
        self.load(source)
        if kwargs != {}:
            self.update(kwargs)
        self.save(source)

    def load(self,source):
        if os.path.isfile(os.path.join(source,'siteInventory.yml')):
            with open(os.path.join(source,'siteInventory.yml')) as f:
                inv = yaml.safe_load(f)
                if inv is not None:
                    self.siteInventory = helperFunctions.unpackDict(inv,'-',limit=2)
        if os.path.isfile(os.path.join(source,'siteInventory.csv')):
            df = pd.read_csv(os.path.join(source,'siteInventory.csv'),index_col=[0])
            df = df.fillna(np.nan).replace([np.nan], [None])
            print(df)
            for ix,row in df.iterrows():
                if ix not in self.siteInventory.keys():
                    self.update(row.to_dict())
    
    def update(self,kwargs):
        args = {}
        for k,v in kwargs.items():
            if k in self.__dataclass_fields__.keys():
                args[k] = v
        self.record = metadataRecord(**args)
        self.siteInventory = helperFunctions.updateDict(self.siteInventory,self.record.entry,overwrite=self.overwrite)
            
    def save(self,source):
        with open(os.path.join(source,'siteInventory.yml'),'w+') as f:
            # Sort alphabetically by ID, maintaining order of metadata
            # Hacked up to maintain desirable sort order regardless of case or pattern used to represent "blank" values
            self.siteInventory = {key.replace(self.fillChar,' '):value for key,value in self.siteInventory.items()}
            self.siteInventory = {key.replace(' ',self.fillChar):value for key,value in dict(sorted(self.siteInventory.items())).items()}
            self.siteInventory = helperFunctions.packDict(copy.deepcopy(self.siteInventory),'-',limit=2,order=1)
            yaml.safe_dump(self.siteInventory,f,sort_keys=False)
        self.siteInventory = helperFunctions.unpackDict(self.siteInventory,'-',limit=2)
        index,data = [k for k in self.siteInventory.keys()],[v for v in self.siteInventory.values()]
        df = pd.DataFrame(data = self.siteInventory.values(), index = index)
        df.to_csv(os.path.join(source,'siteInventory.csv'))