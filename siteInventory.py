######################################################################################################################
# Data Source Management
######################################################################################################################
from dataclasses import dataclass,field
# import helperFunctions as helper
from parseCoordinates import parseCoordinates
from parseFiles.helperFunctions.updateDict import updateDict
from parseFiles.helperFunctions.asdict_repr import asdict_repr
from parseFiles.helperFunctions.log import log
from pathlib import Path
import geopandas as gpd
import pandas as pd
import fnmatch
import json
import yaml
import copy
import os
import re

def safeFormat(string,safeCharacters='[^0-9a-zA-Z]+',safeFill='_'):
    return(re.sub(safeCharacters,safeFill, str(string)))

@dataclass(kw_only=True)
class sourceRecord:
    # executes a file search using wildcard pattern matching cross references against a list of exiting files
    matchPattern: str = 'w*ldcard'
    rootPath: str = None
    fileList: list = field(default_factory=lambda:{},repr=False)
    parserSettings: dict = field(default_factory=lambda:{})

    def __post_init__(self):
        if self.rootPath and os.path.isdir(self.rootPath):
            self.rootPath = os.path.abspath(self.rootPath)

    def __find__(self,fileList=None):
        if fileList is not None:
            self.fileList = fileList            
        if self.rootPath and os.path.isdir(self.rootPath):
            for dir,_,files in os.walk(self.rootPath):
                self.fileList = self.fileList | {os.path.join(dir,f):{'loaded':False,
                                                                      }
                            for f in files if
                            fnmatch.fnmatch(os.path.join(dir,f),self.matchPattern) and
                            os.path.join(dir,f) not in self.fileList
                }

@dataclass(kw_only=True)
class measurementRecord:
    # Records pertaining to a measurement set
    measurementID: str = '.TMP'
    description: str = 'This is a template for defining measurement-level metadata'
    fileType: str = None
    sampleFrequency: str = None
    description: str = None
    latitude: float = None
    longitude: float = None
    startDate: str = None
    stopDate: str = None
    sourceFiles: sourceRecord = field(default_factory=lambda:{k:v for k,v in sourceRecord.__dict__.items() if k[0:2] != '__'})
    template: bool = field(default=False,repr=False)
    dpath: str = field(default=None,repr=False)

    def __post_init__(self):
        if self.measurementID:
            self.measurementID = safeFormat(self.measurementID)
            self.coordinates = parseCoordinates(ID=self.measurementID,latitude=self.latitude,longitude=self.longitude,attributes={'description':self.description,'pointClass':type(self).__name__})
            self.latitude,self.longitude=self.coordinates.latitude,self.coordinates.longitude
            if type(list(self.sourceFiles.values())[0]) is not dict:
                self.sourceFiles = {'':self.sourceFiles}
            sobj = map(lambda values :sourceRecord(**values),self.sourceFiles.values())
            self.sourceFiles = {s.matchPattern:asdict_repr(s) for s in sobj}
            # log('replace repr2dict method')
            # for key in self.sourceFiles:
            #     self.sourceFiles[key].pop('fileList')
            if len(self.sourceFiles)>1 and self.__dataclass_fields__['sourceFiles'].default_factory()['matchPattern'] in self.sourceFiles:
                self.sourceFiles.pop(self.__dataclass_fields__['sourceFiles'].default_factory()['matchPattern'])                

@dataclass(kw_only=True)
class siteRecord:
    # Records pertaining to a field site, including a record of measurements from the site
    siteID: str = '.TMP'
    description: str = 'This is a template for defining site-level metadata which can be used as an example'
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    coordinates: parseCoordinates = field(default_factory=lambda:parseCoordinates(),repr=False)
    geojson: dict = field(default_factory=lambda:{},repr=False)
    geodataframe: gpd.GeoDataFrame = field(default_factory=lambda:gpd.GeoDataFrame(),repr=False)
    Measurements: measurementRecord = field(default_factory=lambda:{k:v for k,v in measurementRecord.__dict__.items() if k[0:2] != '__'})
    dpath: str = field(default=None,repr=False)
    
    def __post_init__(self):
        if self.siteID:
            self.siteID = safeFormat(self.siteID)
            if self.latitude and self.longitude:
                self.coordinates = parseCoordinates(ID=self.siteID,latitude=self.latitude,longitude=self.longitude,attributes={'description':self.description,'pointClass':type(self).__name__})
                self.latitude,self.longitude=self.coordinates.latitude,self.coordinates.longitude
                self.geojson = self.coordinates.geojson
                self.geodataframe = self.coordinates.geodataframe
            if type(list(self.Measurements.values())[0]) is not dict:
                self.Measurements = {'':self.Measurements}
            # map the measurements and unpack to dict
            Measurements = map(lambda key :measurementRecord(**self.Measurements[key]),self.Measurements)
            self.Measurements = {measurement.measurementID:measurement for measurement in Measurements}
            for measurementID in self.Measurements:
                if self.Measurements[measurementID].coordinates == {}:
                    pass
                elif self.geojson == {}:
                    self.geojson = self.Measurements[measurementID].coordinates.geojson
                    self.geodataframe = self.Measurements[measurementID].coordinates.geodataframe
                else:
                    self.geojson = updateDict(self.geojson,self.Measurements[measurementID].coordinates.geojson,overwrite='append')
                    self.geodataframe = pd.concat([self.geodataframe,self.Measurements[measurementID].coordinates.geodataframe])
            self.Measurements = {measurementID:asdict_repr(self.Measurements[measurementID]) for measurementID in self.Measurements}

@dataclass(kw_only=True)
class siteInventory:
    Sites: dict = None
    verbose: bool = False
    spatialInventory: dict = field(default_factory=lambda:{})
    mapTemplate: str = field(default_factory=lambda:Path(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','MapTemplate.html')).read_text())


    def __post_init__(self):
        if type(self.Sites) is str and os.path.isfile(self.Sites):
            with open(self.Sites) as f:
                self.Sites = yaml.safe_load(f)
        Sites = map(lambda key: siteRecord(**self.Sites[key]),self.Sites)
        self.Sites = {site.siteID:site for site in Sites}
        for siteID in self.Sites:
            if self.Sites[siteID].coordinates == {}:
                pass
            elif self.spatialInventory == {}:
                self.spatialInventory['geojson'] = self.Sites[siteID].geojson
                self.spatialInventory['geodataframes'] = {siteID:self.Sites[siteID].geojson}
            else:
                self.spatialInventory['geojson'] = updateDict(self.spatialInventory['geojson'],self.Sites[siteID].geojson,overwrite='append',verbose=self.verbose)
                self.spatialInventory['geodataframes'][siteID] = self.Sites[siteID].geojson

        self.siteInventory = {siteID:asdict_repr(self.Sites[siteID]) for siteID in self.Sites}
        if 'geojson' in self.spatialInventory:
            self.mapTemplate = self.mapTemplate.replace('fieldSitesJson',json.dumps(self.spatialInventory['geojson']))

        self.Sites = {siteID:asdict_repr(self.Sites[siteID]) for siteID in self.Sites}
