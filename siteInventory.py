######################################################################################################################
# Data Source Management
######################################################################################################################
from dataclasses import dataclass,field
import helperFunctions as helper
import siteCoordinates
import os

@dataclass(kw_only=True)
class sourceRecord:
    # executes a file search using wildcard pattern matching cross references against a list of exiting files
    sourceID: str = os.path.join('sourcePath','*wildcard*')
    sourcePath: str = 'sourcePath'
    wildcard: str = '*wildcard*'
    parserKwargs: dict = field(default_factory=lambda:{})

    def __post_init__(self):
        if self.sourcePath != 'sourcePath' and os.path.isdir(self.sourcePath):
            self.sourcePath = os.path.abspath(self.sourcePath)
        self.sourceID = os.path.join(self.sourcePath,self.wildcard)

@dataclass(kw_only=True)
class measurementRecord:
    # Records pertaining to a measurement set
    measurementID: str = '.measurementID'
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
            if self.measurementID != '.measurementID':
                self.measurementID = helper.safeFmt(self.measurementID)
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
            if type(list(self.sourceFiles.values())[0]) is not dict:
                self.sourceFiles = {'':self.sourceFiles}
            if self.dpath:
                pth = os.path.join(self.dpath,self.measurementID)
            else:
                pth = None
            self.sourceFiles = helper.dictToDataclass(sourceRecord,self.sourceFiles,ID=['sourceID'],constants={'dpath':pth},pop=True)
            if len(self.sourceFiles)>1 and self.__dataclass_fields__['sourceFiles'].default_factory()['sourceID'] in self.sourceFiles:
                self.sourceFiles.pop(self.__dataclass_fields__['sourceFiles'].default_factory()['sourceID'])                

@dataclass(kw_only=True)
class siteRecord:
    # Records pertaining to a field site, including a record of measurements from the site
    siteID: str = '.siteID'
    description: str = 'This is a template for defining site-level metadata'
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    Measurements: measurementRecord = field(default_factory=lambda:{k:v for k,v in measurementRecord.__dict__.items() if k[0:2] != '__'})
    dpath: str = field(default=None,repr=False)
    
    def __post_init__(self):
        if self.siteID:
            if self.siteID != '.siteID':
                self.siteID = helper.safeFmt(self.siteID)
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
            if type(list(self.Measurements.values())[0]) is not dict:
                self.Measurements = {'':self.Measurements}
            if self.dpath:
                pth = os.path.join(self.dpath,self.siteID)
            else:
                pth = None
            self.Measurements = helper.dictToDataclass(measurementRecord,self.Measurements,ID=['measurementID'],constants={'dpath':pth})