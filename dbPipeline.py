import os
import sys
import shutil
import fnmatch
import datetime
import siteCoordinates
from dataclasses import dataclass,field
import helperFunctions as helper
import importlib
importlib.reload(siteCoordinates)
importlib.reload(helper)

log = helper.log
now = helper.now

@dataclass(kw_only=True)
class database:
    # Base class containing common functionality across the database
    projectPath: str = None
    fillChar: str = '_'
    sepChar: str = '/'*2
    verbose: bool = False
    projectInfo: dict = field(default_factory=lambda:helper.loadDict(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml')))
    
    def __post_init__(self):
        if self.projectPath:
            if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
                self.make()
            elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')):
                sys.exit('Non-empty, non-project directory provided')
            else:
                self.projectInfo = helper.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
    
    def make(self):
        # make a new database
        self.projectInfo['dateCreated'] = now()
        self.projectInfo['dateModified'] = now()
        for d in self.projectInfo['directoryStructure']:
            os.makedirs(os.path.join(self.projectPath,d))
        self.save()
        
    def save(self,inventory=None,filename=None):
        if self.projectPath:
            self.projectInfo['dateModified'] = helper.now()
            helper.saveDict(self.projectInfo,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)
            if filename:
                log(('Saving: ',filename),ln=False,verbose=self.verbose)
                helper.saveDict(inventory,os.path.join(self.projectPath,'metadata',filename))
                
        elif filename:
            log(('Saving: ',filename),ln=False,verbose=self.verbose)
            helper.saveDict(inventory,filename)

    def projectInventory(self,Sites={},fileSearch=None):
        fpath = os.path.join(self.projectPath,'metadata',Site.fname)
        if os.path.isfile(fpath):
            log(helper.loadDict(fpath))
            self.siteInventory = helper.dictToDataclass(Site,helper.loadDict(fpath),'siteID',constants={'projectPath':self.projectPath})
        else:
            self.siteInventory = {}
        if type(Sites) is str and os.path.isfile(Sites):
            Sites = helper.dictToDataclass(Site,helper.loadDict(Sites),'siteID',constants={'projectPath':self.projectPath})
        elif Sites != {}:
            Sites = helper.dictToDataclass(Site,Sites,'siteID',constants={'projectPath':self.projectPath})
        self.siteInventory = helper.updateDict(self.siteInventory,Sites)
        if self.siteInventory == {}:
            self.siteInventory = helper.dictToDataclass(Site,Site().__dict__,'siteID',constants={'projectPath':self.projectPath})
        self.save(self.siteInventory,fpath)

@dataclass(kw_only=True)
class Measurement:
    # Records pertaining to a measurement set
    measurementID: str = None
    fileType: str = None
    sampleFrequency: str = None
    description: str = None
    latitude: float = None
    longitude: float = None
    startDate: str = None
    stopDate: str = None
    template: bool = field(default=False,repr=False)

    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        if self.measurementID:
            self.measurementID = helper.safeFmt(self.measurementID)
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']

 
@dataclass
class Search:
    # executes a file search using wildcard pattern matching
    # cross references against a list of exiting files
    fname = 'sourceFiles.yml'
    ID: str = None
    sourcePath: str = None
    wildcard: str = '*wildcard*'
    nFiles: int = 0
    nLoaded: int = 0
    nPending: int = 0
    fileList: list = field(default_factory=lambda:[])
    loadList: list = field(default_factory=lambda:[])
    template: bool = field(default=False,repr=False)

    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        elif self.sourcePath and os.path.isdir(self.sourcePath):
            self.sourcePath = os.path.abspath(self.sourcePath)
            self.fileSearch()
        self.ID = os.path.join(self.sourcePath,self.wildcard)

    def fileSearch(self):        
        for dir,_,files in os.walk(self.sourcePath):
            subDir = os.path.relpath(dir,self.sourcePath)
            tmp1 = [os.path.join(subDir,f) for f in files if
                fnmatch.fnmatch(os.path.join(subDir,f),self.wildcard)
                and os.path.join(subDir,f) not in self.fileList
                ]
            tmp2 = [False for l in range(len(tmp1))]
            self.fileList += tmp1
            self.loadList += tmp2
        self.nFiles = len(self.fileList)
        self.nLoaded = sum(self.loadList)
        self.nPending = self.nFiles-self.nLoaded

@dataclass(kw_only=True)
class Site:
    # Records pertaining to a field site, including a record of measurements from the site
    fname = 'siteInventory.yml'
    siteID: str = None
    description: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    Measurements: Measurement = field(default_factory=lambda:Measurement(template=True).__dict__)
    sourceFiles: Search = field(default_factory=lambda:Search(template=True).__dict__,repr=False)
    template: bool = field(default=False,repr=False)
    projectPath: str = field(default=None,repr=False)
    
    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        if self.siteID:
            self.siteID = helper.safeFmt(self.siteID)
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
            if type(list(self.Measurements.values())[0]) is not dict:
                self.Measurements = {'':self.Measurements}
            self.Measurements = helper.dictToDataclass(Measurement,self.Measurements,'measurementID')
            for measurementID in self.Measurements:
                sF = os.path.join(self.projectPath,'metadata',self.siteID,measurementID,Search.fname)
                if not os.path.isfile(sF):
                    sourceFiles = helper.dictToDataclass(Search,self.sourceFiles,['ID'],pop=True)
                else:
                    debug=False
                    sourceFiles = helper.dictToDataclass(Search,helper.loadDict(sF),['ID'],pop=True,debug=debug)
                if len(sourceFiles)>1 and self.sourceFiles['ID'] in sourceFiles:
                    sourceFiles.pop(self.sourceFiles['ID'])
                helper.saveDict(sourceFiles,sF)

@dataclass
class searchInventory:
    # Manages the search class
    projectPath: str = None
    siteID: str = None
    measurementID: str = None
    template: bool = False
    sourceFiles: Search = field(default_factory=lambda:Search(template=True).__dict__)

    def __post_init__(self):
        if self.projectPath is None or not os.path.isdir(self.projectPath):
            return
        sF = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,'sourceFiles.json')
        if self.template and not os.path.isfile(sF):
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        else:
            self.rmID = self.__dataclass_fields__['sourceFiles'].default_factory()['ID']
            self.process(sF)

        if 'ID' in self.sourceFiles:
            ID = self.sourceFiles.pop('ID')
            self.sourceFiles = {ID:self.sourceFiles}
        else:
            if len(self.sourceFiles)>1:
                self.sourceFiles.pop(self.rmID)
        helper.saveDict(self.sourceFiles,sF)

    def process(self,sF):
        if os.path.isfile(sF):
            sourceFiles = helper.loadDict(sF)
            sourceFiles = helper.dictToDataclass(Search,sourceFiles,['ID'],pop=True)
        else:
            sourceFiles = None
        if sourceFiles is None or self.sourceFiles != self.__dataclass_fields__['sourceFiles'].default_factory():
            if type(self.sourceFiles) is str and os.path.isfile(self.sourceFiles):
                self.sourceFiles = helper.loadDict(self.sourceFiles)
            elif type(self.sourceFiles) is not dict:
                log(msg='input must be dict or filepath')
            self.sourceFiles = helper.dictToDataclass(Search,self.sourceFiles,['ID'],pop=True)
            if sourceFiles:
                self.sourceFiles = helper.updateDict(sourceFiles,self.sourceFiles)
        else:
            self.sourceFiles = sourceFiles

