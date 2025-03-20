import os
import sys
import fnmatch
import datetime
import siteCoordinates
from dataclasses import dataclass,field
import helperFunctions as helper
import importlib
importlib.reload(siteCoordinates)
importlib.reload(helper)

log = helper.log

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
        self.projectInfo['dateCreated'] = helper.now()
        self.projectInfo['dateModified'] = helper.now()
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

@dataclass(kw_only=True)
class Measurement:
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

@dataclass(kw_only=True)
class Site:
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
    template: bool = field(default=False,repr=False)
    
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

@dataclass
class Search:
    ID: str = None
    sourcePath: str = None
    wildcard: str = '*'
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

@dataclass
class searchInventory:
    projectPath: str = None
    siteID: str = None
    measurementID: str = None
    template: bool = False
    searchInventory: Search = field(default_factory=lambda:Search(template=True).__dict__)

    def __post_init__(self):
        if self.projectPath is None or not os.path.isdir(self.projectPath):
            return
        sI = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,'searchInventory.yml')
        sF = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,'sourceFiles.json')
        if self.template and not os.path.isfile(sI):
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        else:
            self.rmID = self.__dataclass_fields__['searchInventory'].default_factory()['ID']
            self.process(sF,sI)
        self.sourceFiles = {}
        for ID in self.searchInventory.keys():
            self.sourceFiles[ID]={
                'fileList':self.searchInventory[ID].pop('fileList'),
                'loadList':self.searchInventory[ID].pop('loadList')}
        if len(list(self.searchInventory.keys()))>1:
            self.searchInventory.pop(self.rmID)
            self.sourceFiles.pop(self.rmID)
        helper.saveDict(self.searchInventory,sI)
        helper.saveDict(self.sourceFiles,sF)

    def process(self,sF,sI):
        if os.path.isfile(sI):
            searchInventory = helper.loadDict(sI)
            sourceFiles = helper.loadDict(sF)
            for ID in searchInventory:
                searchInventory[ID]['fileList'] = sourceFiles[ID]['fileList']
                searchInventory[ID]['loadList'] = sourceFiles[ID]['loadList']
            searchInventory = helper.dictToDataclass(Search,searchInventory,['ID'],pop=True)
        else:
            searchInventory = None
        if searchInventory is None or self.searchInventory != self.__dataclass_fields__['searchInventory'].default_factory():
            if type(self.searchInventory) is str and os.path.isfile(self.searchInventory):
                self.searchInventory = helper.loadDict(self.searchInventory)
            elif type(self.searchInventory) is not dict:
                log(msg='input must be dict or filepath')
            self.searchInventory = helper.dictToDataclass(Search,self.searchInventory,['ID'],pop=True)
            if searchInventory:
                for key,values in searchInventory.items():
                    keys = list(self.searchInventory.keys())
                    for k in keys:
                        comp = helper.compareDicts(values,self.searchInventory[k])
                        if not comp:
                            self.searchInventory.pop(k)
                        else:
                            # log(self.searchInventory[k])
                            log(comp)

                self.searchInventory = helper.updateDict(searchInventory,self.searchInventory)#,verbose=True)
        else:
            self.searchInventory = searchInventory
        

@dataclass(kw_only=True)
class projectInventory(database):
    Sites: Site = field(default_factory=lambda:Site(template=True).__dict__)
    fileSearch: searchInventory = field(default_factory=lambda:searchInventory(template=True).__dict__)

    def __post_init__(self):
        super().__post_init__()
        self.fpath = os.path.join(self.projectPath,'metadata',Site().fname)
        if type(self.Sites) is str and os.path.isfile(self.Sites):
            self.Sites = helper.loadDict(self.Sites)
        elif os.path.isfile(self.fpath) and self.Sites == self.__dataclass_fields__['Sites'].default:
            self.Sites = helper.loadDict(self.fpath)
        self.Sites = helper.dictToDataclass(Site,self.Sites,'siteID')
        for siteID in self.Sites:
            for measurementID in self.Sites[siteID]['Measurements']:
                if siteID in self.fileSearch and measurementID in self.fileSearch[siteID]:
                    log(self.fileSearch[siteID][measurementID])
                    helper.dictToDataclass(searchInventory,self.fileSearch[siteID][measurementID],constants={'projectPath':self.projectPath,'siteID':siteID,'measurementID':measurementID},pad=True)
                else:
                    helper.dictToDataclass(searchInventory,searchInventory(template=True).__dict__,constants={'projectPath':self.projectPath,'siteID':siteID,'measurementID':measurementID},pad=True)
        self.save(self.Sites,self.fpath)



