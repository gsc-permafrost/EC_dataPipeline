import os
import sys
import datetime
import siteCoordinates
from dataclasses import dataclass,field
import helperFunctions as helper
import importlib
importlib.reload(siteCoordinates)
importlib.reload(helper)

@dataclass
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
                self.save()
                            
    def now(self,fmt='%Y-%m-%d %H:%M:%S'):
        return(datetime.datetime.now().strftime(fmt))
    
    def make(self):
        self.projectInfo['dateCreated'] = self.now()
        self.projectInfo['dateModified'] = self.now()
        for d in self.projectInfo['directoryStructure']:
            os.makedirs(os.path.join(self.projectPath,d))
        self.save()
        
    def save(self,inventory=None,filename=None):

        self.projectInfo['dateModified'] = self.now()
        helper.saveDict(self.projectInfo,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)
        if filename:
            print('Saving: ',filename)
            helper.saveDict(inventory,os.path.join(self.projectPath,'metadata',filename))

@dataclass
class Measurment:
    measurementID: str = 'measurementID'
    sampleFrequency: str = None
    description: str = None
    latitude: float = None
    longitude: float = None
    startDate: str = None
    stopDate: str = None

    def __post_init__(self):
        self.measurementID = helper.safeFmt(self.measurementID)
        coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
        self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']

@dataclass
class Site:
    siteID: str = 'siteID'
    description: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    Measurments: Measurment = field(default_factory=lambda:Measurment().__dict__)
    
    def __post_init__(self):
        self.siteID = helper.safeFmt(self.siteID)
        coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
        self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
        if type(list(self.Measurments.values())[0]) is dict:
            self.Measurments = {Measurment(**value).measurementID:Measurment(**value).__dict__ for key,value in self.Measurments.items()}
        else:
            self.Measurments = {Measurment(**self.Measurments).measurementID:Measurment(**self.Measurments).__dict__}
            

@dataclass
class inventory(database):
    Sites: Site = field(default_factory=lambda:Site().__dict__)
    fpath: str = 'projectInventory.yml'

    def __post_init__(self):
        print(os.path.isfile(self.fpath))
        if os.path.isfile(self.fpath):
            self.Sites = helper.loadDict(self.fpath)
            self.fpath = self.__dataclass_fields__['fpath'].default
        super().__post_init__()
        self.fpath = os.path.join(self.projectPath,'metadata',self.fpath)
        if self.Sites == Site().__dict__ and os.path.isfile(self.fpath):
            self.Sites = helper.loadDict(self.fpath)
        if type(list(self.Sites.values())[0]) is dict:
            self.Sites = {Site(**value).siteID:Site(**value).__dict__ for key,value in self.Sites.items()}
        else:
            self.Sites = {Site(**self.Sites).siteID:Site(**self.Sites).__dict__}
        for siteID in self.Sites:
            for measurementID in self.Sites[siteID]['Measurments']:
                src = sourceFiles(siteID,measurementID,root=os.path.join(self.projectPath,'metadata'))
        # self.save(self.Sites,self.fpath)

@dataclass
class fileInvnentory:
    sourceID: str = 'sourceID'
    fileType: str = None
    fileExt: str = None
    sourcePath: str = None
    matchPattern: list = field(default_factory=lambda:[])
    excludePattern: list = field(default_factory=lambda:[])
    fileList: list = field(default_factory=lambda:[])
    
    def __post_init__(self):

        for dir,_,files in os.walk(self.sourcePath):
            subDir = os.path.relpath(dir,self.sourcePath)
            self.fileList += [[os.path.join(subDir,f),False] for f in files 
                if f.endswith(self.fileExt)
                and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
                and sum([e in f for e in self.excludePattern]) == 0
                and [os.path.join(subDir,f),False] not in self.fileList
                and [os.path.join(subDir,f),True] not in self.fileList
                ]
        
@dataclass
class fileSearch:
    sourceID: str = 'sourceID'
    fileType: str = None
    fileExt: str = None
    sourcePath: str = None
    matchPattern: list = field(default_factory=lambda:[])
    excludePattern: list = field(default_factory=lambda:[])
    
    def __post_init__(self):
        self.sourceID = helper.safeFmt(self.sourceID)
        for f,v in self.__dataclass_fields__.items():
            if v.type is list:
                if type(self.__dict__[f]) is not list:
                    self.__dict__[f] = [self.__dict__[f]]
        if self.sourcePath:
            self.sourcePath = os.path.abspath(self.sourcePath)
        pass

@dataclass
class sourceFiles(database):
    siteID: str = 'siteID'
    measurementID: str = 'measurementID'
    search: fileSearch = field(default_factory=lambda:fileSearch().__dict__)
    sourceSearch: str = 'sourceSearch.yml'
    sourceInventory: str = 'sourceInventory.json'
    root: str = ''

    def __post_init__(self):
        super().__post_init__()
        if type(list(self.search.values())[0]) is dict:
            self.search = {fileSearch(**value).sourceID:fileSearch(**value).__dict__ for key,value in self.search.items()}
        else:
            self.search = {fileSearch(**self.search).sourceID:fileSearch(**self.search).__dict__}
        self.sourceSearch = os.path.join(self.root,self.siteID,self.measurementID,self.sourceSearch)
        self.save(self.search,self.sourceSearch)
        self.sourceInventory = os.path.join(self.root,self.siteID,self.measurementID,self.sourceInventory)
        if os.path.isfile(self.sourceInventory):
            self.inventory = helper.loadDict(self.sourceInventory)
        else:
            self.inventory = {}
        for k in self.search.keys():
            if k not in self.inventory:
                self.inventory[k] = []
        self.inventory = {k:fileInvnentory(fileList=self.inventory[k],**v).fileList for k,v in self.search.items() if v['sourcePath']}
        self.save(self.inventory,self.sourceInventory)

