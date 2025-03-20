import os
import sys
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
                helper.log(('Saving: ',filename),ln=False,verbose=self.verbose)
                helper.saveDict(inventory,os.path.join(self.projectPath,'metadata',filename))
        elif filename:
            helper.log(('Saving: ',filename),ln=False,verbose=self.verbose)
            helper.saveDict(inventory,filename)

@dataclass(kw_only=True)
class Measurment:
    measurementID: str = None
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
    Measurments: Measurment = field(default_factory=lambda:Measurment(template=True).__dict__)
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
            if type(list(self.Measurments.values())[0]) is not dict:
                self.Measurments = {'':self.Measurments}
            Measurments = {}
            for value in self.Measurments.values():
                measurment = Measurment(**value)
                Measurments[measurment.measurementID] = helper.reprToDict(measurment)
            self.Measurments = Measurments

@dataclass
class SourceFiles:
    fname = 'sourceFiles.json'
    uID:str = field(default=None,repr=False)
    sourcePath: str = field(default=None,repr=False)
    fileType: str = field(default=None,repr=False)
    fileExt: str = field(default=None,repr=False)
    matchPattern: list = field(default=None,repr=False)
    excludePattern: list = field(default=None,repr=False)
    template: bool = field(default=False,repr=False)
    fileList: list = field(default_factory=lambda:[])
    loadList: list = field(default_factory=lambda:[])

    def __post_init__(self):
        if self.sourcePath is not None and os.path.isdir(self.sourcePath) and self.fileExt:
            for dir,_,files in os.walk(self.sourcePath):
                subDir = os.path.relpath(dir,self.sourcePath)
                tmp1 = [os.path.join(subDir,f) for f in files 
                    if f.endswith(self.fileExt)
                    and sum([m in os.path.join(subDir,f) for m in self.matchPattern]) == len(self.matchPattern)
                    and sum([e in os.path.join(subDir,f) for e in self.excludePattern]) == 0
                    and os.path.join(subDir,f) not in self.fileList
                    ]
                tmp2 = [False for l in range(len(tmp1))]
                self.fileList += tmp1
                self.loadList += tmp2

@dataclass
class Search:
    fname = 'searchInventory.yml'
    uID: str = None
    sourcePath: str = None
    fileType: str = None
    fileExt: str = None
    matchPattern: list = field(default_factory=lambda:[])
    excludePattern: list = field(default_factory=lambda:[])
    nFiles: int = 0
    nLoaded: int = 0
    nPending: int = 0
    fileList: list = field(default_factory=lambda:[],repr=False)
    loadList: list = field(default_factory=lambda:[],repr=False)
    template: bool = field(default=False,repr=False)

    def __post_init__(self):
        if self.uID is None:
            self.uID = helper.now(prefix='uID ')
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
                self.fileList = ['someFile',False]
        elif self.sourcePath and os.path.isdir(self.sourcePath):
            self.sourcePath = os.path.abspath(self.sourcePath)

        self.nFiles = len(self.fileList)
        self.nLoaded = sum(self.loadList)
        self.nPending = self.nFiles-self.nLoaded
        print(self.__repr__())

@dataclass
class searchInventory:
    projectPath: str = None
    siteID: str = None
    measurementID: str = None
    searchInventory: Search = field(default_factory=lambda:Search(template=True).__dict__)

    def __post_init__(self):
        sI = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,Search.fname)
        if os.path.isfile(sI):
            searchInventory = helper.loadDict(sI)
            searchInventory = helper.dictToDataclass(Search,searchInventory,'uID')
        else:
            searchInventory = None
        if searchInventory is None or self.searchInventory != self.__dataclass_fields__['searchInventory'].default:
            if type(self.searchInventory) is str and os.path.isfile(self.searchInventory):
                self.searchInventory = helper.loadDict(self.searchInventory)
            elif type(self.searchInventory) is not dict:
                helper.log(msg='input must be dict or filepath')

            self.searchInventory = helper.dictToDataclass(Search,self.searchInventory,'uID')
            if searchInventory:
                for key,values in searchInventory.items():
                    keys = list(self.searchInventory.keys())
                    for k in keys:
                        if not helper.compareDicts(values,self.searchInventory[k],exclude_keys=['uID']):
                            self.searchInventory.pop(k)
                self.searchInventory = helper.updateDict(searchInventory,self.searchInventory,verbose=True)
        else:
            self.searchInventory = searchInventory
    
        # sF = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,SourceFiles.fname)
        # self.sourceFiles = helper.loadDict(sF)
        # for uID,kwargs in self.searchInventory.items():
        #     if uID in self.sourceFiles:
        #         kwargs['fileList'] = self.sourceFiles[uID]['fileList']
        #     self.sourceFiles = helper.updateDict(self.sourceFiles,helper.dictToDataclass(SourceFiles,kwargs,'uID'))
        #     for uID,fileList in self.sourceFiles.items():
        #         print(fileList)
        #     if 'fileList' in kwargs:
        #         kwargs.pop('fileList')
        # helper.saveDict(self.searchInventory,sI)
        # helper.saveDict(self.sourceFiles,sF)
        

@dataclass(kw_only=True)
class projectInventory(database):
    Sites: Site = field(default_factory=lambda:Site(template=True).__dict__)
    # SourceFiles: Source = field(default_factory=lambda:{'siteID':{'measurementID':Source(template=True).__dict__}})

    def __post_init__(self):
        super().__post_init__()
        self.fpath = os.path.join(self.projectPath,'metadata',Site().fname)
        if type(self.Sites) is str and os.path.isfile(self.Sites):
            self.Sites = helper.loadDict(self.Sites)
        elif os.path.isfile(self.fpath) and self.Sites == self.__dataclass_fields__['Sites'].default:
            self.Sites = helper.loadDict(self.fpath)
        self.Sites = helper.dictToDataclass(Site,self.Sites,'siteID')
        self.save(self.Sites,self.fpath)



        # for siteID in self.Sites:
        #     for measurementID in self.Sites[siteID]['Measurments']:
        #         searchInventory(self.projectPath,siteID,measurementID)



        #         print(siteID,measurementID)
        #         self.fpath = os.path.join(self.projectPath,'metadata',siteID,measurementID,Source().fname)
        #         if siteID in self.SourceFiles and measurementID in self.SourceFiles[siteID]:
        #             helper.updateDict(SourceFiles,{siteID:{measurementID:helper.dumpToDc(Source,self.SourceFiles[siteID][measurementID],'uID')}})
        #             self.save(SourceFiles[siteID][measurementID],self.fpath)
        #         elif os.path.isfile(self.fpath):
        #             helper.updateDict(SourceFiles,{siteID:{measurementID:helper.dumpToDc(Source,helper.loadDict(self.fpath),'uID')}})
        #             self.save(SourceFiles[siteID][measurementID],self.fpath)                    
        #         else:
        #             helper.updateDict(SourceFiles,{siteID:{measurementID:helper.dumpToDc(Source,{'template':True},'uID')}})
        #             self.save(SourceFiles[siteID][measurementID],self.fpath)
        #         # else:
        # self.SourceFiles = SourceFiles
                        
    # def dumpToDc(self,method,toDump,ID=None):
    #     if ID is None:
    #         tmp = method(**toDump)
    #     else:
    #         if type(list(toDump.values())[0]) is not dict:
    #             toDump = {'':toDump}
    #         tmp = {}
    #         for value in toDump.values():
    #             t = method(**value)
    #             print(t)
    #             tmp[t.__dict__[ID]] = helper.reprToDict(t)
    #     return(tmp)
        # elif os.path.isfile(self.fpath) and self.Sites == self.__dataclass_fields__['Sites'].default:
        #     self.Sites = helper.loadDict(self.fpath)






# @dataclass(kw_only=True)
# class fileInvnentory:
#     sourceID: str = 'sourceID'
#     fileType: str = None
#     fileExt: str = None
#     sourcePath: str = None
#     matchPattern: list = field(default_factory=lambda:[])
#     excludePattern: list = field(default_factory=lambda:[])
#     fileList: list = field(default_factory=lambda:[])
    
#     def __post_init__(self):

#         for dir,_,files in os.walk(self.sourcePath):
#             subDir = os.path.relpath(dir,self.sourcePath)
#             self.fileList += [[os.path.join(subDir,f),False] for f in files 
#                 if f.endswith(self.fileExt)
#                 and sum([m in f for m in self.matchPattern]) == len(self.matchPattern)
#                 and sum([e in f for e in self.excludePattern]) == 0
#                 and [os.path.join(subDir,f),False] not in self.fileList
#                 and [os.path.join(subDir,f),True] not in self.fileList
#                 ]
        
# @dataclass(kw_only=True)
# class fileSearch:
#     sourcePath: str = os.getcwd()
#     fileType: str = None
#     fileExt: str = None
#     matchPattern: list = field(default_factory=lambda:[])
#     excludePattern: list = field(default_factory=lambda:[])
    
#     def __post_init__(self):
#         # self.sourceID = helper.safeFmt(self.sourceID)
#         for f,v in self.__dataclass_fields__.items():
#             if v.type is list:
#                 if type(self.__dict__[f]) is not list:
#                     self.__dict__[f] = [self.__dict__[f]]
#         if self.sourcePath:
#             self.sourcePath = os.path.abspath(self.sourcePath)
#         pass
