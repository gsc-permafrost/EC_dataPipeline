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
    siteIDs: list = field(default_factory=lambda:[])
    siteInventory: dict = field(default_factory=lambda:{})
    projectInfo: dict = field(default_factory=lambda:helper.loadDict(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml')))
    
    def __post_init__(self):
        if self.projectPath:
            if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
                self.makeNewProject()
            elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')):
                sys.exit('Non-empty, non-project directory provided')
            else:
                self.projectInfo = helper.loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
            if type(self.siteIDs) != list:
                self.siteIDs = [self.siteIDs]
            elif self.siteIDs == []:
                self.siteIDs = [siteID for siteID in self.projectInfo['Sites'] if not siteID.startswith('.')]
            
    def makeNewProject(self):
        # make a new database
        self.projectInfo['.dateCreated'] = now()
        self.projectInfo['.dateModified'] = now()
        for d in self.projectInfo:
            if not d.startswith('.'):
                os.makedirs(os.path.join(self.projectPath,d))
        self.projectInventory()
        
    def save(self,dictObj=None,filename=None):
        # Saves projectInfo.yml and any other relevant metadata files
        if self.projectPath:
            self.projectInfo['.dateModified'] = helper.now()
            helper.saveDict(self.projectInfo,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True)
            if filename:
                log(('Saving: ',filename),verbose=self.verbose)
                helper.saveDict(dictObj,filename)

    def projectInventory(self,newSites={},fileSearch=None):
        # Read existing sites
        for siteID in self.siteIDs:
            record = helper.loadDict(os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))
            self.siteInventory[siteID] = helper.dictToDataclass(siteRecord,record,constants={'dpath':os.path.join(self.projectPath,'Sites')})
        # If given a file template for new sites
        if type(newSites) is str and os.path.isfile(newSites):
            newSites = helper.loadDict(newSites)
        # otherwise check for manual additions
        elif newSites == {}:
            additions = [siteID for siteID in os.listdir(os.path.join(self.projectPath,'Sites'))
                        if '.' not in siteID and siteID not in self.siteIDs]
            if additions != []:
                newSites = {new:helper.loadDict(os.path.join(self.projectPath,'Sites',new,f"{new}_metadata.yml")) for new in additions}
        # If there are any new sits, process them        
        if newSites != {}:
            newSites = helper.dictToDataclass(siteRecord,newSites,ID=['siteID'],constants={'dpath':os.path.join(self.projectPath,'Sites')})
            self.siteInventory = helper.updateDict(self.siteInventory,newSites)
        # If no sites exist yet, create a template
        if self.siteInventory == {}:
            self.siteInventory = helper.dictToDataclass(siteRecord,siteRecord().__dict__,ID=['siteID'],constants={'dpath':os.path.join(self.projectPath,'Sites'),'template':True},debug=True)
        for siteID,values in self.siteInventory.items():
            self.projectInfo['Sites'][siteID] = values['Name']
            self.save(values,os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))

    def rawFileSearch(self,siteID,measurementID,sourcePath=None,wildcard=None):
        sF = os.path.join(self.projectPath,'metadata',siteID,measurementID,Search.fname)
        if not os.path.isfile(sF):
            log(f"Does not exist: {sF}")
            sys.exit()
        else:
            sourceFiles = helper.loadDict(sF)
            if sourcePath and wildcard and os.path.isdir(sourcePath):
                helper.updateDict(sourceFiles,helper.dictToDataclass(Search,{'sourcePath':sourcePath,'wildcard':wildcard},'ID',pop=True))
            else:
                sourceFiles = helper.dictToDataclass(Search,sourceFiles,'ID',pop=True)
        if len(sourceFiles)>1 and Search(template=True).ID in sourceFiles:
            sourceFiles.pop(Search(template=True).ID)
        self.save(sourceFiles,sF)

    def rawFileImport(self,siteID,measurementID,sourcePath=None,wildcard=None):
        sF = os.path.join(self.projectPath,'metadata',siteID,measurementID,Search.fname)
        if not os.path.isfile(sF):
            log(f"Does not exist: {sF}")
            sys.exit()
        else:
            sourceFiles = helper.loadDict(sF)
            for key,value in sourceFiles.items():
                if value['nPending']>0:
                    load = [value['fileList'][i] for i,v in enumerate(value['loadList']) if not v]
                    load = load[:2]
                    value['loadList'] = [True if f in load else False for f in value['fileList']]
                sourceFiles[key]=value
        self.save(sourceFiles,sF)


######################################################################################################################
# Data Source Management
######################################################################################################################

@dataclass(kw_only=True)
class fileSearch:
    # sourceID: str = None
    sourcePath: str = field(default=None,repr=None)
    wildcard: str = field(default='*wildcard*',repr=None)
    fileList: list = field(default_factory=lambda:[])
    loadList: list = field(default_factory=lambda:[])
    inventory: dict = field(default_factory=lambda:{},repr=False)
    # dpath: str

    def __post_init__(self): 
        if self.sourcePath and os.path.isdir(self.sourcePath):
            for dir,_,files in os.walk(self.sourcePath):
                subDir = os.path.relpath(dir,self.sourcePath)
                tmp1 = [os.path.join(subDir,f) for f in files if
                    fnmatch.fnmatch(os.path.join(subDir,f),self.wildcard)
                    and os.path.join(subDir,f) not in self.fileList
                    ]
                tmp2 = [False for l in range(len(tmp1))]
                self.fileList += tmp1
                self.loadList += tmp2

@dataclass(kw_only=True)
class sourceRecord:
    fname = 'sourceFiles.json'
    # executes a file search using wildcard pattern matching
    # cross references against a list of exiting files
    # fname = 'sourceFiles.yml'
    sourceID: str = None
    sourcePath: str = None
    wildcard: str = '*wildcard*'
    nFiles: int = 0
    nLoaded: int = 0
    nPending: int = 0
    # fileList: list = field(default_factory=lambda:[])
    # loadList: list = field(default_factory=lambda:[])
    template: bool = field(default=False,repr=False)
    dpath: str = field(default=None,repr=False)

    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        elif self.sourcePath and os.path.isdir(self.sourcePath):
            self.sourcePath = os.path.abspath(self.sourcePath)
            # self.fileSearch()
        self.sourceID = os.path.join(self.sourcePath,self.wildcard)
        if self.dpath:
            os.makedirs(self.dpath,exist_ok=True)
            self.fname = os.path.join(self.dpath,self.fname)
            self.fileSearch = helper.loadDict(self.fname)
            if self.sourceID not in self.fileSearch:
                self.fileSearch[self.sourceID] = helper.reprToDict(fileSearch(sourcePath=self.sourcePath,wildcard=self.wildcard))
            else:
                self.fileSearch[self.sourceID] = helper.reprToDict(fileSearch(**self.fileSearch[self.sourceID]))
            if len(self.fileSearch)>1 and 'sourcePath\*wildcard*' in self.fileSearch:
                self.fileSearch.pop('sourcePath\*wildcard*')
            self.nFiles = len(self.fileSearch[self.sourceID]['loadList'])
            self.nLoaded = sum(self.fileSearch[self.sourceID]['loadList'])
            self.nPending = self.nFiles-self.nLoaded
            helper.saveDict(self.fileSearch,self.fname)

@dataclass(kw_only=True)
class measurementRecord:
    # Records pertaining to a measurement set
    measurementID: str = None
    description: str = None
    fileType: str = None
    sampleFrequency: str = None
    description: str = None
    latitude: float = None
    longitude: float = None
    startDate: str = None
    stopDate: str = None
    Sources: sourceRecord = field(default_factory=lambda:sourceRecord(template=True).__dict__)
    template: bool = field(default=False,repr=False)
    dpath: str = field(default=None,repr=False)

    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if k == 'Name':
                    self.__dict__[k] = 'This is a template for defining measurement-level metadata'
                elif k == 'measurementID':
                    self.__dict__[k] = '.measurementID'
                elif v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
        if self.measurementID:
            if self.measurementID != '.measurementID':
                self.measurementID = helper.safeFmt(self.measurementID)
            coordinates = siteCoordinates.coordinates(self.latitude,self.longitude)
            self.latitude,self.longitude = coordinates.GCS['y'],coordinates.GCS['x']
            if type(list(self.Sources.values())[0]) is not dict:
                self.Sources = {'':self.Sources}
            if self.dpath:
                pth = os.path.join(self.dpath,self.measurementID)
            else:
                pth = None
            self.Sources = helper.dictToDataclass(sourceRecord,self.Sources,ID=['sourceID'],constants={'dpath':pth},pop=True)
            if len(self.Sources)>1 and self.__dataclass_fields__['Sources'].default_factory()['sourceID'] in self.Sources:
                self.Sources.pop(self.__dataclass_fields__['Sources'].default_factory()['sourceID'])                

@dataclass(kw_only=True)
class siteRecord:
    # Records pertaining to a field site, including a record of measurements from the site
    siteID: str = None
    description: str = None
    Name: str = None
    PI: str = None
    startDate: str = None
    stopDate: str = None
    landCoverType: str = None
    latitude: float = None
    longitude: float = None
    Measurements: measurementRecord = field(default_factory=lambda:measurementRecord(template=True).__dict__)
    template: bool = field(default=False,repr=False)
    dpath: str = field(default=None,repr=False)
    
    def __post_init__(self):
        if self.template:
            for k,v in self.__dataclass_fields__.items():
                if k == 'Name':
                    self.__dict__[k] = 'This is a template for defining site-level metadata'
                elif k == 'siteID':
                    self.__dict__[k] = '.siteID'
                if v.repr and self.__dict__[k] is None:
                    self.__dict__[k] = v.name
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
            # for measurementID in self.Measurements:
            #     sF = os.path.join(self.projectPath,'metadata',self.siteID,measurementID,Search.fname)
            #     if not os.path.isfile(sF):
            #         sourceFiles = helper.dictToDataclass(Search,self.sourceFiles,['ID'],pop=True)
            #     else:
            #         sourceFiles = helper.dictToDataclass(Search,helper.loadDict(sF),['ID'],pop=True)
            #     if len(sourceFiles)>1 and self.sourceFiles['ID'] in sourceFiles:
            #         sourceFiles.pop(self.sourceFiles['ID'])
            #     helper.saveDict(sourceFiles,sF)

# @dataclass
# class searchInventory:
#     # Manages the search class
#     projectPath: str = None
#     siteID: str = None
#     measurementID: str = None
#     template: bool = False
#     sourceFiles: Search = field(default_factory=lambda:Search(template=True).__dict__)

#     def __post_init__(self):
#         if self.projectPath is None or not os.path.isdir(self.projectPath):
#             return
#         sF = os.path.join(self.projectPath,'metadata',self.siteID,self.measurementID,'sourceFiles.json')
#         if self.template and not os.path.isfile(sF):
#             for k,v in self.__dataclass_fields__.items():
#                 if v.repr and self.__dict__[k] is None:
#                     self.__dict__[k] = v.name
#         else:
#             self.rmID = self.__dataclass_fields__['sourceFiles'].default_factory()['ID']
#             self.process(sF)

#         if 'ID' in self.sourceFiles:
#             ID = self.sourceFiles.pop('ID')
#             self.sourceFiles = {ID:self.sourceFiles}
#         else:
#             if len(self.sourceFiles)>1:
#                 self.sourceFiles.pop(self.rmID)
#         helper.saveDict(self.sourceFiles,sF)

#     def process(self,sF):
#         if os.path.isfile(sF):
#             sourceFiles = helper.loadDict(sF)
#             sourceFiles = helper.dictToDataclass(Search,sourceFiles,['ID'],pop=True)
#         else:
#             sourceFiles = None
#         if sourceFiles is None or self.sourceFiles != self.__dataclass_fields__['sourceFiles'].default_factory():
#             if type(self.sourceFiles) is str and os.path.isfile(self.sourceFiles):
#                 self.sourceFiles = helper.loadDict(self.sourceFiles)
#             elif type(self.sourceFiles) is not dict:
#                 log(msg='input must be dict or filepath')
#             self.sourceFiles = helper.dictToDataclass(Search,self.sourceFiles,['ID'],pop=True)
#             if sourceFiles:
#                 self.sourceFiles = helper.updateDict(sourceFiles,self.sourceFiles)
#         else:
#             self.sourceFiles = sourceFiles

