import os
import sys
# import shutil
import fnmatch
# import datetime
from dataclasses import dataclass,field

import siteCoordinates
import helperFunctions as helper
import rawDataFile

import importlib
importlib.reload(siteCoordinates)
importlib.reload(rawDataFile)
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
                self.projectInventory()
            
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
            template = {k:v for k,v in siteRecord.__dict__.items() if k[0:2] != '__'}
            self.siteInventory = helper.dictToDataclass(siteRecord,template,ID=['siteID'],constants={'dpath':os.path.join(self.projectPath,'Sites'),'template':True},debug=True)
        for siteID,values in self.siteInventory.items():
            self.projectInfo['Sites'][siteID] = values['Name']
            self.save(values,os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))

    def rawFileSearch(self,siteID,measurementID,sourcePath=None,wildcard='*',parserKwargs={}):
        if sourcePath and os.path.isdir(sourcePath):
            sourceID = os.path.join(os.path.abspath(sourcePath),wildcard)
        sourceFiles = self.siteInventory[siteID]['Measurements'][measurementID]['sourceFiles']
        kwargs = {'sourcePath':sourcePath,'wildcard':wildcard,'parserKwargs':parserKwargs}
        if sourceID in sourceFiles and parserKwargs=={}:
            kwargs = sourceFiles[sourceID]
        fn = os.path.join(self.projectPath,'Sites',siteID,measurementID,'sourceFiles.json')
        # template={'fileList':[],'loadList':[],'variableMap':{}}
        template={'loaded':False,'parserKwargs':parserKwargs}
        sourceInventory = helper.loadDict(fn)
        if sourceID not in sourceInventory:
            sourceInventory[sourceID] = {}
        sourceMap = sourceRecord(**kwargs)

        fileList = []
        if sourceMap.sourcePath and os.path.isdir(sourceMap.sourcePath):
            for dir,_,files in os.walk(sourceMap.sourcePath):
                fileList += [os.path.join(dir,f) for f in files if fnmatch.fnmatch(os.path.join(dir,f),sourceMap.wildcard) and os.path.join(dir,f) not in sourceInventory[sourceID]]
        log('make robust to kwarg update')
        sourceInventory[sourceID] = sourceInventory[sourceID] | {f:template for f in fileList}
        

        sourceFiles[sourceID] = helper.reprToDict(sourceMap)
        if len(sourceFiles)>1 and sourceRecord.sourceID in sourceFiles:
            sourceFiles.pop(sourceRecord.sourceID)
        self.rawFileImport(siteID,measurementID,sourceInventory)
        self.save(sourceInventory,fn)
        self.save(self.siteInventory[siteID],os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))


    def rawFileImport(self,siteID,measurementID,sourceInventory):
        Processor = {
            'HOBOcsv':rawDataFile.HOBOcsv,
            'TOB3':rawDataFile.TOB3,
        }
        Measurement = self.siteInventory[siteID]['Measurements'][measurementID]
        if Measurement['fileType'] not in Processor:
            log(f"Add functionality for {Measurement['fileType']}")
            return
        method = Processor[Measurement['fileType']]
        for sourceID, sourceFiles in sourceInventory.items():
            parserKwargs = Measurement['sourceFiles'][sourceID]['parserKwargs']
            for file in sourceFiles:
                if not sourceFiles[file]['loaded']:
                    source = method(sourceFile=file,siteID=siteID,measurementID=measurementID,verbose=False,**parserKwargs)          
                    sourceFiles[file]['loaded'] = True
                    sourceInventory[sourceID][file]['parserKwargs'] = helper.reprToDict(source)
                    # self.save(source.variableMap,os.path.join(self.projectPath,'Sites',siteID,measurementID,'variableMap.yml'))


######################################################################################################################
# Data Source Management
######################################################################################################################


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


