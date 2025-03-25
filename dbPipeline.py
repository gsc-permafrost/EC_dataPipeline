import os
import sys
import copy
import fnmatch
from pathlib import Path
from dataclasses import dataclass,field

import numpy as np
import pandas as pd
import geopandas as gpd

import siteCoordinates
import helperFunctions as helper
import rawDataFile
import siteInventory

import importlib
importlib.reload(siteCoordinates)
importlib.reload(siteInventory)
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
    mapTemplate: str = field(default_factory=lambda:Path(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','MapTemplate.html')).read_text())
 
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
            self.siteInventory[siteID] = helper.dictToDataclass(siteInventory.siteRecord,record,constants={'dpath':os.path.join(self.projectPath,'Sites')})
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
            newSites = helper.dictToDataclass(siteInventory.siteRecord,newSites,ID=['siteID'],constants={'dpath':os.path.join(self.projectPath,'Sites')})
            self.siteInventory = helper.updateDict(self.siteInventory,newSites)
        # If no sites exist yet, create a template
        if self.siteInventory == {}:
            template = {k:v for k,v in siteInventory.siteRecord.__dict__.items() if k[0:2] != '__'}
            self.siteInventory = helper.dictToDataclass(siteInventory.siteRecord,template,ID=['siteID'],constants={'dpath':os.path.join(self.projectPath,'Sites'),'template':True},debug=True)

        # save the inventory and make a webmap of sites
        siteDF = pd.DataFrame()
        for siteID,values in self.siteInventory.items():
            self.projectInfo['Sites'][siteID] = {'Name':values['Name'],
                                                 'description':values['description'],
                                                 'latitude':values['latitude'],
                                                 'longitude':values['longitude'],
            }
            self.save(values,os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))
                
            if not siteID.startswith('.'):
                siteDF = pd.concat([siteDF,pd.DataFrame(data = self.projectInfo['Sites'][siteID], index=[siteID])])
        if not siteDF.empty:
            Site_WGS = gpd.GeoDataFrame(siteDF, geometry=gpd.points_from_xy(siteDF.longitude, siteDF.latitude), crs="EPSG:4326")
            self.mapTemplate = self.mapTemplate.replace('fieldSitesJson',Site_WGS.to_json())
            with open(os.path.join(self.projectPath,'fieldSiteMap.html'),'w+') as out:
                out.write(self.mapTemplate)

    def rawFileSearch(self,siteID,measurementID,sourcePath=None,wildcard='*',parserKwargs={}):
        if sourcePath and os.path.isdir(sourcePath):
            sourceID = os.path.join(os.path.abspath(sourcePath),wildcard)
        sourceFiles = self.siteInventory[siteID]['Measurements'][measurementID]['sourceFiles']
        kwargs = {'sourcePath':sourcePath,'wildcard':wildcard,'parserKwargs':parserKwargs}
        if sourceID in sourceFiles and parserKwargs=={}:
            kwargs = sourceFiles[sourceID]
        fn = os.path.join(self.projectPath,'Sites',siteID,measurementID,'sourceFiles.json')
        template={'loaded':False,'parserKwargs':parserKwargs}
        sourceInventory = helper.loadDict(fn)
        if sourceID not in sourceInventory:
            sourceInventory[sourceID] = {}
        sourceMap = siteInventory.sourceRecord(**kwargs)

        fileList = []
        if sourceMap.sourcePath and os.path.isdir(sourceMap.sourcePath):
            for dir,_,files in os.walk(sourceMap.sourcePath):
                fileList += [os.path.join(dir,f) for f in files if fnmatch.fnmatch(os.path.join(dir,f),sourceMap.wildcard) and os.path.join(dir,f) not in sourceInventory[sourceID]]
        
        log('make robust to kwarg update')
        sourceInventory[sourceID] = sourceInventory[sourceID] | {f:copy.deepcopy(template) for f in fileList}
        

        sourceFiles[sourceID] = helper.reprToDict(sourceMap)
        if len(sourceFiles)>1 and siteInventory.sourceRecord.sourceID in sourceFiles:
            sourceFiles.pop(siteInventory.sourceRecord.sourceID)
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
                    if not source.Data.empty:
                        sourceFiles[file]['loaded'] = True
                        sourceInventory[sourceID][file]['parserKwargs'] = helper.reprToDict(source)
                        for year in source.Data.index.year.unique():
                            databaseFolder(path=os.path.join(self.projectPath,'database',siteID,measurementID,str(year)),year=year,Data=source.Data,variableMap=source.variableMap)
    

@dataclass(kw_only=True)
class databaseFolder:
    POSIX_timestamp: dict = field(default_factory=lambda:{'dtype': 'float64','frequency': '30min','timezone': 'UTC','ignore':False,'variableDescription': 'the POSIX_timestamp is stored as a 64-bit floating point number representing the seconds elapsed since 1970-01-01 00:00 UTC time'})
    path: str
    year: int
    verbose: bool = True
    Data: pd.DataFrame = field(default_factory=lambda:pd.DataFrame())
    variableMap: dict = field(default_factory=lambda:{})

    def __post_init__(self):
        self.write = bool(self.variableMap) and not self.Data.empty
        self.Data = self.Data.drop([col for col,val in self.variableMap.items() if val['ignore']],axis=1)
        self.variableMap = {key:values for key,values in self.variableMap.items() if not values['ignore']}
        self.variableMap = {'POSIX_timestamp':self.POSIX_timestamp} |self.variableMap
        
        if os.path.isfile(os.path.join(self.path,'_variableMap.yml')) and os.path.exists(os.path.join(self.path,'POSIX_timestamp')):
            self.Data = self.read()
        else:
            self.Data = self.emptyYear(self.year)

        if self.write:
            self.writeYear()
        self.Data.index.name = 'UTC'
        helper.saveDict(self.variableMap,os.path.join(self.path,'_variableMap.yml'))

    def emptyYear(self,year):
        dataset = pd.DataFrame(index=pd.date_range(str(year),str(year+1),freq=self.POSIX_timestamp['frequency'],inclusive='right'))
        dataset['POSIX_timestamp'] = (dataset.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        for col in self.Data.columns:
            if col in dataset.columns:
                dataset[col] = dataset[col].fillna(self.Data[col])
            else:
                dataset = dataset.join(self.Data[[col]])
        return(dataset)

    def read(self):
        vm = helper.loadDict(os.path.join(self.path,'_variableMap.yml'))
        dataset = {f:np.fromfile(os.path.join(self.path,f),dtype=vm[f]['dtype']) for f in os.listdir(self.path) if not f.endswith('.yml')}
        self.variableMap = vm|self.variableMap
        dataset = pd.DataFrame(data = dataset)
        dataset.index=pd.to_datetime(dataset['POSIX_timestamp'],unit='s')
        for col in self.Data.columns:
            if col in dataset.columns:
                dataset[col] = dataset[col].fillna(self.Data[col])
            else:
                dataset = dataset.join(self.Data[[col]])
        return(dataset)

    def writeYear(self):
        for col in self.Data.columns:
            if not os.path.isdir(self.path): os.makedirs(self.path)
            fname = os.path.join(self.path,col)
            log(f'Writing: {fname}',ln=False,verbose=self.verbose)
            self.Data[col].astype(self.variableMap[col]['dtype']).values.tofile(fname)
