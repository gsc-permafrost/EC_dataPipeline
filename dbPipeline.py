import os
import sys
import copy
# import fnmatch
from pathlib import Path
from dataclasses import dataclass,field
from functools import partial
from multiprocessing import Pool

import numpy as np
import pandas as pd
# import geopandas as gpd


import rawDataFile

from siteInventory import siteInventory
from siteInventory import sourceRecord
from parseFiles.helperFunctions.log import log
from parseFiles.helperFunctions.loadDict import loadDict
from parseFiles.helperFunctions.saveDict import saveDict
from parseFiles.helperFunctions.asdict_repr import asdict_repr

import datetime
import yaml


def now(fmt='%Y-%m-%dT%H:%M:%S',prefix='',suffix=''):
    return(f"{prefix}{datetime.datetime.now().strftime(fmt)}{suffix}")

@dataclass(kw_only=True)
class database:
    # Base class containing common functionality across the database
    projectPath: str = None
    fillChar: str = '_'
    sepChar: str = '/'*2
    verbose: bool = False
    template: bool = True
    enableParallel: bool = True
    loadNew: bool = True
    siteIDs: list = field(default_factory=lambda:[])
    Sites: dict = field(default_factory=lambda:{})
    projectInfo: dict = field(default_factory=lambda:yaml.safe_load(Path(os.path.join(os.path.dirname(os.path.abspath(__file__)),'config_files','databaseMetadata.yml')).read_text()))
 
    def __post_init__(self):
        if self.projectPath:
            if not os.path.isdir(self.projectPath) or len(os.listdir(self.projectPath)) == 0:
                self.makeNewProject()
            elif not os.path.isfile(os.path.join(self.projectPath,'projectInfo.yml')):
                sys.exit('Non-empty, non-project directory provided')
            else:
                self.projectInfo = loadDict(os.path.join(self.projectPath,'projectInfo.yml'))
            if type(self.siteIDs) != list:
                self.siteIDs = [self.siteIDs]
            elif self.siteIDs == []:
                self.siteIDs = [siteID for siteID in self.projectInfo['Sites'] if not siteID.startswith('.')]
            if len(self.siteIDs):
                self.projectInventory()
            elif self.template:
                log('Creating empty project from template',ln=False)
                self.projectInventory(newSites={'template':{}})
            else:
                if self.verbose:
                    log('No sites found in project, creating empty project inventory',ln=False)
    def makeNewProject(self):
        # make a new database
        self.projectInfo['.dateCreated'] = now()
        self.projectInfo['.dateModified'] = now()
        for d in self.projectInfo:
            if not d.startswith('.'):
                os.makedirs(os.path.join(self.projectPath,d))
        
    def save(self,dictObj=None,filename=None,anchors=False):
        # Saves projectInfo.yml and any other relevant metadata files
        if self.projectPath:
            self.projectInfo['.dateModified'] = now()
            saveDict(self.projectInfo,os.path.join(self.projectPath,'projectInfo.yml'),sort_keys=True,anchors=anchors)
            if filename:
                log(('Saving: ',filename),ln=False,verbose=self.verbose)
                saveDict(dictObj,filename)

    def projectInventory(self,newSites={},fileSearch=None):
        # Read existing sites
        for siteID in self.siteIDs:
            self.Sites[siteID] = loadDict(os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))
        # If given a file template for new sites
        if type(newSites) is str and os.path.isfile(newSites):
            newSites = siteInventory(Sites=newSites).Sites
        # otherwise check for manual additions
        elif newSites == {}:
            additions = [siteID for siteID in os.listdir(os.path.join(self.projectPath,'Sites'))
                        if '.' not in siteID and siteID not in self.siteIDs]
            if additions != []:
                newSites = {new:siteInventory(Sites=os.path.join(self.projectPath,'Sites',new,f"{new}_metadata.yml")).Sites for new in additions}
        self.Sites = self.Sites | newSites
        if len(self.Sites)>1 and '.siteID' in self.Sites:
            self.Sites.pop('.siteID')
        self.Sites = siteInventory(Sites=self.Sites)
        self.spatialInventory = self.Sites.spatialInventory
        self.webMap = self.Sites.mapTemplate
        self.Sites = {siteID:self.Sites.Sites[siteID] for siteID in self.Sites.Sites}

        # save the inventory and make a webmap of sites
        siteDF = pd.DataFrame()
        for siteID,values in self.Sites.items():
            self.projectInfo['Sites'][siteID] = {
                'Name':values['Name'],
                'description':values['description'],
                'latitude':values['latitude'],
                'longitude':values['longitude'],
            }
            self.save(values,os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))
            if self.loadNew:
                for measurementID in values['Measurements']:
                    self.rawFileSearch(siteID,measurementID)

        with open(os.path.join(self.projectPath,'fieldSiteMap.html'),'w+') as out:
            out.write(self.webMap)

    def rawFileSearch(self,siteID=None,measurementID=None,kwargs={}):
        soureFiles_alias = self.Sites[siteID]['Measurements'][measurementID]['sourceFiles']
        sourceInventory = loadDict(
            os.path.join(self.projectPath,'Sites',siteID,measurementID,'sourceFiles.yml'),
            template={sourceRecord.matchPattern:asdict_repr(sourceRecord(),repr=None)},
            verbose=False
            )
        if 'matchPattern' in kwargs and kwargs['matchPattern'] not in sourceInventory:
            sourceInventory[kwargs['matchPattern']] = kwargs
        for result in map(lambda values: sourceRecord(**values),sourceInventory.values()):
            result.__find_files__()
            sourceInventory[result.matchPattern] = asdict_repr(result,repr=None)
            soureFiles_alias[result.matchPattern] = asdict_repr(result)
        if len(soureFiles_alias)>1 and sourceRecord.matchPattern in soureFiles_alias:
            soureFiles_alias.pop(sourceRecord.matchPattern)
        if len(sourceInventory)>1 and sourceRecord.matchPattern in sourceInventory:
            sourceInventory.pop(sourceRecord.matchPattern)
        addendum = ([k for k in soureFiles_alias.keys() if k not in sourceInventory.keys()])
        for a in addendum:
            sourceInventory[a] = copy.deepcopy(soureFiles_alias[a])
        self.rawFileImport(siteID,measurementID,sourceInventory)
        self.save(self.Sites[siteID],os.path.join(self.projectPath,'Sites',siteID,f"{siteID}_metadata.yml"))
        self.save(sourceInventory,os.path.join(self.projectPath,'Sites',siteID,measurementID,'sourceFiles.yml'))

    def rawFileImport(self,siteID,measurementID,sourceInventory):
        Measurement = self.Sites[siteID]['Measurements'][measurementID]
        for matchPattern, sourceFiles in sourceInventory.items():
            if 'fileList' not in sourceFiles:
                log('No sourceFiles to import',ln=False,verbose=self.verbose)
                pass
            elif len(sourceFiles['fileList'])>3 and Measurement['fileType'] == 'TOB3' and self.enableParallel:
                nproc = min(os.cpu_count()-2,len(sourceFiles))
                with Pool(processes=nproc) as pool:
                    results = pool.map(partial(rawDataFile.loadRawFile,fileType=Measurement['fileType'],parserSettings=sourceFiles['parserSettings']),sourceFiles['fileList'].items())
            else:
                results = map(partial(rawDataFile.loadRawFile,fileType=Measurement['fileType'],parserSettings=sourceFiles['parserSettings']),(copy.deepcopy(f) for f in sourceFiles['fileList'].items()))
            for result in results:
                if not result['DataFrame'].empty:
                    databaseFolder(path=os.path.join(self.projectPath,'database',siteID,measurementID),dataIn=result['DataFrame'],variableMap=result['variableMap'])
                symLink = [sourceFiles['fileList'][k]['parserSettings'] for k in sourceFiles['fileList'] if sourceFiles['fileList'][k]['parserSettings'] == result['sourceInfo']['parserSettings']]
                if symLink:
                    result['sourceInfo']['parserSettings'] = symLink[0]
                sourceFiles['fileList'][result['filepath']] = result['sourceInfo']
            self.save(sourceInventory,os.path.join(self.projectPath,'Sites',siteID,measurementID,'sourceFiles.yml'),anchors=True)

@dataclass(kw_only=True)
class databaseFolder:
    POSIX_timestamp: dict = field(default_factory=lambda:{'dtype': 'float64','frequency': '30min','timezone': 'UTC','ignore':False,'variableDescription': 'the POSIX_timestamp is stored as a 64-bit floating point number representing the seconds elapsed since 1970-01-01 00:00 UTC time'})
    path: str
    Years: list = None
    verbose: bool = True
    dataIn: pd.DataFrame = field(default_factory=lambda:pd.DataFrame())
    dataOut: pd.DataFrame = field(default_factory=lambda:pd.DataFrame())
    variableMap: dict = field(default_factory=lambda:{})

    def __post_init__(self):
        self.write = bool(self.variableMap) and not self.dataIn.empty
        self.dataIn = self.dataIn.drop([col for col,val in self.variableMap.items() if val['ignore']],axis=1)
        self.variableMap = {key:values for key,values in self.variableMap.items() if not values['ignore']}
        self.variableMap = {'POSIX_timestamp':self.POSIX_timestamp} |self.variableMap
        if self.Years is None:
            if not self.dataIn.empty:
                self.Years = list(self.dataIn.index.year.unique())
            else:
                log('Error, define years to read')
                return()

        elif type(self.Years) == int:
            self.Years = [self.Years]
        elif type(self.Years) == str:
            self.Years = [int(self.Years)]

        for year in self.Years:
            if os.path.isfile(os.path.join(self.path,str(year),'_variableMap.yml')) and os.path.exists(os.path.join(self.path,str(year),'POSIX_timestamp')):
                self.dataOut = pd.concat([self.dataOut,self.readYear(year)])
            else:
                self.dataOut = pd.concat([self.dataOut,self.emptyYear(year)])
            if self.write:
                self.writeYear(year)
            self.dataOut.index.name = 'UTC'
            saveDict(self.variableMap,os.path.join(self.path,str(year),'_variableMap.yml'))

    def emptyYear(self,year):
        dataset = pd.DataFrame(index=pd.date_range(str(year),str(year+1),freq=self.POSIX_timestamp['frequency'],inclusive='right'))
        dataset['POSIX_timestamp'] = (dataset.index - pd.Timestamp("1970-01-01")) / pd.Timedelta('1s')
        for col in set(list(self.dataIn.columns)+list(dataset.columns)):
            if col in dataset.columns and col in self.dataIn.columns:
                dataset[col] = dataset[col].fillna(self.dataIn.loc[self.dataIn.index.year==year,col])
            elif col in self.dataIn.columns:
                dataset = dataset.join(self.dataIn.loc[self.dataIn.index.year==year,[col]])
        return(dataset)

    def readYear(self,year):
        vm = loadDict(os.path.join(self.path,str(year),'_variableMap.yml'))
        dataset = {f:np.fromfile(os.path.join(self.path,str(year),f),dtype=vm[f]['dtype']) for f in os.listdir(os.path.join(self.path,str(year))) if not f.endswith('.yml')}
        self.variableMap = vm|self.variableMap
        dataset = pd.DataFrame(data = dataset)
        dataset.index=pd.to_datetime(dataset['POSIX_timestamp'],unit='s')
        for col in set(list(self.dataIn.columns)+list(dataset.columns)):
            if col in dataset.columns and col in self.dataIn.columns:
                dataset[col] = dataset[col].fillna(self.dataIn.loc[self.dataIn.index.year==year,col])
            elif col in self.dataIn.columns:
                dataset = dataset.join(self.dataIn.loc[self.dataIn.index.year==year,[col]])
        return(dataset)

    def writeYear(self,year):
        for col in self.dataOut.columns:
            if not os.path.isdir(os.path.join(self.path,str(year))): os.makedirs(os.path.join(self.path,str(year)))
            fname = os.path.join(self.path,str(year),col)
            log(f'Writing: {fname}',ln=False,verbose=self.verbose)
            self.dataOut.loc[self.dataOut.index.year==year,col].astype(self.variableMap[col]['dtype']).values.tofile(fname)
