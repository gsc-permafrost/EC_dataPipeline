
import os
import pandas as pd
import helperFunctions as helper
from parseFiles import HOBOcsv,TOB3,TOA5
import importlib

importlib.reload(TOB3)
importlib.reload(HOBOcsv)


def loadRawFile(source,fileType=None,parserSettings={},verbose=False):
    Processor = {
        'HOBOcsv':HOBOcsv,
        'TOB3':TOB3,
        'TOA5':TOA5,
    }
    filePath,sourceInfo = source[0],source[1]
    ID = os.path.split(filePath)[-1].split('.')[0]
    out = {'filepath':filePath, 'sourceInfo':sourceInfo, 'variableMap':{}, 'DataFrame':pd.DataFrame()}
    if not sourceInfo['loaded'] and fileType in Processor:
        loadedFile = Processor[fileType].read(sourceFile=filePath,verbose=False,**parserSettings)
        out['sourceInfo']['parserSettings'] = helper.reprToDict(loadedFile)
        out['sourceInfo']['loaded'] = True
        out['variableMap'] = loadedFile.variableMap
        out['DataFrame'] = loadedFile.DataFrame
    return(out)

# for debuging:

if __name__ == '__main__':
    print('debug: example_data/20240912/Flux_Data2629.dat')
    TOB3(sourceFile=r'example_data\20240912\Flux_Data2629.dat')