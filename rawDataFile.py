
import os
import pandas as pd
from parseFiles import parseCSI,parseCSV
import importlib
from parseFiles.helperFunctions.asdict_repr import asdict_repr

importlib.reload(parseCSI)
importlib.reload(parseCSV)


def loadRawFile(source,fileType=None,parserSettings={},verbose=False):
    Processor = {
        'HOBOcsv':parseCSV.hoboCSV,
        'TOB3':parseCSI.parseTOB3,
        'TOA5':parseCSI.parseTOA5,
    }
    filePath,sourceInfo = source[0],source[1]
    ID = os.path.split(filePath)[-1].split('.')[0]
    out = {'filepath':filePath, 'sourceInfo':sourceInfo, 'variableMap':{}, 'DataFrame':pd.DataFrame()}
    if not sourceInfo['loaded'] and fileType in Processor:
        loadedFile = Processor[fileType](sourceFile=filePath,verbose=False,**parserSettings)
        out['sourceInfo']['parserSettings'] = asdict_repr(loadedFile)
        out['sourceInfo']['loaded'] = True
        out['variableMap'] = loadedFile.variableMap
        out['DataFrame'] = loadedFile.DataFrame
    return(out)

# for debuging:

if __name__ == '__main__':
    print('debug: example_data/20240912/Flux_Data2629.dat')
    TOB3(sourceFile=r'example_data\20240912\Flux_Data2629.dat')