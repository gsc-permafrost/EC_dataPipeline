import os
import re
import sys
import yaml
import json
import psutil
import datetime
import deepdiff
import argparse
import subprocess
import pandas as pd
from inspect import currentframe, getframeinfo

def dictToDataclass(method,toDump,ID=None,pop=False,constants={},pad=False,debug=False):
    if type(ID) is str:
        ID = [ID]
    if type(list(toDump.values())[0]) is not dict or pad:
        toDump = {'':toDump}
    out = {}
    for value in toDump.values():
        for k,v in constants.items():
            value[k] = v
        value = {k:v for k,v in value.items() if k in method.__dataclass_fields__}
        # if debug:
        #     log(value)
        t = method(**value)
        if ID is None:
            tmp = reprToDict(t)
        else:
            r = reprToDict(t)
            if pop:
                tmp = defaultNest([r.pop(id) for id in ID],r)
            else:
                tmp = defaultNest([r[id] for id in ID],r)
        updateDict(out,tmp)
    return(out)

def now(fmt='%Y-%m-%dT%H:%M:%S.%f',prefix='',suffix=''):
    return(f"{prefix}{datetime.datetime.now().strftime(fmt)}{suffix}")

def reprToDict(dc):
    # given a dataclass, dummp itemes where repr=true to a dictionary
    return({k:v for k,v in dc.__dict__.items() if k in dc.__dataclass_fields__ and dc.__dataclass_fields__[k].repr})

def log(msg='',ln=True,fn=True,verbose=True):
    if verbose:
        if type(msg) == list or type(msg) == tuple:
            msg = ' '.join([str(m) for m in msg])
        if ln:
            cf = currentframe()
            msg = f"line {cf.f_back.f_lineno}:\n{msg}\n"
            if fn:
                cf.f_back.f_code.co_filename
                msg = f"{cf.f_back.f_code.co_filename} "+ msg
        print(msg)


def defaultNest(seq,seed={}):
    def addVal(d,k,v):
        d.setdefault(k,v)
        return(d)
    for s in seq[::-1]:
        seed = addVal({},s,seed)
    return(seed)

def baseFields(self,repr=True,ordered = True):#,inherited = False):
    out = list(set(f for f,v in self.__dataclass_fields__.items() if v.repr == repr) - {f for base in type(self).__bases__ if hasattr(base,'__dataclass_fields__') for f,v in base.__dataclass_fields__.items() if v.repr == repr})
    if ordered:
        out = [k for k in self.__dataclass_fields__ if k in out]
    return(out)
    
def safeFmt(string,safeChars='[^0-9a-zA-Z]+',fillChar='_'):
    return(re.sub(safeChars,fillChar, str(string)))

def sorted_nicely(l): 
    # credit: https://stackoverflow.com/a/2669120/5683778
    # sort an alphanumeric list alphabetically for text but numerically for digits
    convert = lambda text: int(text) if text.isdigit() else text 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(l, key = alphanum_key)

def findNestedValue(element,nest,delimiter=os.path.sep):
    # get value of nested in a dict by providing a string of keys separated by delimiter (defaults to path)
    # e.g., findNestedValue(element 'a.b', nest = {'a':{'b':'c'}}, delimiter = '.') returns 'b'
    keys = element.split(delimiter)
    rv = nest
    for key in keys:
        if key in rv:
            rv = rv[key]
        else:
            rv = None
            break
    return rv

def loadDict(file,verbose=False,safemode=False):
    file = os.path.abspath(file)
    # read a dict from file in either .json or .yml format
    if os.path.isfile(file):
        if file.endswith('.yml'):
            with open(file) as f:
                out = yaml.safe_load(f)
        elif file.endswith('.json'):
            with open(file) as f:
                out = json.load(f)   
    elif not safemode:
        if verbose: print(f"{file} does not exist, creating empty file")
        out = {}
        saveDict(out,file)
    elif verbose:
        out = None
        print(f"{file} does not exist")
    return(out)

def saveDict(obj,outputPath,sort_keys=False,indent=None):
    # save a dict (obj) to a file (outputPath) in either .json or .yml format
    if not os.path.isdir(os.path.split(outputPath)[0]):
        os.makedirs(os.path.split(outputPath)[0])
    with open(outputPath,'w') as file:
        if outputPath.endswith('.yml'):
            yaml.safe_dump(obj,file,sort_keys=sort_keys)
        if outputPath.endswith('.json'):
            json.dump(obj,file,indent=indent)

def repForbid(txt):
    # remove problematic characters from filepaths
    for rep in [' ','/','\\']:
        txt = txt.replace(rep,'_')
    for rep in [':','.']:
        txt = txt.replace(rep,'')
    return(txt)

# template/default exclusion patter for compareDicts
def exclude_ignore_callback(obj, path):
    # Exclude any dictionary where it contains a key:value pair 'ignore': True
    # Not recursive
    if isinstance(obj, dict) and obj.get('ignore') is True:
        return True
    return False

def compareDicts(new_dict,old_dict,ignore_order=True,exclude_keys=[],exclude_values=exclude_ignore_callback):
    # a wrapper on the deepdiff algorithm 
    # outputs changes in a format which is easier to interpret as a yaml file
    dd = deepdiff.DeepDiff(old_dict,new_dict,ignore_order=ignore_order,exclude_regex_paths=exclude_keys,exclude_obj_callback=exclude_values)
    if dd == {}:
        return(False)
    dDict = {}
    for key,diff in dd.items():
        if key == 'values_changed':
            dDict[key] = {}
            for root,data in diff.items():
                data['acceptNew'] = True
                keyNest = root.replace("root['",'').rstrip("']")
                logItem = packDict(keyNest,format="']['",fill=data)
                dDict[key] = updateDict(dDict[key],logItem)
        elif key == 'type_changes':
            dDict[key] = {}
            for root,data in diff.items():
                data['acceptNew'] = True
                data['old_type'] = str(data['old_type'])
                data['new_type'] = str(data['new_type'])
                keyNest = root.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem = packDict(keyNest,format="']['",fill=data)
                dDict[key] = updateDict(dDict[key],logItem)
        elif key == 'dictionary_item_added':
            logItem = {}
            for item in diff:
                keyNest = item.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem[keyNest] = findNestedValue(keyNest,new_dict,delimiter='~')
            dDict[key] = packDict(logItem,format='~')
        elif key == 'dictionary_item_removed':
            logItem = {}
            for item in diff:
                keyNest = item.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem[keyNest] = findNestedValue(keyNest,old_dict,delimiter='~')
            dDict[key] = packDict(logItem,format='~')
        elif key == 'iterable_item_added':
            logItem = {}
            for item in diff:
                keyNest = item.replace("root['",'').rstrip("']").replace("']['",'~')
                logItem[keyNest] = findNestedValue(keyNest,new_dict,delimiter='~')
            dDict[key] = packDict(logItem,format='~')
        else:
            log(f'New for dict compare: {key}')
            sys.exit()
    return(dDict)
    
def unpackDict(Tree,format=os.path.sep,limit=None):
    # recursive function to condense a nested dict by concatenating keys to a string
    def unpack(child,parent=None,root=None,format=None,limit=None):
        pth = {}
        if type(child) is dict and (limit is None or limit >= 0) and child:
            if limit is not None:
                limit -= 1
            for key,value in child.items():
                if parent is None:
                    pass
                else:
                    key = format.join([parent,key])
                if type(value) is not dict or (limit is not None and limit < 0) or not value:
                    pth[key] = unpack(value,key,root,format,limit)
                else:
                    pth = pth | unpack(value,key,root,format,limit)
        else:
            if type(child) is not dict or (limit is not None and limit < 0) or not child:
                return(child)
            else:
                sys.exit('Error in file tree unpack')
        return(pth)
    return(unpack(Tree,format=format,limit=limit))

def packDict(itemList,format=os.path.sep,limit=None,order=-1,fill=None):
    # recursive function to generate nested dict from list of strings, splitting by sep
    Tree = {}
    if type(itemList) is list:
        if fill == 'key':
            itemList = {key:key for key in itemList}
        elif type(fill) is list:
            itemList = {key:f for key,f in zip(itemList,fill)}
        else:
            itemList = {key:fill for key in itemList}
    elif type(itemList) is not dict:
        itemList = {itemList:fill}
    for key,value in itemList.items():
        b = key.split(format)
        if order == -1:
            if limit is None: lm = len(b)+order
            else: lm = limit
            start = len(b)
            end = max(0,len(b)+lm*order+order)
            for i in range(start,end,order):
                if i == start:
                    subTree = {b[i+order]:value}
                elif i>end+1:
                    subTree =  {b[i+order]:subTree}
                else:
                    subTree = {format.join(b[:i]):subTree}
        else:
            if limit is None: lm = len(b)
            else: lm = limit
            start = 0
            end = min(lm+1,len(b))
            for i in range(start,end,order):
                if i == start:
                    subTree = {format.join(b[end-1:]):value}
                else:
                    subTree = {b[end-1-i]:subTree}
        Tree = updateDict(Tree,subTree,overwrite='append')
    return(Tree)

def updateDict(base,new,overwrite=False,verbose=False):
    if base == new: return(base)
    # more comprehensive way to update items in a nested dict
    for key,value in new.items():
        if type(base) is dict and key not in base.keys():
            if verbose: log('setting: ',key,' = ',base,'\n to: ',key,' = ',value)
            base[key]=value
        elif type(value) is dict and type(base[key]) is dict:
            base[key] = updateDict(base[key],value,overwrite,verbose)
        elif overwrite == True and base[key]!= value:
            if verbose: log('setting: ',key,' = ',base[key],'\n to: ',key,' = ',value)
            base[key] = value
        elif overwrite == 'append' and type(base[key]) is list:
            if type(base[key][0]) is not list and type(value) is list:
                base[key] = [base[key]]
            if verbose: log('adding: ',value,'\n to: ',key,' = ',base[key])
            base[key].append(value)
        elif overwrite == 'append' and type(base[key]) is not list:
            base[key] = [base[key]]
            if verbose: log('adding: ',value,'\n to: ',key,' = ',base[key])
            base[key].append(value)
        elif base[key] is None and value is not None:
            if verbose: log('setting: ',key,' = ',base[key],'\n to: ',key,' = ',value)
            base[key] = value
        elif base[key] != value:
            if verbose: log(f'overwrite = {overwrite} will not update matching keys: ',base[key],value)
    return(base) 

def lists2DataFrame(**kwargs):
    # convert a sequence of named list arguments to a dataframe
    df = pd.DataFrame(data = {key:val for key,val in kwargs.items()})
    if 'index' in df.columns:
        df = df.set_index('index',drop=True)
    return(df)

def str2bool(v):
    # credit: https://stackoverflow.com/a/43357954/5683778
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def getCMD(defaultArgs):
    # helper function to parse command line arguments
    CLI=argparse.ArgumentParser()
    dictArgs = []
    for key,val in defaultArgs.items():
        dt = type(val)
        nargs = "?"
        print(key,val,dt)
        if val == None:
            dt = str
        if dt == type({}):
            dictArgs.append(key)
            dt = type('')
            val = '{}'
        elif dt == type([]):
            nargs = '+'
            dt = type('')
        elif dt == type(False):
            dt = str2bool
        CLI.add_argument(f"--{key}",nargs=nargs,type=dt,default=val)

    # parse the command line
    args = CLI.parse_args()
    kwargs = vars(args)
    for d in dictArgs:
        kwargs[d] = json.loads(kwargs[d])
        # replace booleans
    print(kwargs)
    return(kwargs)

class progressbar():
    # simple progress bar function with text display
    def __init__(self,items,prefix='',size=60,out=sys.stdout):
        self.nItems = items
        self.out = out
        self.i = 0
        self.prefix=prefix
        self.size=size
        self.msg = None
        self.show(0)

    def show(self,j):
        if self.nItems > 0:
            x = int(self.size*j/self.nItems)
            if self.msg is None:
                suffix = ""
            else:
                suffix = ' '+self.msg
            print(f"{self.prefix}[{u'█'*x}{('.'*(self.size-x))}] {j}/{self.nItems}{suffix} ", end='\r', file=self.out, flush=True)

    def step(self,step_size=1,msg=None,L=20):
        if msg is not None:
            self.msg = msg[-L:]
        self.i+=step_size
        self.show(self.i)

    def close(self):
        print('\n')

# def set_high_priority():
#     p = psutil.Process(os.getpid())
#     p.nice(psutil.HIGH_PRIORITY_CLASS)

# def pasteWithSubprocess(source, dest, option = 'copy',Verbose=False,pb=None):
#     set_high_priority()
#     cmd=None
#     source = os.path.abspath(source)
#     dest = os.path.abspath(dest)
#     if sys.platform.startswith("darwin"): 
#         # These need to be tested/flushed out
#         if option == 'copy' or option == 'xcopy':
#             cmd=['cp', source, dest]
#         elif option == 'move':
#             cmd=['mv',source,dest]
#     elif sys.platform.startswith("win"): 
#         cmd=[option, source, dest]
#         if option == 'xcopy':
#             cmd.append('/s')
#     if cmd:
#         proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
#     if pb is not None:
#         pb.step(msg=f"{source}")

#     if Verbose==True:
#         print(proc)

if __name__ == '__main__':
    prefix = 'Test'
    nItems=10
    pb = progressbar(nItems,prefix)
    for i in range(nItems):
        pb.step()
    pb.close()
