import os
import cdflib
import cdawmeta

def url2file(url):
  return url.replace("https://cdaweb.gsfc.nasa.gov/", "")

def open_cdf(file, logger=None, use_cache=True, cache_dir=None):

  if file.startswith('http'):
    kwargs = {
      'url2file': url2file,
      'logger': logger,
      'use_cache': use_cache,
      'cache_dir': cache_dir
    }
    file_path = cdawmeta.util.get_file(file, **kwargs)
    if file_path is None:
      return None

  try:
    cdffile = cdflib.CDF(file_path)
    return cdffile
  except Exception as e:
    if logger is not None:
      logger.error(f"Error opening {file_path}: {e}")
    return None

def read_cdf_depend_0s(file, _return='names', logger=None, use_cache=True, cache_dir=None):

  cdffile = open_cdf(file, logger=logger, use_cache=use_cache, cache_dir=None)
  if cdffile is None:
    return None

  meta = {}
  depend_0s = []
  info = cdffile.cdf_info()
  rVariables = info.rVariables
  zVariables = info.zVariables
  for variable in rVariables + zVariables:
    meta = cdffile.varattsget(variable=variable)
    if 'DEPEND_0' in meta:
      depend_0s.append(meta['DEPEND_0'])

  depend_0s = list(set(depend_0s))
  if _return == 'names':
    return depend_0s

  if _return == 'data':
    data = {}
    for depend_0 in depend_0s:
      try:
        data[depend_0] = cdffile.varget(variable=depend_0)
      except Exception as e:
        if logger is not None:
          logger.error(f"Error reading {depend_0}: {e}")
        data[depend_0] = None
    return data

def read_cdf_meta(file, subset=False, logger=None, use_cache=True):

  cdffile = open_cdf(file, logger=logger, use_cache=use_cache)
  if cdffile is None:
    return None

  meta = {}
  depend_0s = []
  info = cdffile.cdf_info()
  rVariables = info.rVariables
  zVariables = info.zVariables
  for variable in rVariables + zVariables:
    meta[variable] = {'VarDescription': {}, 'VarAttributes': None}
    meta[variable]['VarAttributes'] = cdffile.varattsget(variable=variable)
    # Add information that is in Master CDF JSONs
    vdata = cdffile.varinq(variable)
    # Use convention used in Master CDF JSONs for names
    methods = dir(vdata)
    method_map = {
      'Block_Factor': 'BlockingFactor',
      'Compress': 'Compress',
      'Data_Type': 'DataTypeValue',
      'Data_Type_Description': 'DataType',
      'Dim_Sizes': 'DimSizes',
      'Dim_Vary': 'DimVariances',
      'Last_Rec': 'LastRecord',
      'Num': 'Num',
      'Num_Dims': 'NumDims',
      'Num_Elements': 'NumElements',
      'Pad': 'PadValue',
      'Rec_Vary': 'RecVariance',
      'Sparse': 'SparseRecords',
      'Var_Type': 'VarType',
      'Variable': 'VariableName',
    }
    for method in methods:
      if not method.startswith('_'):
        if method in method_map:
          method_renamed = method_map[method]
        else:
          print("??? Method not in map: ", method)
          method_renamed = method
        meta[variable]['VarDescription'][method_renamed] = getattr(vdata, method)

    if subset and 'DEPEND_0' in meta[variable]:
      depend_0s.append(meta[variable]['DEPEND_0'])

  if subset:
    depend_0s = list(set(depend_0s))
    meta_subsetted = {}
    for depend_0 in depend_0s:
      meta_subsetted[depend_0] = {}
      for variable in rVariables + zVariables:
        if 'DEPEND_0' in meta[variable] and meta[variable]['DEPEND_0'] == depend_0:
          meta_subsetted[depend_0][variable] = meta[variable]
          for key in meta[variable]:
            if 'PTR' in key:
              meta_subsetted[depend_0][variable][key] = cdffile.varget(variable=meta[variable][key])
    return meta_subsetted

  return meta

def xread_cdf(file, parameters=None, start=None, stop=None, logger=None, use_cache=True):

  meta = []
  data = []
  meta = read_cdf_meta(file, logger=None, use_cache=use_cache)

  cdffile = open_cdf(file, logger=logger, use_cache=False)
  if cdffile is None:
    return None

  depend_0s = []
  parameters = parameters.split(",")
  for parameter in parameters:
    data.append(cdffile.varget(variable=parameter))
    meta.append(cdffile.varattsget(variable=parameter))
    depend_0s.append(meta[-1]['DEPEND_0'])

  udepend_0s = list(set(depend_0s)); # Unique depend_0s
  assert len(udepend_0s) == 1, 'Multiple DEPEND0s not implemented. Found: ' + ", ".join(udepend_0s)

  epoch = cdffile.varget(variable=depend_0s[0])
  time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

  if isinstance(time, str):
    time = [time]

  start_idx = 0
  stop_idx = len(time)

  if start is not None or stop is not None:
    n = len(time)
    startr = start[0:n-1]
    stopr = stop[0:n-1]
    start_found = False
    for idx, t in enumerate(time):
      if start_found == False and t >= startr:
        start_found = True
        start_idx = idx
      if t >= stopr:
        stop_idx = idx + 1
        break

    time = time[start_idx:stop_idx]

  return time, data, meta

def read_cdf(file, variables=None, start=None, stop=None, logger=None, use_cache=True):

  cdffile = open_cdf(file, logger=logger, use_cache=False)
  if cdffile is None:
    return None

  meta_all = read_cdf_meta(file, logger=None, use_cache=True)
  if variables is None:
    variables = list(meta_all.keys())

  data = {}
  meta = {}
  for variable in variables:
    try:
      data[variable] = cdffile.varget(variable=variable)
    except:
      data[variable] = None
    meta[variable] = meta_all[variable]
    meta[variable]['VarData'] = data[variable]

  return meta

def xprint_dict(meta):
  import pprint
  pp = pprint.PrettyPrinter(depth=4, compact=True)
  pp.pprint(meta)

def subset_meta(meta, DEPEND_0=None, VAR_TYPE='data', RecVariance=True):

  if DEPEND_0 is not None:
    depend_0s = [DEPEND_0]
  else:
    depend_0s = list(meta.keys())
  meta_sub = {}
  for depend_0 in depend_0s:
    meta_sub[depend_0] = {}
    for id, variable in meta[depend_0].items():

      a = True
      if 'VAR_TYPE' in variable:
        if VAR_TYPE is not None:
          a = variable['VAR_TYPE'] == VAR_TYPE
      else:
        a = False

      b = True
      if 'VAR_TYPE' in variable:
        if RecVariance is not None:
          b = variable and RecVariance
      else:
        b = False
      if a and b:
        meta_sub[depend_0][id] = variable

  if DEPEND_0 is not None:
    return meta_sub[DEPEND_0]

  return meta_sub

def read(dataset, data_dir=None, variables=None, start=None, stop=None, logger=None, use_cache=True):

  meta_master = cdawmeta.metadata(id=dataset, data_dir=data_dir, update=False, embed_data=True, no_orig_data=False)
  files_all = meta_master[dataset]['orig_data']['data']['FileDescription']
  files_needed = []
  start = start.strip()
  stop = stop.strip()
  for file in files_all:
    file_start = file['StartTime'].strip()
    file_stop = file['EndTime'].strip()
    if file_start >= start:
      files_needed.append(file['Name'])
    if file_stop >= stop:
      break

  for file in files_needed:
    data, meta = read_cdf(file, variables=variables, start=start, stop=stop, logger=logger, use_cache=use_cache)

  return data, meta

if __name__ == '__main__':
  if True:
    import cdawmeta
    id = 'AC_OR_SSC'

    metadata = cdawmeta.metadata(id=id, data_dir="../../data", embed_data=True, update=False, max_workers=1, diffs=False, restructure_master=True, no_spase=True, no_orig_data=False)

    import json
    Epoch = metadata[id]['master']['data']['CDFVariables']['Epoch']
    #print_dict(Epoch, sort=True)
    cdawmeta.util.print_dict(Epoch, sort=True)
    file = metadata[id]['samples']['file']
    #file = "https://cdaweb.gsfc.nasa.gov/pub/data/ace/mag/level_2_cdaweb/mfi_h0/1998/ac_h0_mfi_19980203_v04.cdf"
    #file = "https://cdaweb.gsfc.nasa.gov/pub/data/dscovr/h1/faraday_cup/2018/dscovr_h1_fc_20180603_v08.cdf"
    data = read_cdf(file)

    print('----')
    cdawmeta.util.print_dict(data['Epoch'], sort=True)
    exit()
    #data, meta = read(id, data_dir="../../data", start='1997-09-03T00:00:12.000Z', stop='1997-09-05T00:00:12.000Z')
    #files_needed = read(id, data_dir="../../data", start='1997-09-03T00:00:12.000Z', stop='1997-09-05T00:00:12.000Z')
    print_dict(files_needed)
    meta = read_cdf_meta(file)
    #print_dict(meta)
    data = read_cdf(file)
    print_dict(data['Epoch'])
    #exit()

    meta = read_cdf_meta(file, subset=True)

    depend_0s = list(meta.keys())
    meta_sub = subset_meta(meta, DEPEND_0=depend_0s[0])
    print_dict(meta_sub)
    print(list(meta_sub.keys()))
    #print(data[2] == min(data[1][0]))
    #print(min(data[1][0]))

  if False:
    cdffile = cdflib.CDF("ac_h0_mfi_19980203_v04.cdf")
    data = cdffile.varget(variable='Magnitude')
    meta = cdffile.varattsget(variable='Magnitude')
    vdata = cdffile.varinq('Magnitude')
    print(vdata)
    import numpy as np
    print(data.dtype)
    print( meta['FILLVAL'].dtype)
    print(np.sum(data == np.float32(-1e31)))
    print(np.sum(data == meta['FILLVAL']))
    #"AC_H0_MFI"&parameters=Magnitude&time.min=1998-02-03T05:57:00&time.max=1998-02-03T05:57:04&format=binary