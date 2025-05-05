import os

import cdflib

import cdawmeta

def open_cdf(file, logger=None, cache_dir=None, use_cache=True):

  cache_dir = _cache_dir(cache_dir)

  if file.startswith('http'):
    # File is URL
    kwargs = {
      'logger': logger,
      'stream': True        # Stream to file, don't return data.
    }
    file_out = os.path.join(cache_dir, _url2file(file))
    if use_cache and os.path.exists(file_out) and logger is not None:
      logger.info("use_cache = True and cached file found. Using it.")
    else:
      info = cdawmeta.util.get_conditional(file, file_out, **kwargs)
      if 'emsg' in info:
        if logger is not None:
          logger.error(f"Error: {file}: {info['emsg']}")
        return None

  try:
    cdffile = cdflib.CDF(file_out)
    return cdffile
  except Exception as e:
    if logger is not None:
      logger.error(f"Error opening {file_out}: {e}")

def read_cdf_depend_0s(file, logger=None, cache_dir=None, use_cache=True):

  cache_dir = _cache_dir(cache_dir)

  cdffile = open_cdf(file, logger=logger, cache_dir=cache_dir, use_cache=use_cache)
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

  return list(set(depend_0s))

def read_cdf_depend_0s_test(logger=None, cache_dir=None, use_cache=True):
  test_config = _test_config()
  for id in test_config.keys():
    print(f"Testing {id}")
    metadatum = test_config[id]['metadata']
    depend_0s = read_cdf_depend_0s(metadatum['sample_links']['data']['file'], logger=logger, cache_dir=cache_dir, use_cache=use_cache)
    ok = len(depend_0s) == len(test_config[id]['depend_0s'])
    if ok:
      print(f"PASS: Number of DEPEND_0s expected = {len(test_config[id]['depend_0s'])} = number found = {len(depend_0s)}")
    else:
      print(f"FAIL: Number of DEPEND_0s expected = {len(test_config[id]['depend_0s'])} != number found = {len(depend_0s)}")

  return ok

def read_cdf_meta(file, subset=False, resolve=False, logger=None, cache_dir=None, use_cache=True):

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
        'Version': 'FileVersion'
  }

  def file_info(info, file=None, logger=None):

    if hasattr(info, 'CDF'):
      file_path = str(info.CDF)
    else:
      raise ValueError(f"No CDF attribute in cdffile.cdf_info() result for {file}")

    CDFFileInfo = {"File": file_path}
    if file is not None:
      if file.startswith('http'):
        CDFFileInfo['FileSource'] = file
      if file != file_path:
        CDFFileInfo['FileSource'] = file

    unused = ['Attributes', 'CDF', 'Copyright', 'Num_rdim', 'rDim_sizes', 'rVariables', 'zVariables']
    used = ['Checksum', 'Compressed', 'Encoding', 'Format', 'LeapSecondUpdate', 'Version']

    # TODO: For Encoding, translate integer to string, e.g. 'NETWORK' appears in
    # master CDF JSONs. In what is returned by cdflib for data CDFs, it is an integer.

    for attr in used:
      if hasattr(info, attr):
        value = getattr(info, attr)
      if attr in method_map:
        attr = method_map[attr]
      CDFFileInfo[attr] = value

    if hasattr(info, 'Majority'):
      used.append('Majority')
      Majority = info.Majority
      if Majority == 'Row_major':
        Majority = 'ROW'
      else:
        Majority = 'COLUMN'
      CDFFileInfo['Majority'] = Majority

    knowns = [*used, *unused]
    for attr in dir(info):
      if attr.startswith('__'):
        continue
      if attr not in knowns:
        if logger is not None:
          logger.error(f"Unhandled attribute '{attr}' found in {file}")

    return CDFFileInfo

  cache_dir = _cache_dir(cache_dir)

  cdffile = open_cdf(file, logger=logger, cache_dir=cache_dir, use_cache=use_cache)
  if cdffile is None:
    return None

  depend_0s = []
  info = cdffile.cdf_info()

  CDFFileInfo = file_info(info, file=file, logger=logger)

  CDFglobalAttributes = cdffile.globalattsget()
  for key, val in CDFglobalAttributes.items():
    if isinstance(val, list):
      if all(isinstance(item, str) for item in val):
        CDFglobalAttributes[key] = "\n".join(val)

  CDFVariables = {}
  rVariables = info.rVariables
  zVariables = info.zVariables
  for variable in rVariables + zVariables:
    CDFVariables[variable] = {'VarDescription': {}, 'VarAttributes': None}
    CDFVariables[variable]['VarAttributes'] = cdffile.varattsget(variable)
    # Add information that is in Master CDF JSONs
    vdata = cdffile.varinq(variable)
    # Use convention used in Master CDF JSONs for names
    methods = dir(vdata)
    for method in methods:
      if not method.startswith('_'):
        if method in method_map:
          method_renamed = method_map[method]
        else:
          print("??? Method not in map: ", method)
          method_renamed = method
        CDFVariables[variable]['VarDescription'][method_renamed] = getattr(vdata, method)

    if subset and 'DEPEND_0' in CDFVariables[variable]:
      depend_0s.append(CDFVariables[variable]['DEPEND_0'])

  if False and subset:
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

  metadatum = {
        "CDFFileInfo": CDFFileInfo,
        "CDFglobalAttributes": CDFglobalAttributes,
        "CDFVariables": CDFVariables
      }

  return metadatum

def read_cdf(file, variables=None, depend_0=None, start=None, stop=None, iso8601=True, logger=None, cache_dir=None, use_cache=True):

  def record_range(epoch, start_iso, stop_iso):

    lens = {
      'CDF_EPOCH': 23,
      'CDF_EPOCH16': 32,
      'CDF_TIME_TT2000': 29
    }
    dataType = epoch['VarDescription']['DataType']
    if dataType not in lens.keys():
      msg = f"Data type {epoch['VarDescription']['DataType']} is not one of {lens.keys()}"
      raise ValueError(msg)

    to = cdflib.cdfepoch.parse(cdawmeta.util.pad_iso8601(start_iso)[0:lens[dataType]])
    tf = cdflib.cdfepoch.parse(cdawmeta.util.pad_iso8601(stop_iso)[0:lens[dataType]])
    starttime = to.item()
    endtime = tf.item()

    rr = cdflib.cdfepoch.findepochrange(epoch['VarData'], starttime=starttime, endtime=endtime)
    if rr is None or len(rr) == 0:
      msg = f"Could not find record range for {epoch['VariableName']} between {start_iso} and {stop_iso}"
      raise ValueError(msg)

    return [rr[0], rr[-1]]

  cache_dir = _cache_dir(cache_dir)

  cdffile = open_cdf(file, logger=logger, cache_dir=cache_dir, use_cache=use_cache)
  if cdffile is None:
    return None

  meta_all = read_cdf_meta(file, logger=None, cache_dir=cache_dir, use_cache=True)

  if isinstance(variables, str):
    variables = variables.split(",")

  if depend_0 is not None:
    if depend_0 not in meta_all:
      raise ValueError(f"DEPEND_0 = '{depend_0}' is not a variable in file {file}")

    variables_depend_0 = []
    for variable in list(meta_all.keys()):
      if 'DEPEND_0' in meta_all[variable]['VarAttributes']:
        if meta_all[variable]['VarAttributes']['DEPEND_0'] == depend_0:
          variables_depend_0.append(variable)

    if len(variables_depend_0) == 0:
      raise ValueError(f"No variables in {file} depend on {depend_0}")

    for variable in variables:
      if variable not in variables_depend_0:
        raise ValueError(f"Requsted variable {variable} does not have DEPEND_0 = {depend_0}")

    variables = variables_depend_0

  if variables is None:
    variables = list(meta_all.keys())

  meta = {}
  for variable in variables:
    meta[variable] = meta_all['CDFVariables'][variable]

    rr = {} # record range
    depend_0 = None
    if start is not None and stop is not None:
      if 'DEPEND_0' in meta[variable]['VarAttributes']:
        depend_0 = meta[variable]['VarAttributes']['DEPEND_0']
        if depend_0 not in meta:
          meta[depend_0] = meta_all[depend_0]
        if 'VarData' not in meta[depend_0]:
          if logger is not None:
            logger.info(f"Reading DEPEND_0 = '{depend_0}' for variable = '{variable}'")
          meta[depend_0]['VarData'] = cdffile.varget(variable=depend_0)

        rr[depend_0] = record_range(meta[depend_0], start, stop)

    try:
      if depend_0 is None:
        if logger is not None:
          logger.info(f"Reading variable = {variable}")
        meta[variable]['VarData'] = cdffile.varget(variable=variable)
      else:
        if logger is not None:
          logger.info(f"Reading variable = {variable}[{rr[depend_0][0]}:{rr[depend_0][1]}]")
        meta[variable]['VarData'] = cdffile.varget(variable=variable, startrec=rr[depend_0][0], endrec=rr[depend_0][1])
    except Exception as e:
      if depend_0 is None:
        call_str = f"cdffile.varget(variable='{variable}')"
      else:
        call_str = f"cdffile.varget(variable='{variable}', startrec={rr[depend_0][0]}, endrec={rr[depend_0][1]})"
      if logger is not None:
        logger.info(f"Error executing {call_str} for file {file}:\n  {e}")
      meta[variable]['VarData'] = None

    if iso8601:
      try:
        varDescription = meta[variable]['VarDescription']
        time_variable = varDescription['DataType'].startswith('CDF_EPOCH')
        time_variable = time_variable or (varDescription['DataType'] == 'CDF_TIME_TT2000')
        if time_variable:
          epoch = cdflib.cdfepoch.encode(meta[variable]['VarData'], iso_8601=True)
          meta[variable]['VarDataISO8601'] = epoch
      except Exception as e:
        if logger is not None:
          logger.error(f"Error when executing cdflib.cdfepoch.encode(..., iso_8601=True) for {variable}:\n  {e}")
        meta[variable]['VarDataISO8601'] = None

  return meta

def files(id=id, start=None, stop=None, logger=None, cache_dir=None, update=False):

  cache_dir = _cache_dir(cache_dir)

  if logger is not None:
    logger.info(f"Getting file URLs for {id} between {start} and {stop}")

  metadata = cdawmeta.metadata(id=id, meta_type='orig_data', update=update, embed_data=True, write_catalog=False)
  files_all = metadata[id]['orig_data']['data']['FileDescription']
  if logger is not None:
    logger.info(f"Total of {len(files_all)} URLs for {id}")

  files_needed = []
  start = cdawmeta.util.pad_iso8601(start.strip())
  stop = cdawmeta.util.pad_iso8601(stop.strip())
  #print(start, stop)
  for file in files_all:
    #print(file['StartTime'], file['EndTime'], file['Name'].split('/')[-1])
    file_start = file['StartTime'].strip().replace("Z", "")
    file_stop = file['EndTime'].strip().replace("Z", "")
    if file_start >= start[0:len(file_start)]:
      files_needed.append(file['Name'])
    if file_stop >= stop:
      break

  if logger is not None:
    logger.info(f"Total {len(files_needed)} URLs for {id} between {start} and {stop}")

  return files_needed

def read_cdf_test1(id=None, depend_0=None, variable=None):
  """
  Read a data variable from three files with the three different DataTypes
  for a DEPEND_0 variable (CDF_EPOCH, CDF_EPOCH16, and CDF_TIME_TT2000).

  Read all data for the DEPEND_0 variable, gets the start and stop times, and
  then reads the data variable in the range [start, stop] and verify that the
  number of returned records matches that for all data for the DEPEND_0 variable.
  """

  test_config = _test_config()

  if id is None:
    for id in test_config.keys():
      print(f"Testing {id}")
      depend_0 = test_config[id]['depend_0s'][0]
      read_cdf_test1(id=id, depend_0=depend_0, variable=test_config[id]['variable'])
    return

  metadatum = test_config[id]['metadata']
  #cdawmeta.util.print_dict(metadata)
  master = metadatum['master']['data']

  depend_0_ = master['CDFVariables'][depend_0]
  variable_ = master['CDFVariables'][variable]

  print('')

  print(f"Master metadata for {id}/{depend_0} (DEPEND_0 for {variable})")
  cdawmeta.util.print_dict(depend_0_, sort_dicts=True)

  print('')

  print(f"Master metadata for id = {id}/{variable}")
  cdawmeta.util.print_dict(variable_, sort_dicts=True)

  print('')

  file = metadatum['sample_links']['data']['file']
  print(f"Reading data for {id}/{depend_0} (DEPEND_0 for {variable}) in sample file = {file}")
  data = read_cdf(file, variables=depend_0, iso8601=False)

  print(f"File content for id = {id}/{depend_0}")
  cdawmeta.util.print_dict(data[depend_0], sort_dicts=True)

  epoch = data[depend_0]
  start_iso = cdflib.cdfepoch.encode(epoch['VarData'][0], iso_8601=True)
  stop_iso = cdflib.cdfepoch.encode(epoch['VarData'][-1], iso_8601=True)

  print(f"{id}/{depend_0} has {len(epoch['VarData'])} records in range [{start_iso}, {stop_iso}]")

  print('')

  print(f"Reading data in sample file for {id}/{variable} in range [{start_iso}, {stop_iso}]")
  data = read_cdf(file, variables=variable, start=start_iso, stop=stop_iso, iso8601=False)

  ok = len(epoch['VarData']) == len(data[variable]['VarData'])
  if ok:
    print(f"PASS: Number of records expected = {len(epoch['VarData'])} = number of records found = {len(data[variable]['VarData'])}")
  else:
    print(f"FAIL: Number of records expected = {len(epoch['VarData'])} != number of records found = {len(data[variable]['VarData'])}")

  return ok

def read_cdf_test2(id=None, depend_0=None, variable=None):

  test_config = _test_config()

  if id is None:
    for id in test_config.keys():
      print(f"Testing {id}")
      depend_0 = test_config[id]['depend_0s'][0]
      read_cdf_test2(id=id, depend_0=depend_0, variable=test_config[id]['variable'])
    return

  metadatum = test_config[id]['metadata']

  file = metadatum['sample_links']['data']['file']
  print(f"Reading data for {id}/{depend_0} (DEPEND_0 for {variable}) in sample file = {file}")
  data = read_cdf(file, variables=variable, depend_0=depend_0, iso8601=False)
  cdawmeta.util.print_dict(data, sort_dicts=True)

def _cache_dir(dir):
  if dir is None:
    dir = os.path.join(cdawmeta.DATA_DIR, 'cdaweb.gsfc.nasa.gov')
  return os.path.abspath(dir)

def _url2file(url):
  return url.replace("https://cdaweb.gsfc.nasa.gov/", "")

def _test_config():

  kwargs = {
    "update": False,
    "max_workers": 1,
    "diffs": False
  }

  tests = {
    'AC_OR_SSC': {
      'start': None,
      'stop': None,
      'depend_0s': ['Epoch'],
      'variable': 'RADIUS'
    },
    'C1_WAVEFORM_WBD': {
      'start': None,
      'stop': None,
      'depend_0s': ['Epoch'],
      'variable': 'Bandwidth'
    },
    'BAR_1A_L2_EPHM': {
      'start': None,
      'stop': None,
      'depend_0s': ['Epoch'],
      'variable': 'Quality'
    },
    'VOYAGER1_10S_MAG': {
      'start': None,
      'stop': None,
      'depend_0s': ['Epoch','Epoch2'],
      'variable': 'magStatus'
    }
  }

  for id in tests.keys():
    tests[id]['metadata'] = cdawmeta.metadata(id=id, **kwargs)[id]

  return tests

def _subset_meta(meta, DEPEND_0=None, VAR_TYPE='data', RecVariance=True):

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

if __name__ == '__main__':

  if False:
    read_cdf_test1()
    read_cdf_test2()
    read_cdf_depend_0s_test()

  if True:
    id = 'AC_OR_SSC'
    metadatum = cdawmeta.metadata(id=id, meta_type=['master','orig_data'])
    orig_data = metadatum[id]['orig_data']['data']
    file = orig_data['FileDescription'][0]['Name']
    meta_file = read_cdf_meta(file)

    metadatum = cdawmeta.metadata(id='AC_OR_SSC', meta_type='master')
    meta_master = metadatum['AC_OR_SSC']['master']['data']

    cdawmeta.util.compare_dicts(meta_master, meta_file, restructure_str=True)
