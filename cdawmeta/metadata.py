import os
import re
import shutil
import xmltodict
import deepdiff

import cdawmeta

# TODO: Find a better way to handle this.
# Can't call cdawmeta.logger('cdaweb') here because it calls cdawmeta.DATA_DIR
# which is set to a default. If user modifies using cdawmeta.DATA_DIR = ...,
# logger does not know about the change.
logger = None
def _logger(log_level='info'):
  global logger
  if logger is None:
    logger = cdawmeta.logger('cdaweb')
    logger.setLevel(log_level.upper())
  return logger

def ids(id=None, skip=None, update=True):

  # Needed to set logger for any called underscore functions.
  logger = _logger()

  def _remove_skips(ids_reduced):
    regex = re.compile(skip)
    return [id for id in ids_reduced if not regex.match(id)]

  timeouts = cdawmeta.CONFIG['cdaweb']['timeouts']
  allxml = _allxml(timeout=timeouts['allxml'], update=update)
  datasets_all = _datasets(allxml, update=update)
  ids_all = datasets_all.keys()

  if id is None:
    return _remove_skips(ids_all)

  if isinstance(id, str):
    if id.startswith('^'):
      regex = re.compile(id)
      ids_reduced = [id for id in ids_all if regex.match(id)]
      if len(ids_reduced) == 0:
        raise ValueError(f"Error: id = {id}: No matches.")
    elif id not in ids_all:
      raise ValueError(f"Error: id = {id}: Not found.")
    else:
      ids_reduced = [id]

  if skip is None:
    return ids_reduced

  return _remove_skips(ids_reduced)

def metadata(id=None, skip=None, embed_data=False, update=True, max_workers=1, diffs=False,
             restructure_allxml=True, restructure_master=True, restructure_spase=True,
             spase=False, orig_data=False, log_level='info'):

  logger = _logger(log_level)
  timeouts = cdawmeta.CONFIG['cdaweb']['timeouts']

  dsids = ids(id=id, skip=skip, update=update)

  # Create base datasets using info in all.xml
  allxml = _allxml(timeout=timeouts['allxml'], update=update)
  datasets_all = _datasets(allxml, restructure=restructure_allxml, update=False)

  def get_one(dataset):
    dataset['master'] = _master(dataset, restructure=restructure_master, diffs=diffs, timeout=timeouts['master'], update=update)
    if orig_data:
      dataset['orig_data'] = _orig_data(dataset, diffs=diffs, timeout=timeouts['orig_data'], update=update)
      dataset['samples'] = _samples(dataset['id'], dataset['orig_data'])
    if spase:
      dataset['spase'] = _spase(dataset['master'], restructure=restructure_spase, diffs=diffs, timeout=timeouts['spase'], update=update)

    if not embed_data:
      del dataset['master']['data']
      if 'orig_data' in dataset and 'data' in dataset['orig_data']:
        del dataset['orig_data']['data']
      if 'spase' in dataset and dataset['spase'] is not None and 'data' in dataset['spase']:
        del dataset['spase']['data']

  if max_workers == 1 or len(dsids) == 1:
    for id in dsids:
      get_one(datasets_all[id])
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(id):
      try:
        get_one(datasets_all[id])
      except Exception as e:
        import traceback
        logger.error(f"Error: {datasets_all['id']}: {traceback.print_exc()}")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call, dsids)

  return {key: datasets_all[key] for key in dsids}

def _datasets(allxml, restructure=True, update=False):
  '''
  Returns dict of datasets. Keys are dataset IDs and values are dicts 
  with keys 'id' and 'allxml'. The value of 'allxml' is
  all.xml/data/'sites//datasite/0/dataset
  '''

  datasites = cdawmeta.util.get_path(allxml, ['data', 'sites', 'datasite'])
  if datasites is None:
    raise Exception("Error[all.xml]: No 'sites/datasite' node in all.xml")

  datasets_allxml = None
  for datasite in datasites:
    if datasite['@ID'] == 'CDAWeb_HTTPS':
      datasets_allxml = datasite['dataset']
      break
  if datasets_allxml is None:
    raise Exception("Error[all.xml]: No 'sites/datasite' with ID=CDAWeb_HTTPS")

  datasets_ = {}
  for dataset_allxml in datasets_allxml:

    id = dataset_allxml['@serviceprovider_ID']
    dataset = {'id': id, 'allxml': dataset_allxml}

    if 'mastercdf' not in dataset_allxml:
      msg = f'Error[all.xml]: {id}: No mastercdf node in all.xml'
      logger.error(msg)
      continue

    if isinstance(dataset_allxml['mastercdf'], list):
      if not id.endswith('_MOVIES'):
        msg = f'Warning[all.xml]: Not implemented: {id}: More than one mastercdf referenced in all.xml mastercdf node.'
        logger.warning(msg)
      continue

    if '@ID' not in dataset_allxml['mastercdf']:
      msg = f'Error[all.xml]: {id}: No ID attribute in all.xml mastercdf node'
      logger.error(msg)
      continue

    if restructure:
      if 'mission_group' in dataset['allxml']:
        mission_groups = cdawmeta.util.array_to_dict(dataset['allxml']['mission_group'],'@ID')
        dataset['allxml']['mission_group'] = mission_groups
      if 'instrument_type' in dataset['allxml']:
        instrument_type = cdawmeta.util.array_to_dict(dataset['allxml']['instrument_type'],'@ID')
        dataset['allxml']['instrument_type'] = instrument_type
      if 'links' in dataset['allxml']:
        links = cdawmeta.util.array_to_dict(dataset['allxml']['link'],'@URL')
        dataset['allxml']['links'] = links
      for key, val in dataset['allxml'].items():
        #if val is not None and '@ID' in val:
          # e.g., dataset['allxml']['observatory] = {'@ID': 'ACE', ...} ->
          #       dataset['allxml']['observatory']['ACE'] = {'@ID': 'ACE', ...}
          #dataset['allxml'][key] = {val['@ID']: val}
        # TODO: Read all.xsd file and check if any others are lists that converted to dicts.
        if isinstance(val, list):
          logger.warning(f"Warning[all.xml]: {id}: {key} is a list and was not restructured.")
          exit()

    datasets_[id] = dataset

  return datasets_

def _allxml(timeout=20, update=False, diffs=False):

  if hasattr(_allxml, 'allxml'):
    # Use curried result (So update only updates all.xml once per execution of main program)
    return _allxml.allxml

  allurl = cdawmeta.CONFIG['cdaweb']['allurl']
  allxml = _fetch(allurl, 'all', 'all', timeout=timeout, update=update)
  # Curry result
  _allxml. allxml = allxml

  return allxml

def _master(dataset, restructure=True, timeout=20, update=False, diffs=False):

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  master = _fetch(url, dataset['id'], 'master', timeout=timeout, update=update, diffs=diffs)
  if restructure and 'data' in master:
    master['data'] = _restructure_master(master['data'], logger=logger)
  return master

def _spase(master, restructure=True, timeout=20, update=True, diffs=False):

  id = master['id']
  if 'data' not in master:
    return {'error': 'No spase_DatasetResourceID because no master'}

  global_attributes = master['data']['CDFglobalAttributes']
  if 'spase_DatasetResourceID' not in global_attributes:
    msg = f"Error[master]: {master['id']}: Missing or invalid spase_DatasetResourceID attribute in master"
    logger.error(msg)
    return {'error': msg}

  if 'spase_DatasetResourceID' in global_attributes:
    spase_id = global_attributes['spase_DatasetResourceID']
    if spase_id and not spase_id.startswith('spase://'):
      msg = f"Error[master]: {master['id']}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://'"
      logger.error(msg)
      return {'error': msg}
    url = spase_id.replace('spase://', 'https://hpde.io/') + '.json'

  spase = _fetch(url, id, 'spase', timeout=timeout, diffs=diffs, update=update)

  if restructure and 'data' in spase:
    spase['data'] = _restructure_spase(spase['data'], logger=logger)
  return spase

def _orig_data(dataset, timeout=120, update=True, diffs=False):

  wsbase = cdawmeta.CONFIG['cdaweb']['wsbase']
  start = dataset['allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  stop = dataset['allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  #headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
  headers = {'Accept': 'application/json'}
  return _fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, diffs=diffs)

def _samples(id, _orig_data):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  wsbase = cdawmeta.CONFIG['cdaweb']['wsbase']

  last_file = _orig_data['data']['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  _samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return _samples

def _restructure_spase(spase, logger=None):
  if 'Spase' not in spase:
    return spase
  if 'NumericalData' not in spase['Spase']:
    return spase
  if 'Parameter' in spase['Spase']['NumericalData']:
    for pidx, parameter in enumerate(spase['Spase']['NumericalData']['Parameter']):
      if 'ParameterKey' not in parameter:
        logger.error(f'Error[SPASE]: No ParameterKey in Parameter array element {pidx}: {parameter}')
    data = spase['Spase']['NumericalData']['Parameter']
    spase['Spase']['NumericalData']['Parameter'] = cdawmeta.util.array_to_dict(data, 'ParameterKey', ignore_error=True)
  return spase

def _restructure_master(master, logger=None):

  """
  Convert dict with arrays of objects to objects with objects. For example
    { "Epoch": [ 
        {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
        {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
      ],
      ...
    }
  is converted to
    {
      "Epoch": {
        "VarDescription": {
          "DataType": "CDF_TIME_TT2000",
          ...
        },
        "VarAttributes": {
          "CATDESC": "Default time",
          ...
        }
      }
    }
  """

  def sort_keys(obj):
    return {key: obj[key] for key in sorted(obj)}

  # TODO: Check that only one key.
  file = list(master.keys())[0]

  fileinfo_r = cdawmeta.util.array_to_dict(master[file]['CDFFileInfo'])

  variables = master[file]['CDFVariables']
  variables_r = {}

  for variable in variables:

    variable_keys = list(variable.keys())
    if len(variable_keys) > 1:
      msg = "Expected only one variable key in variable object. Exiting witih code 1."
      logger.error(msg)
      exit(1)

    variable_name = variable_keys[0]
    variable_array = variable[variable_name]
    variable_dict = cdawmeta.util.array_to_dict(variable_array)

    for key, value in variable_dict.items():

      if key == 'VarData':
        variable_dict[key] = value
      else:
        variable_dict[key] = sort_keys(cdawmeta.util.array_to_dict(value))

    variables_r[variable_name] = variable_dict

  # Why do they use lower-case G? Inconsistent with CDFFileInfo and CDFVariables.
  globals = master[file]['CDFglobalAttributes'] 
  globals_r = {}

  for _global in globals:
    gkey = list(_global.keys())
    if len(gkey) > 1:
      if logger is not None:
        msg = "Expected only one key in _global object."
        logger.error(msg)
    gvals = _global[gkey[0]]
    text = []
    for gval in gvals:
      line = gval[list(gval.keys())[0]]
      text.append(str(line))

    globals_r[gkey[0]] = "\n".join(text)

  master = {
              'CDFFileInfo': {'FileName': file, **fileinfo_r},
              'CDFglobalAttributes': globals_r,
              'CDFVariables': variables_r
            }

  return master

def CachedSession(cache_dir):
  import requests_cache
  from requests.adapters import HTTPAdapter

  # https://requests-cache.readthedocs.io/en/stable/#settings
  # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html

  import logging
  logging.getLogger("requests").setLevel(logging.ERROR)
  logging.getLogger('requests_cache').setLevel(logging.ERROR)
  logging.getLogger("urllib3").setLevel(logging.ERROR)

  # Cache dir
  copts = {
    # Save files in the default user cache dir
    "use_cache_dir": True,

    # Use Cache-Control response headers for expiration, if available
    "cache_control": True,

    # Expire responses after expire_after if no cache control header
    # This causes caching to not work.
    # "expire_after": expire_after,

    # Cache responses with these status codes
    "allowable_codes": [200],

    # In case of request errors, use stale cache data if possible
    "stale_if_error": True,

    "serializer": "json",

    # This causes caching to not work unless decode_content = False
    # See https://github.com/requests-cache/requests-cache/issues/963
    "backend": "filesystem",

    "decode_content": False
  }

  session = requests_cache.CachedSession(cache_dir, **copts)
  session.mount('https://', HTTPAdapter(max_retries=5))

  return session

def _cache_info(cache_dir, cache_key, diffs=False):

  cache_file = os.path.join(cache_dir, cache_key + ".json")
  cache_subdir = os.path.join(cache_dir, cache_key)
  cache_file_copy = os.path.join(cache_subdir, cache_key + ".json")
  os.makedirs(cache_subdir, exist_ok=True)

  _return = {"file": cache_file}

  if diffs:
    if os.path.exists(cache_file):
      try:
        now = cdawmeta.util.read(cache_file, logger=logger)
      except Exception as e:
        logger.error(f"Error reading {cache_file}: {e}")
        return {"file": None, "file_last": cache_file_copy, "diff": None}

    if os.path.exists(cache_file):
      try:
        last = cdawmeta.util.read(cache_file_copy, logger=logger)
      except Exception as e:
        logger.error(f"Error reading {cache_file_copy}: {e}")
        return {"file": cache_file, "file_last": None, "diff": None}

      diff = deepdiff.DeepDiff(last, now)
      _return['cache_copy'] = cache_file_copy
      _return['diff'] = diff
      cache_diff_file = os.path.join(cache_subdir, cache_key + ".diff.json")
      cdawmeta.util.write(cache_diff_file, diff.to_json(), logger=logger)
      shutil.copyfile(cache_file, cache_file_copy)

  return _return

def _print_request_log(resp, url, cache):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = ""
  msg += f'Got: {url}\n'
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  msg += f"  Current cache file: {cache['file']}\n"
  if 'file_last' in cache:
    msg += f"  Last cache file:    {cache['file_last']}\n"
  msg += "  Request Cache-Related Headers:\n"
  for k, v in req_cache_headers.items():
    print(k,v)
    msg += f"    {k}: {v}\n"
  msg += "  Response Cache-Related Headers:\n"
  for k, v in res_cache_headers.items():
    msg += f"    {k}: {v}\n"
  if 'diff' in cache:
    if len(cache['diff']) == 0:
      msg += "  Cache diff: None\n"
    else:
      msg += "  Cache diff:\n    "
      json_indented = "\n    ".join(cache['diff'].to_json(indent=2).split('\n'))
      msg += f"{json_indented}\n"
  if logger is not None:
    logger.info(msg)
  return msg

def _fetch(url, id, what, headers=None, timeout=20, diffs=False, update=False):

  cache_dir = os.path.join(cdawmeta.DATA_DIR, 'cache', what)
  file_out_json = os.path.join(cdawmeta.DATA_DIR, what, f"{id}.json")
  file_out_pkl = os.path.join(cdawmeta.DATA_DIR, what, f"{id}.pkl")

  if not update and os.path.exists(file_out_pkl):
    return cdawmeta.util.read(file_out_pkl, logger=logger)

  session = CachedSession(cache_dir)

  if logger is not None:
    logger.info('Get: ' + url)

  master = {}
  try:
    resp = session.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    if resp.headers['Content-Type'] == 'text/xml':
      text = resp.text
      json_dict = xmltodict.parse(text)
    else:
      json_dict = resp.json()

  except Exception as e:
    msg = f"Error[{what}]: {id}: {e}"
    if logger is not None:
      logger.error(msg)
    master['error'] = msg
    return master

  cache = _cache_info(cache_dir, resp.cache_key, diffs=diffs)

  master['id'] = id
  master['request-log'] = _print_request_log(resp, url, cache)
  master['request-cache'] = cache['file'].replace(cdawmeta.DATA_DIR + "/", '')
  master['url'] = url
  master['data'] = json_dict
  master['data-cache'] = file_out_json.replace(cdawmeta.DATA_DIR + "/", '')

  if resp.from_cache:
    return master

  try:
    cdawmeta.util.write(file_out_json, master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_json}: {e}")

  try:
    cdawmeta.util.write(file_out_pkl, master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_pkl}: {e}")

  return master
