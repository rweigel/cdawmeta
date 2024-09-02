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
    logger = cdawmeta.logger('metadata')
    logger.setLevel(log_level.upper())
  return logger

def ids(id=None, skip=None, update=True):

  # Needed to set logger for any called underscore functions.
  logger = _logger()

  def _remove_skips(ids_reduced):
    regex = re.compile(skip)
    return [id for id in ids_reduced if not regex.match(id)]

  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)
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

def metadata(id=None, skip=None, embed_data=False, update=True, regen=False, max_workers=1, diffs=False, log_level='info'):

  logger = _logger(log_level)

  if diffs and not update:
    logger.warning("diffs=True but update=False. No diffs can be computed.")

  dsids = ids(id=id, skip=skip, update=update)

  # Create base datasets using info in all.xml
  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)

  def get_one(dataset):
    dataset['master'] = _master(dataset, diffs=diffs, update=update)
    dataset['orig_data'] = _orig_data(dataset, diffs=diffs, update=update)
    dataset['spase'] = _spase(dataset['master'], diffs=diffs, update=update)
    dataset['hapi'] = cdawmeta.generate.hapi(dataset, regen=regen, update=update)
    dataset['soso'] = cdawmeta.generate.soso(dataset, regen=regen, update=update)

    if not embed_data:
      for key in ['master', 'orig_data', 'spase', 'hapi', 'soso']:
        if 'data' in dataset[key]:
          del dataset[key]['data']

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

def _datasets(allxml):
  '''
  Returns dict of datasets. Keys are dataset IDs and values are dicts 
  with keys 'id' and 'allxml'. The value of 'allxml' is
  all.xml/data/'sites//datasite/0/dataset
  '''

  restructure = True

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
      dataset['allxml'] = _restructure_allxml(dataset['allxml'], logger=logger)

    datasets_[id] = dataset

  return datasets_

def _allxml(update=False, diffs=False):

  timeout = cdawmeta.CONFIG['cdaweb']['timeouts']['allxml']

  if hasattr(_allxml, 'allxml'):
    # Use curried result (So update only updates all.xml once per execution of main program)
    return _allxml.allxml

  allurl = cdawmeta.CONFIG['cdaweb']['allurl']
  allxml = _fetch(allurl, 'all', 'all', timeout=timeout, update=update)
  # Curry result
  _allxml. allxml = allxml

  return allxml

def _master(dataset, update=False, diffs=False):

  restructure=True
  timeout = cdawmeta.CONFIG['cdaweb']['timeouts']['master']

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  master = _fetch(url, dataset['id'], 'master', timeout=timeout, update=update, diffs=diffs)
  if restructure and 'data' in master:
    master['data'] = _restructure_master(master['data'], logger=logger)
  return master

def _spase(master, update=True, diffs=False):

  restructure=True
  timeout = cdawmeta.CONFIG['cdaweb']['timeouts']['spase']

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

def _orig_data(dataset, update=True, diffs=False):

  timeout = cdawmeta.CONFIG['cdaweb']['timeouts']['orig_data']

  wsbase = cdawmeta.CONFIG['cdaweb']['wsbase']
  start = dataset['allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  stop = dataset['allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  headers = {'Accept': 'application/json'}
  return _fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, diffs=diffs)

def _restructure_allxml(allxml, logger=None):
  if 'mission_group' in allxml:
    mission_groups = cdawmeta.util.array_to_dict(allxml['mission_group'],'@ID')
    allxml['mission_group'] = mission_groups
  if 'instrument_type' in allxml:
    instrument_type = cdawmeta.util.array_to_dict(allxml['instrument_type'],'@ID')
    allxml['instrument_type'] = instrument_type
  if 'links' in allxml:
    links = cdawmeta.util.array_to_dict(allxml['link'],'@URL')
    allxml['links'] = links
  for key, val in allxml.items():
    #if val is not None and '@ID' in val:
      # e.g., allxml['observatory] = {'@ID': 'ACE', ...} ->
      #       allxml['observatory']['ACE'] = {'@ID': 'ACE', ...}
      #allxml[key] = {val['@ID']: val}
    # TODO: Read all.xsd file and check if any others are lists that converted to dicts.
    if isinstance(val, list):
      logger.warning(f"Warning[all.xml]: {id}: {key} is a list and was not restructured.")

  return allxml

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

  # CachedSession does not handle relative paths properly.
  if not os.path.isabs(cache_dir):
    cache_dir = os.path.abspath(cache_dir)

  session = requests_cache.CachedSession(cache_dir, **copts)
  session.mount('https://', HTTPAdapter(max_retries=5))

  return session

def _fetch_request_log(resp, diff):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = "\n"
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  if diff and 'diff' in diff:
    msg += f"  Current cache file: {diff['file_now']}\n"
    if 'file_last' in diff:
      msg += f"  Last cache file:    {diff['file_last']}\n"
  msg += "  Request Cache-Related Headers:\n"
  for k, v in req_cache_headers.items():
    print(k,v)
    msg += f"    {k}: {v}\n"
  msg += "  Response Cache-Related Headers:\n"
  for k, v in res_cache_headers.items():
    msg += f"    {k}: {v}\n"
  if diff and 'diff' in diff:
    if diff['diff'] is None or len(diff['diff']) == 0:
      msg += "  Cache diff: None\n"
    else:
      msg += "  Cache diff:\n    "
      json_indented = "\n    ".join(diff['diff'].to_json(indent=2).split('\n'))
      msg += f"{json_indented}\n"
  return msg

def _fetch_diff(cache_dir, cache_key):

  subdir = os.path.join(cache_dir, cache_key)
  file_last = os.path.join(subdir, cache_key + ".json")
  os.makedirs(subdir, exist_ok=True)

  file_now = os.path.join(cache_dir, cache_key + ".json")
  try:
    data_now = cdawmeta.util.read(file_now, logger=logger)
  except Exception as e:
    logger.error(f"Error reading {file_now}: {e}")
    return {"diff": None, "file_now": None, "file_last": None}

  if not os.path.exists(file_last):
    return {"diff": None, "file_now": file_now, "file_last": None}

  try:
    data_last = cdawmeta.util.read(file_last, logger=logger)
  except Exception as e:
    logger.error(f"Error reading {file_last}: {e}")
    return {"diff": None, "file": file_now, "file_last": None}

  diff = deepdiff.DeepDiff(data_last, data_now)
  file_diff = os.path.join(subdir, cache_key + ".diff.json")
  cdawmeta.util.write(file_diff, diff.to_json(), logger=logger)

  shutil.copyfile(file_now, file_last)

  return {"diff": diff, "file_now": file_now, "file_last": file_last}

def _fetch(url, id, what, headers=None, timeout=20, diffs=False, update=False):

  cache_dir = os.path.join(cdawmeta.DATA_DIR, 'CachedSession', what)
  file_out_json = os.path.join(cdawmeta.DATA_DIR, what, f"{id}.json")
  file_out_pkl = os.path.join(cdawmeta.DATA_DIR, what, f"{id}.pkl")

  if not update and os.path.exists(file_out_pkl):
    msg = "update = False; bypassing CachedSession request and using last result."
    logger.info(msg)
    data = cdawmeta.util.read(file_out_pkl, logger=logger)
    data['request']["log"] = msg
    return data

  session = CachedSession(cache_dir)

  logger.info('CachedSession get: ' + url)

  result = {'id': id, 'url': url}
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
    logger.error(msg)
    result['error'] = msg
    return result

  cache_file = os.path.join(cache_dir, resp.cache_key + ".json")

  result['id'] = id
  result['url'] = url
  result['data-file'] = file_out_json
  result['data'] = json_dict

  result['request'] = {}
  result['request']["url"] = url
  result['request']["file"] = cache_file
  result['request']["file-header"] = dict(resp.headers)

  diff = None
  if diffs:
    logger.info("Computing diff")
    diff = _fetch_diff(cache_dir, resp.cache_key)
    result['request']["diff"] = diff

  #_fetch_request_log(resp, url, diff)
  result['request']["log"] = _fetch_request_log(resp, diff)
  logger.info(result['request']["log"])

  if not os.path.exists(file_out_json):
    try:
      cdawmeta.util.write(file_out_json, result, logger=logger)
    except Exception as e:
      logger.error(f"Error writing {file_out_json}: {e}")

  if not os.path.exists(file_out_pkl):
    try:
      cdawmeta.util.write(file_out_pkl, result, logger=logger)
    except Exception as e:
      logger.error(f"Error writing {file_out_pkl}: {e}")

  return result
