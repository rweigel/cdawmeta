import os
import re
import shutil
import xmltodict
import deepdiff

from . import util

# These are set in call to metadata()
DATA_DIR = None
logger = None

allurl = 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
wsbase = 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/'

def logger_config():

  config = {
    'name': 'cdaweb.py',
    'file_log': os.path.join(DATA_DIR, 'cdaweb.log'),
    'file_error': os.path.join(DATA_DIR, 'cdaweb.errors.log'),
    'format': '%(message)s',
    'rm_string': DATA_DIR + '/'
  }
  return config

def metadata(id=None, embed_data=False, update=True, max_workers=1, diffs=False, timeouts=None, no_spase=True, no_orig_data=True):

  global DATA_DIR
  global logger
  from . import DATA_DIR
  logger = util.logger(**logger_config())

  if timeouts is None:
    timeouts = {
      'allxml': 30,
      'master': 30,
      'spase': 30,
      'orig_data': 120
  }

  datasets_ = datasets(timeout=timeouts['allxml'], update=update)
  ids_all = datasets_.keys()
  if isinstance(id, str):
    if id.startswith('^'):
      ids = []
      for id_ in list(ids_all):
        if re.search(id, id_):
          ids.append(id_)
    elif id not in ids_all:
      logger.error(f"Error: {id}: Not found.")
      return None
    else:
      ids = [id]
    datasets_reduced = {}
    for id in ids:
      datasets_reduced[id] = datasets_[id]
    datasets_ = datasets_reduced
  if id is None:
    ids = list(datasets_.keys())

  def get_one(dataset):
    dataset['master'] = master(dataset, diffs=diffs, timeout=timeouts['master'], update=update)
    if no_orig_data == False:
      dataset['orig_data'] = orig_data(dataset, diffs=diffs, timeout=timeouts['orig_data'], update=update)
      dataset['samples'] = samples(dataset['id'], dataset['orig_data'])
    if no_spase == False:
      dataset['spase'] = spase(dataset['master'], diffs=diffs, timeout=timeouts['spase'], update=update)

    if embed_data == False:
      del dataset['master']['data']
      del dataset['orig_data']['data']
      del dataset['spase']['data']

  if max_workers == 1:
    for id in ids:
      get_one(datasets_[id])
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(id):
      try:
        get_one(datasets_[id])
      except Exception as e:
        import traceback
        logger.error(f"Error: {datasets_['id']}: {traceback.print_exc()}")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call, ids)

  return datasets_

def rel_path(base_dir, path):
  return path.replace(base_dir + '/', '')

def CachedSession(cache_dir):
  import requests_cache
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

  return requests_cache.CachedSession(cache_dir, **copts)

def print_request_log(resp, url, cache):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = ""
  msg += f'Got: {url}\n'
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  msg += f"  Current cache file: {rel_path(DATA_DIR, cache['file'])}\n"
  if 'file_last' in cache:
    msg += f"  Last cache file:    {rel_path(DATA_DIR, cache['file_last'])}\n"
  msg += f"  Request Cache-Related Headers:\n"
  for k, v in req_cache_headers.items():
    msg += f"    {k}: {v}\n"
  msg += f"  Response Cache-Related Headers:\n"
  for k, v in res_cache_headers.items():
    msg += f"    {k}: {v}\n"
  if 'diff' in cache:
    if len(cache['diff']) == 0:
      msg += f"  Cache diff: None\n"
    else:
      msg += f"  Cache diff:\n    "
      json_indented = "\n    ".join(cache['diff'].to_json(indent=2).split('\n'))
      msg += f"{json_indented}\n"
  if logger is not None:
    logger.info(msg)
  return msg

def cache_info(cache_dir, cache_key, diffs=False):

  cache_file = os.path.join(cache_dir, cache_key + ".json")
  cache_subdir = os.path.join(cache_dir, cache_key)
  cache_file_copy = os.path.join(cache_subdir, cache_key + ".json")
  os.makedirs(cache_subdir, exist_ok=True)

  _return = {"file": cache_file}

  if diffs == True:
    if os.path.exists(cache_file):
      try:
        now = util.read(cache_file, logger=logger)
      except Exception as e:
        logger.error(f"Error reading {cache_file}: {e}")
        return {"file": None, "file_last": cache_file_copy, "diff": None}

    if os.path.exists(cache_file):
      try:
        last = util.read(cache_file_copy, logger=logger)
      except Exception as e:
        logger.error(f"Error reading {cache_file_copy}: {e}")
        return {"file": cache_file, "file_last": None, "diff": None}

      diff = deepdiff.DeepDiff(last, now)
      _return['cache_copy'] = cache_file_copy
      _return['diff'] = diff
      cache_diff_file = os.path.join(cache_subdir, cache_key + ".diff.json")
      util.write(cache_diff_file, diff.to_json(), logger=logger)
      shutil.copyfile(cache_file, cache_file_copy)

  return _return

def fetch(url, id, what, headers=None, timeout=20, diffs=False, update=False):

  cache_dir = os.path.join(DATA_DIR, 'cache', what)
  file_out_json = os.path.join(DATA_DIR, what, f"{id}.json")
  file_out_pkl = os.path.join(DATA_DIR, what, f"{id}.pkl")

  if update == False and os.path.exists(file_out_pkl):
    return util.read(file_out_pkl, logger=logger)

  session = CachedSession(cache_dir)

  if logger is not None:
    logger.info('Get: ' + url)

  _master = {}
  try:
    resp = session.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    if resp.headers['Content-Type'] == 'text/xml':
      text = resp.text
      json_dict = xmltodict.parse(text);
    else:
      json_dict = resp.json()
  except Exception as e:
    msg = f"Error[{what}]: {id}: {e}"
    if logger is not None:
      logger.error(msg)
    _master['error'] = msg
    return _master

  cache = cache_info(cache_dir, resp.cache_key, diffs=diffs)

  _master['id'] = id
  _master['request-log'] = print_request_log(resp, url, cache)
  _master['request-cache'] = rel_path(DATA_DIR, cache['file'])
  _master['url'] = url
  _master['data'] = json_dict
  _master['data-cache'] = rel_path(DATA_DIR, file_out_json)

  try:
    util.write(file_out_json, _master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_json}: {e}")

  try:
    util.write(file_out_pkl, _master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_pkl}: {e}")

  return _master

def datasets(timeout=20, update=False):

  json_dict = fetch(allurl, 'all', 'all', timeout=timeout, update=update)
  _datasets = {}
  for dataset_allxml in json_dict['data']['sites']['datasite'][0]['dataset']:

    id = dataset_allxml['@serviceprovider_ID']

    dataset = {'id': id, 'allxml': dataset_allxml}

    if not 'mastercdf' in dataset_allxml:
      msg = f'Error[all.xml]: {id}: No mastercdf node in all.xml'
      logger.error(msg)
      continue

    if isinstance(dataset_allxml['mastercdf'], list):
      msg = f'Warning[all.xml]: Not implemented: {id}: More than one mastercdf referenced in all.xml mastercdf node.'
      logger.info(msg)
      continue

    if not '@ID' in dataset_allxml['mastercdf']:
      msg = f'Error[all.xml]: {id}: No ID attribute in all.xml mastercdf node'
      logger.error(msg)
      continue

    _datasets[id] = dataset

  return _datasets

def master(dataset, timeout=20, update=False, diffs=False):

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  return fetch(url, dataset['id'], 'master', timeout=timeout, update=update, diffs=diffs)

def spase(_master, timeout=20, update=True, diffs=False):

  id = _master['id']
  if 'data' not in _master:
    return None

  files = list(_master['data'].keys())

  global_attributes = _master['data'][files[0]]['CDFglobalAttributes']
  for attribute in global_attributes:
    if 'spase_DatasetResourceID' in attribute:
      spase_id = attribute['spase_DatasetResourceID'][0]['0']
      if spase_id and not spase_id.startswith('spase://'):
        msg = f"Error[master]: {_master['id']}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://'"
        logger.error(msg)
        return None
      url = spase_id.replace('spase://', 'https://hpde.io/') + '.json';

  return fetch(url, id, 'spase', timeout=timeout, diffs=diffs, update=update)

def orig_data(dataset, timeout=120, update=True, diffs=False):

  start = dataset['allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  stop = dataset['allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  #headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
  headers = {'Accept': 'application/json'}
  return fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, diffs=diffs)

def samples(id, _orig_data):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  last_file = _orig_data['data']['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  _samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return _samples
