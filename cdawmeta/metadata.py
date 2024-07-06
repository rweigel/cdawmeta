import os
import re
import json
import shutil
import xmltodict
import deepdiff

import cdawmeta

wsbase = 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/'

def rel_path(base_dir, path):
  return path.replace(base_dir + '/', '')

def CachedSession(cache_dir, expire_after=0):
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
    "expire_after": expire_after,

    # Cache responses with these status codes
    "allowable_codes": [200],

    # In case of request errors, use stale cache data if possible
    "stale_if_error": True,

    "serializer": "json",

    # This causes caching to not work unless decode_content = False
    "backend": "filesystem",

    # https://github.com/requests-cache/requests-cache/issues/963
    "decode_content": True
  }

  return requests_cache.CachedSession(cache_dir, **copts)

def print_resp_info(resp, url, cache, data_dir=None, logger=None):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = ""
  msg += f'Got: {url}\n'
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  msg += f"  Current cache file: {rel_path(data_dir, cache['file'])}\n"
  if 'file_last' in cache:
    msg += f"  Last cache file:    {rel_path(data_dir, cache['file_last'])}\n"
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

def cache_info(cache_dir, cache_key, diffs=False, logger=None):

  cache_file = os.path.join(cache_dir, cache_key + ".json")
  cache_subdir = os.path.join(cache_dir, cache_key)
  cache_file_copy = os.path.join(cache_subdir, cache_key + ".json")
  os.makedirs(cache_subdir, exist_ok=True)

  _return = {"file": cache_file}

  if diffs == True:
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

def fetch(url, id, what, headers=None, timeout=20, expire_after=0, diffs=False, update=False, data_dir=None, logger=None):

  cache_dir = os.path.join(data_dir, 'cache', what)
  file_out_json = os.path.join(data_dir, what, f"{id}.json")
  file_out_pkl = os.path.join(data_dir, what, f"{id}.pkl")

  if update == False and os.path.exists(file_out_pkl):
    return cdawmeta.util.read(file_out_pkl, logger=logger)

  session = CachedSession(cache_dir, expire_after=expire_after)

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

  cache = cache_info(cache_dir, resp.cache_key, diffs=diffs, logger=logger)

  _master['id'] = id
  _master['info'] = print_resp_info(resp, url, cache, data_dir=data_dir, logger=logger)
  _master['cache'] = rel_path(data_dir, cache['file'])
  _master['url'] = url
  _master['data'] = json_dict

  try:
    cdawmeta.util.write(file_out_json, _master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_json}: {e}")

  try:
    cdawmeta.util.write(file_out_pkl, _master, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out_pkl}: {e}")

  return _master

def datasets(timeout=20, update=False, expire_after=0, data_dir=None, logger=None):

  url = 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
  json_dict = fetch(url, 'all', 'all', timeout=timeout, expire_after=expire_after, update=update, data_dir=data_dir, logger=logger)
  _datasets = {}
  for dataset_allxml in json_dict['data']['sites']['datasite'][0]['dataset']:

    id = dataset_allxml['@serviceprovider_ID']

    dataset = {'id': id, '_allxml': dataset_allxml}

    if not 'mastercdf' in dataset_allxml:
      msg = f'Error[all.xml]: {id}: No mastercdf node in all.xml'
      logger.error(msg)
      continue

    if isinstance(dataset_allxml['mastercdf'], list):
      msg = f'Warning[all.xml]: Not implemented: {id}: More than one mastercdf referenced in all.xml mastercdf node.'
      logger.error(msg)
      continue

    if not '@ID' in dataset_allxml['mastercdf']:
      msg = f'Error[all.xml]: {id}: No ID attribute in all.xml mastercdf node'
      logger.error(msg)
      continue

    _datasets[id] = dataset

  return _datasets

def master(dataset, timeout=20, update=False, expire_after=0, diffs=False, data_dir=None, logger=None):

  mastercdf = dataset['_allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  return fetch(url, dataset['id'], 'master', timeout=timeout, expire_after=expire_after, update=update, diffs=diffs, data_dir=data_dir, logger=logger)

def spase(_master, timeout=20, update=True, expire_after=0, diffs=False, data_dir=None, logger=None):

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

  return fetch(url, id, 'spase', timeout=timeout, expire_after=expire_after, diffs=diffs, update=update, data_dir=data_dir, logger=logger)

def orig_data(dataset, timeout=120, update=True, expire_after=0, diffs=False, data_dir=None, logger=None):

  start = dataset['_allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  stop = dataset['_allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

  return fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, expire_after=expire_after, diffs=diffs, data_dir=data_dir, logger=logger)

def samples(_orig_data):

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

def metadata(id=None, update=True, expire_after=0, max_workers=1, diffs=False, data_dir=None, logger=None, timeouts=None, no_spase=True, no_orig_data=True):

  kwargs = {
    'update': update,
    'expire_after': expire_after,
    'data_dir': data_dir,
    'logger': logger
  }

  datasets_ = datasets(timeout=timeouts['allxml'], **kwargs)

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
    dataset['master'] = master(dataset, diffs=diffs, timeout=timeouts['master'], **kwargs)
    if no_orig_data == False:
      dataset['orig_data'] = orig_data(dataset, diffs=diffs, timeout=timeouts['orig_data'], **kwargs)
      dataset['samples'] = samples(dataset['orig_data'])
    if no_spase == False:
      dataset['spase'] = spase(dataset['master'], diffs=diffs, timeout=timeouts['spase'], **kwargs)

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
