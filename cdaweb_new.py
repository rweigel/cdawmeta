# Usage: python cdaweb.py --help
#
# Creates data/cdaweb.json, which is an object with keys of dataset id, each with
# keys of _all_xml, _master, _spase, and _file_list. _all_xml contains the
# content of the dataset node in all.xml as JSON. The values of the other keys
# are paths to the cached JSON files.

import os
import json
import shutil
import xmltodict
import deepdiff

import cdawmeta

allxml = 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
wsbase = 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/'

timeouts = {
  'allxml': 30,
  'master': 30,
  'spase': 30,
  'file_list': 120
}

def omit(id):
  import re
  if id == 'AIM_CIPS_SCI_3A':
    return True
  if args.include:
    if re.search(args.include, id):
      return False
    return True
  else:
    return False

def rel_path(path):
  return path.replace(script_dir + '/', '')

def get(function, datasets, cache_dir):

  session = CachedSession(cache_dir)

  if args.max_workers == 1:
    for dataset in datasets:
      function(dataset, session, cache_dir)
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(dataset):
      try:
        function(dataset, session, cache_dir)
      except Exception as e:
        import traceback
        logger.error(f"Error: {dataset['id']}: {traceback.print_exc()}")
    with ThreadPoolExecutor(max_workers=args.max_workers) as pool:
      pool.map(call, datasets)

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
    "expire_after": args.expire_after,

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

def print_resp_info(resp, url, cache, logger=None):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = ""
  msg += f'Got: {url}\n'
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  msg += f"  Current cache file: {rel_path(cache['file'])}\n"
  if 'file_last' in cache:
    msg += f"  Last cache file:    {rel_path(cache['file_last'])}\n"
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

def cache_info(cache_dir, cache_key):

  cache_file = os.path.join(cache_dir, cache_key + ".json")
  cache_subdir = os.path.join(cache_dir, cache_key)
  cache_file_copy = os.path.join(cache_subdir, cache_key + ".json")
  os.makedirs(cache_subdir, exist_ok=True)

  _return = {"file": cache_file}

  if args.diffs == True:
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

def datasets(data_dir, logger):
  """
  Create a list of datasets; each element has content of dataset node in
  all.xml. An info and id node is also added.
  """
  cache_dir = os.path.join(data_dir, 'cache', 'all')
  session = CachedSession(cache_dir)
  url = allxml

  logger.info("Get: " + url)
  try:
    resp = session.get(url, timeout=timeouts['allxml'])
    logger.info("Got: " + url)
  except:
    logger.error(f"Error getting {url}. Cannot continue")
    exit(1)

  try:
    allxml_text = resp.text
    all_dict = xmltodict.parse(allxml_text);
  except:
    logger.error(f"Error parsing {url}. Cannot continue")
    exit(1)

  cache = cache_info(cache_dir, resp.cache_key)
  print_resp_info(resp, url, cache)

  datasets = []
  for dataset_allxml in all_dict['sites']['datasite'][0]['dataset']:

    id = dataset_allxml['@serviceprovider_ID']

    if omit(id):
      continue

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

    datasets.append(dataset)

  return datasets

def fetch(url, id, what, headers=None, timeout=20, update=False, data_dir=None, logger=None):

  cache_dir = os.path.join(data_dir, 'cache', what)
  file_out_json = os.path.join(data_dir, what, f"{id}.json")
  file_out_pkl = os.path.join(data_dir, what, f"{id}.pkl")

  if update == False and os.path.exists(file_out_pkl):
    return cdawmeta.util.read(file_out_pkl, logger=logger)

  session = CachedSession(cache_dir)

  if logger is not None:
    logger.info('Get: ' + url)

  _master = {}
  try:
    resp = session.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    json_dict = resp.json()
  except Exception as e:
    msg = f"Error[{what}]: {id}: {e}"
    if logger is not None:
      logger.error(msg)
    _master['error'] = msg
    return _master

  files = list(json_dict.keys())
  if len(files) > 1:
    msg = f"Error[master]: {id}: More than one file key in master CDF JSON"
    logger.error(msg)
    _master['error'] = msg
    return _master

  cache = cache_info(cache_dir, resp.cache_key)

  _master['id'] = id
  _master['info'] = print_resp_info(resp, url, cache, logger=logger)
  _master['cache'] = rel_path(cache['file'])
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

def master(dataset, timeout=20, update=False, data_dir=None, logger=None):

  mastercdf = dataset['_allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  return fetch(url, dataset['id'], 'master', timeout=timeout, update=update, data_dir=data_dir, logger=logger)

def spase(_master, timeout=20, update=True, data_dir=None, logger=None):

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

  return fetch(url, id, 'spase', timeout=timeout, update=update, data_dir=data_dir, logger=logger)

def orig_data(dataset, timeout=20, update=True, data_dir=None, logger=None):

  start = dataset['_allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  stop = dataset['_allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z";
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

  return fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, data_dir=data_dir, logger=logger)

def add_samples(id, file_list):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  last_file = file_list['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  _samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return _samples

def metadata(id, update=True, data_dir=None, logger=None):
  datasets_ = datasets(data_dir, logger=logger)
  for dataset in datasets_:
    if dataset['id'] == id:
      _master = master(dataset, update=update, logger=logger, data_dir=data_dir)
      _spase = spase(_master, update=update, logger=logger, data_dir=data_dir)
      _orig_data = orig_data(dataset, update=update, logger=logger, data_dir=data_dir)
      return _orig_data
  return None

def ids(data_dir=None, logger=None):
  datasets_ = datasets(data_dir, logger=logger)
  return [dataset['id'] for dataset in datasets_]

if __name__ == '__main__':

  def cli():
    clkws = {
      "include": {
        "help": "Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)"
      },
      "max_workers": {
        "type": int,
        "help": "Number of threads to use for downloading",
        "default": 4
      },
      "expire_after": {
        "type": int,
        "help": "Expire cache after this many seconds",
        "default": 0
      },
      "no_file_list": {
        "action": "store_true",
        "help": "Exclude _file_list in catalog.json (_file_list, _sample_{file,url} are not created)",
        "default": False
      },
      "no_spase": {
        "action": "store_true",
        "help": "Exclude _spase in catalog.json",
        "default": False
      },
      "diffs": {
        "action": "store_true",
        "help": "Compute response diffs",
        "default": False
      }
    }

    import argparse
    parser = argparse.ArgumentParser()
    for k, v in clkws.items():
      parser.add_argument(f'--{k}', **v)
    return parser.parse_args()

  args = cli()

  if args.include is None:
    partial_ext = ''
    partial_dir = ''
  else:
    partial_ext = '.partial'
    partial_dir = 'partial'

  script_dir = os.path.dirname(__file__)
  base_name = f'cdaweb{partial_ext}'
  data_dir = os.path.join(script_dir, 'data')
  file_out = os.path.join(script_dir, 'data', partial_dir, f'{base_name}.json')
  log_config = {
    'file_log': os.path.join(script_dir, 'data', partial_dir, f'{base_name}.log'),
    'file_error': os.path.join(script_dir, 'data', partial_dir, f'{base_name}.errors.log'),
    'format': '%(message)s',
    'rm_string': script_dir + '/'
  }
  logger = cdawmeta.util.logger(**log_config)
  #ids(logger=logger, data_dir=data_dir)
  _metadata = metadata('AC_H2_MFI', update=True, logger=logger, data_dir=data_dir)
  print(json.dumps(_metadata, indent=2))
  exit()

  datasets = create_datasets(cache_root)
  add_master(datasets, cache_root)

  if args.no_spase is False:
    add_spase(datasets, cache_root)
  else:
    logger.info("Skipping SPASE b/c of command line --no_spase argument")

  if args.no_file_list is False:
    add_file_list(datasets, cache_root)
  else:
    logger.info("Skipping creation of file list b/c of command line --no_file_list argument")

  logger.info(f'# of datasets in all.xml: {create_datasets.n}')
  logger.info(f'# of datasets handled:    {len(datasets)}')

  try:
    cdawmeta.util.write(file_out, datasets, logger=logger)
  except Exception as e:
    logger.error(f"Error writing {file_out}: {e}")
    exit(1)

