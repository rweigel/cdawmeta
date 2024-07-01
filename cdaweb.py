# Usage: python cdaweb.py
#
# Creates data/main.json, which is an object with keys of dataset id, each with
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

def print_resp_info(resp, url, cache):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = ""
  msg += f'\nGot: {url}\n'
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
  logger.info(msg)

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

def create_datasets(cache_dir):

  """
  Create a list of datasets; each element has content of dataset node in
  all.xml. An info and id node is also added.
  """
  cache_dir = os.path.join(cache_dir, 'all')
  session = CachedSession(cache_dir)
  url = allxml

  logger.info("Get: " + url)
  try:
    resp = session.get(url, timeout=timeouts['allxml'])
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

  create_datasets.n = len(all_dict['sites']['datasite'][0]['dataset'])
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

def add_master(datasets, cache_dir):

  """
  Add a _master key to each dataset containing JSON from master CDF
  """

  def get_master(dataset, session, cache_dir):

    mastercdf = dataset['_allxml']['mastercdf']['@ID']
    url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

    logger.info('Get: ' + url)
    try:
      resp = session.get(url, timeout=timeouts['master'])
      resp.raise_for_status()
      json_dict = resp.json()
    except Exception as e:
      msg = f"Error[master]: {dataset['id']}: {e}"
      logger.error(msg)
      dataset['_master_data'] = None
      return

    cache= cache_info(cache_dir, resp.cache_key)
    print_resp_info(resp, url, cache)

    dataset['_master'] = rel_path(cache['file'])
    dataset['_master_urls'] = {
      'cdf': mastercdf,
      'json': url,
      'skt': mastercdf.replace('.cdf', '.skt').replace('0MASTERS', '0SKELTABLES')
    }
    dataset['_master_data'] = json_dict

    files = list(dataset['_master_data'].keys())
    if len(files) > 1:
      msg = f"Error[master]: {dataset['id']}: More than one file key in master CDF JSON"
      logger.error(msg)
      dataset['_master_data'] = None
      return

  cache_dir = os.path.join(cache_dir, 'masters')
  get(get_master, datasets, cache_dir)

def add_spase(datasets, cache_dir):

  """
  Add a _spase key to each dataset containing JSON from master CDF
  """

  def get_spase(dataset, session, cache_dir):

    def spase_url(dataset):

      if '_master_data' not in dataset:
        return None

      files = list(dataset['_master_data'].keys())

      global_attributes = dataset['_master_data'][files[0]]['CDFglobalAttributes']
      for attribute in global_attributes:
        if 'spase_DatasetResourceID' in attribute:
          spase_id = attribute['spase_DatasetResourceID'][0]['0']
          if spase_id and not spase_id.startswith('spase://'):
            msg = f"Error[master]: {dataset['id']}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://'"
            logger.error(msg)
            return None
          return spase_id.replace('spase://', 'https://hpde.io/') + '.json';

    url = spase_url(dataset)
    del dataset['_master_data']

    if url is None:
      dataset['_spase'] = None
      return

    logger.info('Get: ' + url)
    try:
      resp = session.get(url, timeout=timeouts['spase'])
      resp.raise_for_status()
    except Exception as e:
      msg = f"Error[master/spase]: {dataset['id']}: {e}"
      logger.error(msg)
      return

    cache = cache_info(cache_dir, resp.cache_key)
    print_resp_info(resp, url, cache)

    dataset['_spase_url'] = url
    dataset['_spase'] = rel_path(cache['file'])

  cache_dir = os.path.join(cache_dir, 'spase')
  get(get_spase, datasets, cache_dir)

def add_file_list(datasets, cache_dir):

  def get_file_list(dataset, session, cache_dir):

    start = dataset['_allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z";
    stop = dataset['_allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z";
    url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

    logger.info("Get: " + url)
    try:
      resp = session.get(url, timeout=timeouts['file_list'], headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
      resp.raise_for_status()
      json_dict = resp.json()
    except Exception as e:
      msg = f"Error[orig_data]: {dataset['id']}: {e}"
      logger.error(msg)
      return

    if not 'FileDescription' in json_dict:
      msg = f"Error[orig_data]: {dataset['id']}: No FileDescription key"
      logger.error(msg)

    if len(json_dict['FileDescription']) == 0:
      msg = f"Error[orig_data]: {dataset['id']}: FileDescription array is empty"
      logger.error(msg)

    cache = cache_info(cache_dir, resp.cache_key)
    print_resp_info(resp, url, cache)

    dataset['_samples'] = add_samples(dataset['id'], json_dict)
    dataset['_file_list_url'] = url
    dataset['_file_list'] = rel_path(cache['file'])

  cache_dir = os.path.join(script_dir, 'data/cache/files')
  get(get_file_list, datasets, cache_dir)

def add_samples(id, file_list):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  last_file = file_list['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  _samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}/datasets/{id}/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}/datasets/{id}/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return _samples

args = cli()

if args.include is None:
  partial_ext = ''
  partial_dir = ''
else:
  partial_ext = '.partial'
  partial_dir = 'partial'

script_dir = os.path.dirname(__file__)
base_name = f'cdaweb{partial_ext}'
cache_root = os.path.join(script_dir, 'data', 'cache')
file_out = os.path.join(script_dir, 'data', partial_dir, f'{base_name}.json')
log_config = {
  'file_log': os.path.join(script_dir, 'data', partial_dir, f'{base_name}.log'),
  'file_error': os.path.join(script_dir, 'data', partial_dir, f'{base_name}.errors.log'),
  'format': '%(message)s',
  'rm_string': script_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

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
