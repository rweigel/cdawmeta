# Usage: python cdaweb.py
#
# Creates data/main.json, which is an object with keys of dataset id, each with
# keys of _all_xml, _master, _spase, and _file_list. _all_xml contains the
# content of the dataset node in all.xml as JSON. The values of the other keys
# are paths to the cached JSON files.

import os
import json

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--include', help="Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)")
args = parser.parse_args()

max_workers = 4
cache_control = True # Use Cache-Control response headers for expiration, if available
expire_after = None  # Set to 0 to force re-download

allxml = 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
filews = 'https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/'

cache_dir = os.path.join(os.path.dirname(__file__), 'data', 'cache')
out_file = os.path.join(os.path.dirname(__file__), 'data', 'cdaweb.json')

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

def get(function, datasets, cache_dir):

  session = CachedSession(cache_dir)

  if max_workers == 1:
    for dataset in datasets:
      function(dataset, session, cache_dir)
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(dataset):
      function(dataset, session, cache_dir)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call, datasets)

def CachedSession(cache_dir):
  import requests_cache
  # https://requests-cache.readthedocs.io/en/stable/#settings
  # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html

  # Cache dir
  copts = {
    "use_cache_dir": True,          # Save files in the default user cache dir
    "cache_control": True,          # Use Cache-Control response headers for expiration, if available
    "expire_after": expire_after,   # Expire responses after expire_after if no cache control header
    "allowable_codes": [200],       # Cache responses with these status codes
    "stale_if_error": True,         # In case of request errors, use stale cache data if possible
    "serializer": "json",
    "backend": "filesystem",        # This causes caching to not work unless decode_content = False
    "decode_content": True          # https://github.com/requests-cache/requests-cache/issues/963
  }

  return requests_cache.CachedSession(cache_dir, **copts)

def print_resp_info(resp, url):
  print(f'Got: {url}')
  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  print(f"  Status Code: {resp.status_code}")
  print(f"  From cache:  {resp.from_cache}")
  print(f"  Request Cache-Related Headers:")
  for k, v in req_cache_headers.items():
    print(f"    {k}: {v}")
  print(f"  Response Cache-Related Headers:")
  for k, v in res_cache_headers.items():
    print(f"    {k}: {v}")

def create_datasets(cache_dir):

  """
  Create a list of datasets; each element has content of dataset node in
  all.xml. An info and id node is also added.
  """
  cache_dir = os.path.join(cache_dir, 'all')
  session = CachedSession(cache_dir)
  url = allxml

  print("Getting: " + url)
  try:
    resp = session.get(url)
  except:
    print("Error getting " + url)
    exit(1)
  print_resp_info(resp, url)

  allxml_text = resp.text

  import xmltodict
  all_dict = xmltodict.parse(allxml_text);

  datasets = []
  for dataset_allxml in all_dict['sites']['datasite'][0]['dataset']:

    id = dataset_allxml['@serviceprovider_ID']

    if omit(id):
      continue

    dataset = {'id': id, '_allxml': dataset_allxml}

    if not 'mastercdf' in dataset_allxml:
      print('No mastercdf for ' + id)
      continue

    if not '@ID' in dataset_allxml['mastercdf']:
      print('No ID attribute for mastercdf for ' + id)
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

    print('Get: ' + url)
    try:
      resp = session.get(url)
    except:
      print("Error getting " + url)
      exit(1)
    print_resp_info(resp, url)

    dataset['_master_data'] = resp.json()

    dataset['_master'] = cache_dir + "/" + resp.cache_key + ".json"

    files = list(dataset['_master_data'].keys())
    if len(files) > 1:
      print("Aborting. Expected only one file key in _master object for " + dataset['id'])
      exit(1)

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
      if len(files) > 1:
        print("Expected only one file key in _master object.")
        exit(1)

      global_attributes = dataset['_master_data'][files[0]]['CDFglobalAttributes']
      for attribute in global_attributes:
        if 'spase_DatasetResourceID' in attribute:
          id = attribute['spase_DatasetResourceID'][0]['0']
          return id.replace('spase://', 'https://hpde.io/') + '.json';

    url = spase_url(dataset)
    del dataset['_master_data']

    if url is None:
      dataset['_spase'] = None
      return

    print('Got: ' + url)
    try:
      resp = session.get(url)
    except:
      print(f"{dataset['id']}: Error getting '{url}'")
      return
    if resp.status_code != 200:
      print(f"{dataset['id']}: HTTP status != 200 ({resp.status_code}) when getting '{url}'")
      return
    print_resp_info(resp, url)

    dataset['_spase'] = cache_dir + "/" + resp.cache_key + ".json"

  cache_dir = os.path.join(cache_dir, 'spase')
  get(get_spase, datasets, cache_dir)

def add_file_list(datasets, cache_dir):

  def get_file_list(dataset, session, cache_dir):

    start = dataset['_allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z";
    stop = dataset['_allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z";
    url = filews + dataset["id"] + "/orig_data/" + start + "," + stop

    print("Get: " + url)
    try:
      resp = session.get(url, timeout=10, headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
    except:
      print(f"{dataset['id']}: Error getting '{url}'")
      return
    if resp.status_code != 200:
      print(f"HTTP status code {resp.status_code} when getting '{url}'")
      return
    print_resp_info(resp, url)

    dataset['_file_list'] = cache_dir + "/" + resp.cache_key + ".json"

  cache_dir = os.path.join(os.path.dirname(__file__), 'data/cache/files')
  get(get_file_list, datasets, cache_dir)

datasets = create_datasets(cache_dir)
add_master(datasets, cache_dir)
add_spase(datasets, cache_dir)
add_file_list(datasets, cache_dir)

print(f'# of datasets: {len(datasets)}')

from cdawmeta.write_json import write_json
write_json(datasets, out_file)
