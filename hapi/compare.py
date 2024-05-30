import os
import copy
import time
import json
import datetime
from urllib.parse import urlparse

import requests
import requests_cache
import urllib3
import deepdiff

from hapiclient import hapitime2datetime

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('--include', help="Pattern for dataset IDs to include, e.g., '^A|^B' (default: .*)")
args = parser.parse_args()

base_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

opts = {'compare_data': False, 'parallel': False, 'base_dir': base_dir}

if False:
  opts = {**opts,
          'keep': r'^a',
          "sample_duration": {"days": 1},

          "s1": "chunk",
          "url1": "https://hapi-server.org/servers/SSCWeb/hapi",
          "s1_expire_after": {"days": 1},

          "s2": "chunk-ltfloats-parallel",
          "url2": "http://localhost:8999/SSCWeb/hapi",
          "s2_expire_after": {"days": 1}
        }

if False:
  opts = {**opts,
          "s1": "jf",
          "s2": "nl",
          "url1": "https://cottagesystems.com/server/cdaweb/hapi",
          "url2": "https://cdaweb.gsfc.nasa.gov/hapi",
          "s1_expire_after": {"days": 1},
          "s2_expire_after": {"days": 1},
          "sample_duration": {"days": 1}
        }

if True:
  opts = {**opts,
          "s1": "bw",
          "s2": "nl",
          "url1": "",
          "url2": "https://cdaweb.gsfc.nasa.gov/hapi",
          "s1_expire_after": None,
          "s2_expire_after": {"days": 1},
          "sample_duration": {"days": 1}
        }

def omit(id):
  # TODO: This is a copy of function in cdaweb.py. Move to a common module.
  import re
  if id == 'AIM_CIPS_SCI_3A':
    return True
  if args.include:
    if re.search(args.include, id):
      return False
    return True
  else:
    return False

def compare_metadata(datasets_s1, datasets_s2, opts):

  for dsid in datasets_s2.keys():
    if not dsid in datasets_s1:
      report(f"{dsid} not in {opts['s1']}",'fail')
      dsid0 = dsid + "@0"
      if dsid[-2] != "@" and dsid0 in list(datasets_s1.keys()):
        report(f"  But {dsid0} in {opts['s1']}",'info')

  for dsid in datasets_s1.keys():

    report(f"{dsid} - Checking metadata")

    extra = ""
    if "x_cdf_depend_0_name" in datasets_s1[dsid]["info"]["parameters"][0]:
      # Special case for when s1 = 'bw'
      x_cdf_depend_0_name = datasets_s1[dsid]["info"]["parameters"][0]["x_cdf_depend_0_name"]
      extra = f'for s1 DEPEND_0 = {x_cdf_depend_0_name}'

    if not dsid in datasets_s2:

      report(f"{dsid} not in {opts['s2']} {extra}",'fail')
      dsid0 = dsid + "@0"
      if dsid[-2] != "@" and dsid0 in list(datasets_s2.keys()):
        report(f"  But {dsid0} in {opts['s2']}",'info')

    else:

      compare_info(dsid, datasets_s2[dsid]["info"], datasets_s1[dsid]["info"])

      keys_s2 = datasets_s2[dsid]["info"]["_parameters"].keys()
      keys_s1 = datasets_s1[dsid]["info"]["_parameters"].keys()

      n_params_s2 = len(keys_s2)
      n_params_s1 = len(keys_s1)

      if n_params_s2 != n_params_s1:
        m = min(n_params_s2, n_params_s1)
        if list(keys_s1)[0:m] != list(keys_s2)[0:m]:
          report(f"n_params_{opts['s2']} = {n_params_s2} != n_params_{opts['s1']} = {n_params_s1} {extra}",'fail')
          report(f"  Differences: {set(keys_s1) ^ set(keys_s2)}",'info')
          report(f"  Error because first {m} parameters are not identical.",'info')
        else:
          report(f"n_params_{opts['s2']} = {n_params_s2} != n_params_{opts['s1']} = {n_params_s1} {extra}",'warn')
          report(f"  Differences: {set(keys_s1) ^ set(keys_s2)}",'info')
          report(f"  Not error because first {m} parameters are identical.",'info')
          parameters = list(keys_s1)[0:m]
          compare_data(dsid, datasets_s1, datasets_s2, opts, parameters=parameters, datasets_s0=datasets_s0)
      else:
        if keys_s2 != keys_s1:
          report(f'Order differs {extra}','fail')
          report(f"  {opts['s2_padded']}: {list(keys_s2)}",'info')
          report(f"  {opts['s1_padded']}: {list(keys_s1)}",'info')
        else:
          for i in range(len(datasets_s2[dsid]["info"]["parameters"])):
            param_s2 = datasets_s2[dsid]["info"]["parameters"][i]
            param_s1 = datasets_s1[dsid]["info"]["parameters"][i]
            compare_parameter(dsid, param_s2, param_s1)

          compare_data(dsid, datasets_s1, datasets_s2, opts, datasets_s0=datasets_s0)

def compare_info(dsid, info_s2, info_s1):

  keys_s2 = list(info_s2.keys())
  keys_s1 = list(info_s1.keys())

  keys_s1 = remove_keys(keys_s1)
  keys_s2 = remove_keys(keys_s2)

  n_keys_s2 = len(keys_s2)
  n_keys_s1 = len(keys_s1)
  if n_keys_s2 != n_keys_s1:
    #print(f"{dsid}")
    report(f'n_keys_{opts["s2"]} = {n_keys_s2} != n_keys_{opts["s1"]} = {n_keys_s1}','fail')
    report(f"  Differences: {set(keys_s1) ^ set(keys_s2)}",'info')
  else:
    common_keys = set(keys_s2) & set(keys_s1)
    for key in common_keys:
      if info_s2[key] != info_s1[key]:
        #print(f"{dsid}/info/{key}")
        report(f'{key} val_{opts["s2"]} = {info_s2[key]} != val_{opts["s1"]} = {info_s1[key]}','fail')

def compare_parameter(dsid, param_s2, param_s1):

  param_s1_keys = sorted(list(param_s1.keys()))
  param_s2_keys = sorted(list(param_s2.keys()))

  for key in param_s1_keys.copy():
    if key.startswith("x_"):
      param_s1_keys.remove(key)

  n_param_keys_s1 = len(param_s1_keys)
  n_param_keys_s2 = len(param_s2_keys)
  if n_param_keys_s1 != n_param_keys_s2:
    if {'bins'} != set(param_s1_keys) ^ set(param_s2_keys):
      report(f"{dsid}/{param_s2['name']}",'info')
      report(f"  n_param_keys_{opts['s2']} = {n_param_keys_s2} != n_param_keys_{opts['s1']} = {n_param_keys_s1}",'fail')
      report(f"    Differences: {set(param_s1_keys) ^ set(param_s2_keys)}",'info')

  common_keys = set(param_s2_keys) & set(param_s1_keys)
  for key in common_keys:
    if key == 'bins':
      continue
    if param_s2[key] != param_s1[key]:
      if key == 'fill' and 'type' in param_s2 and 'type' in param_s1:
        a = (param_s2['type'] == 'int' or param_s2['type'] == 'double')
        b = (param_s1['type'] == 'int' or param_s1['type'] == 'double')
        if a and b:
          if float(param_s2[key]) != float(param_s1[key]):
            report(f"{param_s2['name']}/{key}",'info')
            report(f"val_{opts['s2']} = {param_s2[key]} != val_{opts['s1']} = {param_s1[key]}",'fail')
      elif key == 'size' and isinstance(param_s2[key],list) and isinstance(param_s1[key],list):
        if param_s2[key] != param_s1[key]:
          report(f"{param_s2['name']}/{key}",'info')
          report(f"val_{opts['s2']} = {param_s2[key]} != val_{opts['s1']} = {param_s1[key]}",'fail')
      elif type(param_s2[key]) != type(param_s1[key]):
        report(f"{param_s2['name']}/{key}",'info')
        report(f"  type_{opts['s2']} = {type(param_s2[key])} != type_{opts['s1']} = {type(param_s1[key])}",'fail')
      else:
        report(f"{param_s2['name']}/{key}",'info')
        report(f"val_{opts['s2']} = '{param_s2[key]}' != val_{opts['s1']} = '{param_s1[key]}'",'fail')

  compare_bins(param_s2, param_s1)

def compare_bins(params_s2, params_s1):

  name_s2 = params_s2["name"]
  name_s1 = params_s1["name"]
  if 'bins' in params_s2:
    if not 'bins' in params_s1:
      #report(f"{dsid}")
      report(f'{s2} has bins for {name_s2} but {s1} does not','fail')
  if 'bins' in params_s1:
    if not 'bins' in params_s2:
      #print(f"{dsid}")
      report(f'{s1} has bins for {name_s1} but {s2} does not','fail')
  if 'bins' in params_s1:
    if 'bins' in params_s2:
      n_bins_s2 = len(params_s1["bins"])
      n_bins_s1 = len(params_s2["bins"])
      if n_bins_s2 != n_bins_s1:
        #print(f"{dsid}")
        report(f'{s1} has {n_bins_s1} bins objects; {s2} has {n_bins_s2}','fail')
      # TODO: Compare content at bins level

def compare_data(dsid, datasets_s1, datasets_s2, opts, parameters="", datasets_s0=None):

  if opts['compare_data'] is False:
    return
  if datasets_s0 is not None and not dsid in datasets_s0:
    return
  if not dsid in datasets_s1:
    return
  if not dsid in datasets_s2:
    return

  sampleStartDate = None
  sampleStopDate = None

  if 'sampleStartDate' in datasets_s2[dsid]['info']:
    sampleStartDate = datasets_s2[dsid]['info']['sampleStartDate']
  if 'sampleStartDate' in datasets_s1[dsid]['info']:
    sampleStartDate = datasets_s1[dsid]['info']['sampleStartDate']

  if 'sampleStopDate' in datasets_s2[dsid]['info']:
    sampleStopDate = datasets_s2[dsid]['info']['sampleStopDate']
  if 'sampleStartDate' in datasets_s1[dsid]['info']:
    sampleStopDate = datasets_s1[dsid]['info']['sampleStopDate']

  if datasets_s0 is not None and 'sampleStartDate' in datasets_s0[dsid]['info']:
    sampleStartDate = datasets_s0[dsid]['info']['sampleStartDate']
    sampleStopDate = datasets_s0[dsid]['info']['sampleStopDate']

  if sampleStartDate is None or sampleStopDate is None:
    startDate_s1 = hapitime2datetime(datasets_s1[dsid]['info']['startDate'])[0]
    startDate_s2 = hapitime2datetime(datasets_s2[dsid]['info']['startDate'])[0]
    if startDate_s1 != startDate_s2:
      sampleStartDate = max(startDate_s1, startDate_s2)
    else:
      sampleStartDate = startDate_s1

    sampleStopDate = sampleStartDate + datetime.timedelta(**opts['sample_duration'])

    sampleStartDate = sampleStartDate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    sampleStopDate = sampleStopDate.strftime('%Y-%m-%dT%H:%M:%S.%fZ')

  #print(f"  sampleStartDate = {sampleStartDate}")
  #print(f"  sampleStopDate = {sampleStopDate}")

  times = 2*[None]
  resps = 2*[None]

  urlo = "/data?id=" + dsid \
        + "&parameters=" + ",".join(parameters) \
        + "&time.min=" + sampleStartDate \
        + "&time.max=" + sampleStopDate

  urls = [opts['url1'] + urlo, opts['url2'] + urlo]

  def get(i):
    start = time.time()
    report(urls[i],'info')
    #resps[i] = requests.get(urls[i])
    resps[i] = requests.get(urls[i], verify=False)
    times[i] = time.time() - start

  report(f"{dsid} - Checking data")

  if opts['parallel'] is False:
    get(0)
    get(1)
  else:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
      pool.map(get, range(2))

  dt1 = "{0:.6f}".format(times[0])
  dt2 = "{0:.6f}".format(times[1])
  report(f"{opts['s1_padded']} time = {dt1} [s]; status = {resps[0].status_code}",'info')
  report(f"{opts['s2_padded']} time = {dt2} [s]; status = {resps[1].status_code}",'info')

  if resps[0].status_code != resps[1].status_code:
    report(f"{opts['s2']} HTTP status = {resps[1].status_code} != {opts['s1']} HTTP status = {resps[0].status_code}",'fail')
  elif resps[0].text != resps[1].text:
    report(f"{opts['s2']} data (length = {len(resps[1].text)}) != {opts['s1']} data (length = {len(resps[0].text)})",'fail')


def remove_keys(keys):
  for key in keys.copy():
    if opts['s1'] == 'bw' and key in ['sampleStartDate', 'sampleStopDate']:
      keys.remove(key)
    if key.startswith("x_"):
      keys.remove(key)
    if key in ['_parameters', 'parameters']:
      keys.remove(key)
  return keys

def get_all_metadata(server_url, server_name, expire_after={"days": 1}):

  def server_dir(url):
    url_parts = urlparse(url)
    url_dir = os.path.join(opts['base_dir'], 'compare', url_parts.netloc, *url_parts.path.split('/'))
    os.makedirs(url_dir, exist_ok=True)
    return url_dir

  cache_dir = server_dir(server_url)
  out_file = os.path.join(cache_dir, f'hapi-{server_name}.json')
  report(f"\n{server_name} = {server_url}")
  report(f"cache_dir = {cache_dir}")

  if False:
    print(f"Reading: {out_file}")
    with open(out_file, 'r', encoding='utf-8') as f:
      datasets = json.load(f)
    print(f"Read: {out_file}")
    return datasets

  os.makedirs(os.path.dirname(out_file), exist_ok=True)

  def CachedSession():
    # https://requests-cache.readthedocs.io/en/stable/#settings
    # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html
    copts = {
      "cache_control": True,                # Use Cache-Control response headers for expiration, if available
      "expire_after": datetime.timedelta(**expire_after), # Otherwise expire after one day
      "allowable_codes": [200],             # Cache responses with these status codes
      "stale_if_error": False,              # In case of request errors, use stale cache data if possible
      "backend": "filesystem"
    }
    return requests_cache.CachedSession(cache_dir, **copts)

  session = CachedSession()

  urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
  resp = session.request('get', server_url + '/catalog', verify=False)
  datasets = resp.json()['catalog']

  for dataset in datasets:

    id = dataset['id']
    if omit(id):
      continue

    url = server_url + '/info?id=' + id

    start = time.time()
    report(f'Getting: {url}', msg_type='info')
    resp = session.request('get', url, verify=False)
    if resp.from_cache:
      report(f'Got: (from cache) {url}', msg_type='info')
    else:
      dt = "{0:.6f}".format(time.time() - start)
      report(f'Got: (time = {dt} [s]) {url}', msg_type='info')

    if resp.status_code != 200:
      continue

    dataset['info'] = resp.json()
    del dataset['info']['status']
    del dataset['info']['HAPI']

  with open(out_file, 'w', encoding='utf-8') as f:
    json.dump(datasets, f, indent=2)
  #print(f'Wrote: {out_file}')

  return datasets

def restructure(datasets):
  """Create _parameters dict with keys of parameter name."""
  datasetsr = {}
  for dataset in datasets:
    id = dataset["id"]
    if omit(id):
      continue
    datasetsr[id] = copy.deepcopy(dataset)
    datasetsr[id]["info"]["_parameters"] = {}
    for parameter in dataset["info"]["parameters"]:
      name = parameter["name"]
      datasetsr[id]["info"]["_parameters"][name] = parameter
  return datasetsr

def read_hapi_bw():

  # Get sample starts/stops from s0
  all_input = os.path.join(opts['base_dir'], 'hapi', f'hapi-new.json')

  print(f"Reading: {all_input}")
  with open(all_input, 'r', encoding='utf-8') as f:
    datasets = json.load(f)
  print(f"Read: {all_input}")

  return datasets

def report(msg, msg_type=None):

  prefix = ""
  if msg_type == 'pass':
    prefix = "  ✓ "
  if msg_type == 'fail':
    prefix = "  ✗ "
  if msg_type == 'warn':
    prefix = "  ⚠ "
  if msg_type == 'info':
    prefix = "  "

  print(prefix + msg)

def pad_server_name(opts):
  l1 = len(opts['s1'])
  l2 = len(opts['s2'])
  opts['s2_padded'] = opts['s2']
  opts['s1_padded'] = opts['s1']
  if l1 > l2:
    opts['s2_padded'] = opts['s2'] + ' '*(l1-l2)
  if l2 > l1:
    opts['s1_padded'] = opts['s1'] + ' '*(l2-l1)

  return opts

opts = pad_server_name(opts)

datasets_s0 = None
if opts['s1'] == 'jf' and opts['s2'] == 'nl' and opts['compare_data'] is True:
  # datasets_s0 has sample{Start,Stop} for all datasets. Needed for comparing data.
  datasets_s0  = restructure(read_hapi_bw())

if opts['s1'] == 'bw':
  datasets_s1o = read_hapi_bw()
else:
  datasets_s1o = get_all_metadata(opts['url1'], opts['s1'], expire_after=opts['s1_expire_after'])

datasets_s2o = get_all_metadata(opts['url2'], opts['s2'], expire_after=opts['s2_expire_after'])

report("")

if {} == deepdiff.DeepDiff(datasets_s1o, datasets_s2o):
  report("All /info metadata is the same.\n")
  if opts['compare_data'] is False:
    report("Not checking data responses.")
    exit(0)

datasets_s1 = restructure(datasets_s1o)
datasets_s2 = restructure(datasets_s2o)

compare_metadata(datasets_s1, datasets_s2, opts)
