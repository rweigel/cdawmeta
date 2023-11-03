import os
import copy
import time
import json
from datetime import datetime

import requests

data = False    # Compare data
serial = False  # If False, request dataset data from servers in parallel

s1 = 'jf'
s2 = 'nl'

url1 = "https://cottagesystems.com/server/cdaweb-nand/hapi"
url2 = "https://cdaweb.gsfc.nasa.gov/hapi"

base_dir = os.path.join(os.path.dirname(__file__), 'data')
all_input_s1 = os.path.join(base_dir, f'hapi-{s1}.json')
all_input_s2 = os.path.join(base_dir, f'hapi-{s2}.json')

def restructure(datasets):
  """Create _parameters dict with keys of parameter name."""
  datasetsr = {}
  for dataset in datasets:
    id = dataset["id"]
    datasetsr[id] = copy.deepcopy(dataset)
    datasetsr[id]["info"]["_parameters"] = {}
    for parameter in dataset["info"]["parameters"]:
      name = parameter["name"]
      datasetsr[id]["info"]["_parameters"][name] = parameter
  return datasetsr

def compare_data(dsid, datasets_s0=None):

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

  if datasets_s0 is not None:
    sampleStartDate = datasets_s0[dsid]['info']['sampleStartDate']
    sampleStopDate = datasets_s0[dsid]['info']['sampleStopDate']

  if not dsid.startswith("G"):
    return

  times = 2*[None]
  resps = 2*[None]

  urlo = "/data?id=" + dsid + "&parameters=&time.min=" + sampleStartDate + "&time.max=" + sampleStopDate
  urls = [url1 + urlo, url2 + urlo]

  def get(i):
    start = time.time()
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d %H:%M:%S")
    print("  " + dt_string)
    print("  " + urls[i])
    resps[i] = requests.get(urls[i])
    times[i] = time.time() - start

  print(dsid)

  if serial is True:
    get(0)
    get(1)
  else:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as pool:
      pool.map(get, range(2))

  print(f"  {s1} time = {times[0]}; status = {resps[0].status_code}")
  print(f"  {s2} time = {times[1]}; status = {resps[1].status_code}")

  if resps[0].status_code != resps[1].status_code:
    print(f"  {s2} HTTP status = {resps[1].status_code} != {s1} HTTP status = {resps[0].status_code}")

  if resps[0].text != resps[1].text:
    print(f"  {s2} data (length = {len(resps[1].text)}) != {s1} data (length = {len(resps[0].text)})")

def compare_param(param_s2, param_s1):

  param_s2_keys = sorted(list(param_s2.keys()))
  param_s1_keys = sorted(list(param_s1.keys()))

  for key in param_s1_keys.copy():
    if key.startswith("x_cdf_"):
      param_s1_keys.remove(key)

  n_param_keys_s2 = len(param_s2_keys)
  n_param_keys_s1 = len(param_s1_keys)
  if n_param_keys_s2 != n_param_keys_s1:
    if {'bins'} != set(param_s1_keys) ^ set(param_s2_keys):
      print(f"{dsid}/{param_s2['name']}")
      print(f'  n_param_keys_{s2} = {n_param_keys_s2} != n_param_keys_{s1} = {n_param_keys_s1} for s1 DEPEND_0  = {x_cdf_depend_0_name}')
      print(f"  Differences: {set(param_s1_keys) ^ set(param_s2_keys)}")

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
            print(f"{dsid}/{param_s2['name']}/{key}")
            print(f"  val_{s2} = {param_s2[key]} != val_{s1} = {param_s1[key]}")
      elif key == 'size' and isinstance(param_s2[key],list) and isinstance(param_s1[key],list):
        if param_s2[key] != param_s1[key]:
          print(f"{dsid}/{param_s2['name']}/{key}")
          print(f"  val_{s2} = {param_s2[key]} != val_{s1} = {param_s1[key]}")
      elif type(param_s2[key]) != type(param_s1[key]):
        print(f"{dsid}/{param_s2['name']}/{key}")
        print(f"  type_{s2} = {type(param_s2[key])} != type_{s1} = {type(param_s1[key])}")
      else:
        print(f"{dsid}/{param_s2['name']}/{key}")
        print(f"  val_{s2} = {param_s2[key]} != val_{s1} = {param_s1[key]}")

  compare_bins(param_s2, param_s1)

def compare_bins(params_s2, params_s1):

  name_s2 = params_s2["name"]
  name_s1 = params_s1["name"]
  if 'bins' in params_s2:
    if not 'bins' in params_s1:
      print(f"{dsid}")
      print(f'  {s2} has bins for {name_s2} but {s1} does not')
  if 'bins' in params_s1:
    if not 'bins' in params_s2:
      print(f"{dsid}")
      print(f'  {s1} has bins for {name_s1} but {s2} does not')
  if 'bins' in params_s1:
    if 'bins' in params_s2:
      n_bins_s2 = len(params_s1["bins"])
      n_bins_s1 = len(params_s2["bins"])
      if n_bins_s2 != n_bins_s1:
        print(f"{dsid}")
        print(f'  {s1} has {n_bins_s1} bins objects; {s2} has {n_bins_s2}')
      # TODO: Compare content at bins level

def compare_info(dsid, info_s2, info_s1):
    keys_s2 = list(info_s2.keys())
    keys_s1 = list(info_s1.keys())
    for ignore in ['_parameters', 'parameters', 'stopDate']:
      keys_s2.remove(ignore)
      keys_s1.remove(ignore)

    # Special case for s1. TODO: Create list of elements to ignore
    if 'sampleStartDate' in keys_s1:
      keys_s1.remove('sampleStartDate')
    if 'sampleStopDate' in keys_s1:
      keys_s1.remove('sampleStopDate')

    n_keys_s2 = len(keys_s2)
    n_keys_s1 = len(keys_s1)
    if n_keys_s2 != n_keys_s1:
      print(f"{dsid}")
      print(f'  n_keys_{s2} = {n_keys_s2} != n_keys_{s1} = {n_keys_s1}')
      print(f"  Differences: {set(keys_s1) ^ set(keys_s2)}")
    else:
      common_keys = set(keys_s2) & set(keys_s1)
      for key in common_keys:
        if info_s2[key] != info_s1[key]:
          print(f"{dsid}/info/{key}")
          print(f"  val_{s2} = {info_s2[key]} != val_{s1} = {info_s1[key]}")

if data == False and all_input_s1 == all_input_s2:
  exit()

datasets_s0 = None
if s1 == 'jf' and s2 == 'nl':
  # Get sample starts/stops from s0
  all_input_s0 = os.path.join(base_dir, f'hapi-bw.json')
  print(f"Reading: {all_input_s0}")
  with open(all_input_s0, 'r', encoding='utf-8') as f:
    datasets_s0o = json.load(f)
  print(f"Read: {all_input_s0}")
  datasets_s0 = restructure(datasets_s0o)

print(f"Reading: {all_input_s1}")
with open(all_input_s1, 'r', encoding='utf-8') as f:
  datasets_s1o = json.load(f)
print(f"Read: {all_input_s1}")

print(f"Reading: {all_input_s2}")
with open(all_input_s2, 'r', encoding='utf-8') as f:
  datasets_s2o = json.load(f)
print(f"Read: {all_input_s2}")

print(f"s1 = {s1}")
print(f"s2 = {s2}")

datasets_s1 = restructure(datasets_s1o)
datasets_s2 = restructure(datasets_s2o)

for dsid in datasets_s1.keys():

  extra = ""
  if "x_cdf_depend_0_name" in datasets_s1[dsid]["info"]["parameters"][0]:
    x_cdf_depend_0_name = datasets_s1[dsid]["info"]["parameters"][0]["x_cdf_depend_0_name"]
    extra = f'for s1 DEPEND_0  = {x_cdf_depend_0_name}'

  if not dsid in datasets_s2:
    print(f'{dsid} not in {s2} {extra}')
    dsid0 = dsid + "@0"
    if dsid[-2] != "@" and dsid0 in list(datasets_s2.keys()):
      print(f"  But {dsid0} in {s2}")
  else:
    compare_info(dsid, datasets_s2[dsid]["info"], datasets_s1[dsid]["info"])
    keys_s2 = datasets_s2[dsid]["info"]["_parameters"].keys()
    keys_s1 = datasets_s1[dsid]["info"]["_parameters"].keys()
    n_params_s2 = len(keys_s2)
    n_params_s1 = len(keys_s1)

    if n_params_s2 != n_params_s1:
      print(f"{dsid}")
      print(f'  n_params_{s2} = {n_params_s2} != n_params_{s1} = {n_params_s1} {extra}')
      print(f"  Differences: {set(keys_s1) ^ set(keys_s2)}")
    else:
      if keys_s2 != keys_s1:
        print(f"{dsid}")
        print(f'  Order differs {extra}')
        print(f"  {s2}: {list(keys_s2)}")
        print(f"  {s1}: {list(keys_s1)}")
      else:
        for i in range(len(datasets_s2[dsid]["info"]["parameters"])):
          param_s2 = datasets_s2[dsid]["info"]["parameters"][i]
          param_s1 = datasets_s1[dsid]["info"]["parameters"][i]
          compare_param(param_s2, param_s1)

      compare_data(dsid, datasets_s0=datasets_s0)
