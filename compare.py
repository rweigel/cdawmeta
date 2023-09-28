import os
import copy

base_dir = os.path.join(os.path.dirname(__file__), 'data')
all_input_bw = os.path.join(base_dir, 'hapi-bw.json')
all_input_nl = os.path.join(base_dir, 'hapi-nl.json')

import json
print(f"Reading: {all_input_bw}")
with open(all_input_bw, 'r', encoding='utf-8') as f:
  datasets_bwo = json.load(f)
print(f"Read: {all_input_bw}")

print(f"Reading: {all_input_nl}")
with open(all_input_nl, 'r', encoding='utf-8') as f:
  datasets_nlo = json.load(f)
print(f"Read: {all_input_nl}")

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

def compare_param(params_nl, params_bw):

  params_nl_keys = sorted(list(params_nl.keys()))
  params_bw_keys = sorted(list(params_bw.keys()))

  for key in params_bw_keys.copy():
    if key.startswith("x_cdf_"):
      params_bw_keys.remove(key)

  n_param_keys_nl = len(params_nl_keys)
  n_param_keys_bw = len(params_bw_keys)
  if n_param_keys_nl != n_param_keys_bw:
    print(f"{dsid}/{params_nl['name']}")
    print(f'  n_param_keys_nl = {n_param_keys_nl} != n_param_keys_bw = {n_param_keys_bw} for bw DEPEND_0  = {x_cdf_depend_0_name}')
    print(f"  Differences: {set(params_bw_keys) ^ set(params_nl_keys)}")

  compare_bins(params_nl, params_bw)

def compare_bins(params_nl, params_bw):

  name_nl = params_nl["name"]
  name_bw = params_bw["name"]
  if 'bins' in params_nl:
    if not 'bins' in params_bw:
      print(f"{dsid}")
      print(f'  nl has bins for {name_nl} but bw does not')
  if 'bins' in params_bw:
    if not 'bins' in params_nl:
      print(f"{dsid}")
      print(f'  bw has bins for {name_bw} but nl does not')
  if 'bins' in params_bw:
    if 'bins' in params_nl:
      n_bins_nl = len(params_bw["bins"])
      n_bins_bw = len(params_nl["bins"])
      if n_bins_nl != n_bins_bw:
        print(f"{dsid}")
        print(f'  bw has {n_bins_bw} bins objects; nl has {n_bins_nl}')
      # TODO: Compare content at bins level

datasets_bw = restructure(datasets_bwo)
datasets_nl = restructure(datasets_nlo)

for dsid in datasets_nl.keys():
  if not dsid in datasets_bw:
    print(f"{dsid} not in bw")
    dsid0 = dsid + "@0"
    if dsid[-2] != "@" and dsid0 in list(datasets_bw.keys()):
      print(f"  But {dsid0} in bw")

for dsid in datasets_bw.keys():
  if not dsid in datasets_nl:
    x_cdf_depend_0_name = datasets_bw[dsid]["info"]["parameters"][0]["x_cdf_depend_0_name"]
    print(f'{dsid} not in nl (DEPEND_0 = {x_cdf_depend_0_name})')
    dsid0 = dsid + "@0"
    if dsid[-2] != "@" and dsid0 in list(datasets_nl.keys()):
      print(f"  But {dsid0} in nl")
  else:
    keys_nl = datasets_nl[dsid]["info"]["_parameters"].keys()
    keys_bw = datasets_bw[dsid]["info"]["_parameters"].keys()
    n_params_nl = len(keys_nl)
    n_params_bw = len(keys_bw)
    x_cdf_depend_0_name = datasets_bw[dsid]["info"]["parameters"][0]["x_cdf_depend_0_name"]
    if n_params_nl != n_params_bw:
      print(f"{dsid}")
      print(f'  n_params_nl = {n_params_nl} != n_params_bw = {n_params_bw} for bw DEPEND_0  = {x_cdf_depend_0_name}')
      print(f"  Differences: {set(keys_bw) ^ set(keys_nl)}")
    else:
      if keys_nl != keys_bw:
        print(f"{dsid}")
        print(f'  Order differs for bw DEPEND_0 = {x_cdf_depend_0_name}')
        print(f"  nl: {list(keys_nl)}")
        print(f"  bw: {list(keys_bw)}")
      else:
        for i in range(len(datasets_nl[dsid]["info"]["parameters"])):
          param_nl = datasets_nl[dsid]["info"]["parameters"][i]
          param_bw = datasets_bw[dsid]["info"]["parameters"][i]
          compare_param(param_nl, param_bw)

