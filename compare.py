import os
import copy

base_dir = os.path.join(os.path.dirname(__file__), 'data')
all_input_bw = os.path.join(base_dir, 'hapi-bw.json')
all_input_nl = os.path.join(base_dir, 'hapi-nl.json')

import json
print(f"Reading: {all_input_bw}")
with open(all_input_bw, 'r', encoding='utf-8') as f:
  datasets_bw = json.load(f)
print(f"Read: {all_input_bw}")

print(f"Reading: {all_input_nl}")
with open(all_input_nl, 'r', encoding='utf-8') as f:
  datasets_nl = json.load(f)
print(f"Read: {all_input_nl}")

def restructure(datasets):
  datasetsr = {}
  for dataset in datasets:
    id = dataset["id"]
    datasetsr[id] = copy.deepcopy(dataset)
    datasetsr[id]["info"] = {}
    for pidx, parameter in enumerate(dataset["info"]["parameters"]):
      name = parameter["name"]
      datasetsr[id]["info"][name] = parameter
      datasetsr[id]["info"][name]["index"] = pidx
  return datasetsr

datasets_bw = restructure(datasets_bw)
datasets_nl = restructure(datasets_nl)

for dsid in datasets_nl.keys():
  if not dsid in datasets_bw:
    print(f"{dsid} not in bw")

for dsid in datasets_bw.keys():
  if not dsid in datasets_nl:
    print(f"{dsid} not in nl")