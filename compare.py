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
  datasetsr = {}
  for dataset in datasets:
    id = dataset["id"]
    datasetsr[id] = copy.deepcopy(dataset)
    datasetsr[id]["info"]["_parameters"] = {}
    for pidx, parameter in enumerate(dataset["info"]["parameters"]):
      name = parameter["name"]
      datasetsr[id]["info"]["_parameters"][name] = parameter
  return datasetsr

datasets_bw = restructure(datasets_bwo)
datasets_nl = restructure(datasets_nlo)

for dsid in datasets_nl.keys():
  if not dsid in datasets_bw:
    print(f"{dsid} not in bw")
    dsid0 = dsid + "@0"
    if dsid0 in list(datasets_bw.keys()):
      print(f"  But {dsid}@0 in bw")

for dsid in datasets_bw.keys():
  if not dsid in datasets_nl:
    print(f"{dsid} not in nl")
    dsid0 = dsid + "@0"
    if dsid0 in list(datasets_nl.keys()):
      print(f"  But {dsid}@0 in nl")
  else:
    knl = datasets_nl[dsid]["info"]["_parameters"].keys()
    kbw = datasets_bw[dsid]["info"]["_parameters"].keys()
    nnl = len(knl)
    nbw = len(kbw)
    if nnl != nbw:
      print(f"{dsid}")
      print(f"  nnl = {nnl} != nbw = {nbw}")
      print(f"  {set(kbw) ^ set(knl)}")
    else:
      order_diff = False
      for i in range(len(datasets_nl[dsid]["info"]["parameters"])):
        inl = datasets_nl[dsid]["info"]["parameters"][i]["name"]
        ibw = datasets_bw[dsid]["info"]["parameters"][i]["name"]
        if inl != ibw:
          order_diff = True
      if order_diff:
        print(f"{dsid}")
        print("  Order differs")