import os
import json

base_dir = os.path.join(os.path.dirname(__file__), '..', 'data') 
in_file  = os.path.join(base_dir, 'cdaweb.json')
out_dir  = os.path.join(base_dir, 'spase')
out_file = os.path.join(out_dir, 'spase.json')

print(f'Reading: {in_file}')
with open(in_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {in_file}')

from cdawmeta.restructure_master import add_master_restructured
datasets = add_master_restructured(datasets)

def array_to_dict(array):
  obj = {}
  for element in array:
    obj[element['ParameterKey']] = element
  return obj

for dataset in datasets:
  with open(dataset['_spase'], 'r', encoding='utf-8') as f:
    dataset['_spase'] = json.load(f)["_decoded_content"]

  Parameter = dataset['_spase']['Spase']['NumericalData']['Parameter']

  dataset['_spase_restructured'] = {'Parameter': array_to_dict(Parameter)}
  parameters = dataset['_spase_restructured']['Parameter']
  variables = dataset['_master_restructured']['_variables']
  print(f"Dataset: {dataset['id']}")
  for id in list(variables.keys()):
    if variables[id]['VarAttributes']['VAR_TYPE'] == 'data':
      if id not in parameters:
        print(f"Key '{id}' not in SPASE")
        continue

      print(f" {id}")

      if 'UNITS' in variables[id]['VarAttributes']:
        units = variables[id]['VarAttributes']['UNITS']
        print(f"  master: {units}")
      else:
        print("  master: No units")

      if 'Units' in parameters[id]:
        units = parameters[id]['Units']
        print(f"  spase:  {units}")
      else:
        print("  spase: No units")
