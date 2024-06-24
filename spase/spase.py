import os
import json

base_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'data'))
in_file  = os.path.normpath(os.path.join(base_dir, 'cdaweb.json'))
out_dir  = os.path.join(base_dir, 'spase')

spase_units_file = os.path.join(os.path.dirname(__file__), 'spase-units.txt')
master_units_file = os.path.join(os.path.dirname(__file__), 'master-units.txt')
master2spase_units_file = os.path.join(os.path.dirname(__file__), 'master2spase-units.txt')

rel_path = in_file.replace(base_dir,'data')
print(f'Reading: {rel_path}')

with open(in_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {in_file}')

from cdawmeta.restructure_master import add_master_restructured
datasets = add_master_restructured(datasets)

def get_path(obj, path):
  for key in path:
    if key not in obj:
      return None
    obj = obj[key]
  return obj

def array_to_dict(array):
  obj = {}
  for element in array:
    if 'ParameterKey' in element:
      try:
        obj[element['ParameterKey']] = element
      except:
        print(f"  {array}")
    else:
      print(f"No ParameterKey in {element}")
  return obj

master_units = []
spase_units = []
master_units_dict = {}
for dataset in datasets:
  if '_spase' not in dataset or dataset['_spase'] is None:
    print(f"No SPASE for: {dataset['id']}")
    continue
  with open(dataset['_spase'], 'r', encoding='utf-8') as f:
    print(f"Reading: {dataset['_spase'].replace(base_dir,'data')}")
    dataset['_spase'] = json.load(f)["_decoded_content"]

  print(f"Dataset: {dataset['id']}")

  Parameter = get_path(dataset['_spase'], ['Spase', 'NumericalData','Parameter'])
  if Parameter is None:
    print(f"No Spase/NumericalData/Parameter in {dataset['id']}")
    continue

  parameter_dict = array_to_dict(Parameter)

  dataset['_spase_restructured'] = {'Parameter': parameter_dict}
  parameters = dataset['_spase_restructured']['Parameter']
  variables = dataset['_master_restructured']['_variables']

  for id in list(variables.keys()):
    if 'VarAttributes' not in variables[id]:
      print(f"No VarAttributes in {dataset['id']}")
      continue
    if 'VAR_TYPE' not in variables[id]['VarAttributes']:
      print(f"No VarAttributes/VAR_TYPE in {dataset['id']}")
      continue

    if variables[id]['VarAttributes']['VAR_TYPE'] == 'data':
      if id not in parameters:
        print(f" {id} not in SPASE")
        continue

      print(f" {id}")

      if 'UNITS' in variables[id]['VarAttributes']:
        UNITS = variables[id]['VarAttributes']['UNITS']
        print(f"  master/UNITS: {UNITS}")
        master_units.append(UNITS)
        if not UNITS in master_units_dict:
          master_units_dict[UNITS] = []
      else:
        UNITS = None
        print("  master/UNITS: No UNITS attribute")

      if 'Units' in parameters[id]:
        Units = parameters[id]['Units']
        if UNITS is not None:
          master_units_dict[UNITS].append(Units)
        print(f"  spase/Units:  {Units}")
        spase_units.append(Units)
      else:
        print("  spase/Units: No <Units> element")

      if 'COORDINATE_SYSTEM' in variables[id]['VarAttributes']:
        csys = variables[id]['VarAttributes']['COORDINATE_SYSTEM']
        print(f"  master/COORDINATE_SYSTEM: {csys}")
      else:
        print(f"  master/COORDINATE_SYSTEM: No COORDINATE_SYSTEM attribute")

      if 'CoordinateSystem' in parameters[id]:
        csys = parameters[id]['CoordinateSystem']
        print(f"  spase/CoordinateSystem:   {csys}")
      else:
        print(f"  spase/CoordinateSystem:   No <CoordinateSystem> element")

master_units = '\n'.join(set(master_units))
spase_units = '\n'.join(set(spase_units))

def write_file(filename, content):
  dir = os.path.dirname(filename)
  if not os.path.exists(dir):
    print(f'Creating {dir}')
    os.makedirs(dir, exist_ok=True)
  print(f"Writing: {filename.replace(base_dir,'data')}")
  with open(filename, 'w', encoding='utf-8') as f:
    f.write(content)
  print(f"Wrote:   {filename}")

write_file(spase_units_file, spase_units)
write_file(master_units_file, master_units)

print("Unique UNITS in masters")
print(master_units)
print(70*"-")
print("Unique Units in SPASE")
print(spase_units)

print(70*"-")
print("Unique UNITS -> Units mapping")
content = ''
for key in master_units_dict:
  uniques = list(set(master_units_dict[key]))
  if len(uniques) > 0:
    print(f"'{key}': {uniques}")
    content += f"'{key}': {uniques}\n"

write_file(master2spase_units_file, content)
