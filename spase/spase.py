import os
import json

import cdawmeta

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.join(root_dir, 'data')
in_file  = os.path.join(base_dir, 'cdaweb.json')
spase_units_file = os.path.join(os.path.dirname(__file__), 'spase-units.txt')
master_units_file = os.path.join(os.path.dirname(__file__), 'master-units.txt')
master2spase_units_file = os.path.join(os.path.dirname(__file__), 'master2spase-units.txt')

log_config = {
  'file_log': os.path.join(root_dir, 'data', 'spase', 'spase.log'),
  'file_error': os.path.join(root_dir, 'data', 'spase', 'spase.errors.log'),
  'format': '%(message)s',
  'rm_string': root_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

logger.info(f'Reading: {in_file}')

with open(in_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
logger.info(f'Read: {in_file}')

from cdawmeta.restructure_master import add_master_restructured
datasets = add_master_restructured(root_dir, datasets)

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
      logger.error(f"  Error - No ParameterKey in {element}")
  return obj

master_units = []
spase_units = []
master_units_dict = {}
for dataset in datasets:

  print(f"Dataset: {dataset['id']}")
  if '_spase' not in dataset or dataset['_spase'] is None:
    logger.error(f"  Error - No SPASE for: {dataset['id']}")
    continue
  fname = os.path.join(root_dir, dataset['_spase'])
  with open(fname, 'r', encoding='utf-8') as f:
    #print(f"Reading: {dataset['_spase'].replace(base_dir,'data')}")
    dataset['_spase_content'] = json.load(f)["_decoded_content"]

  logger.info(f"  SPASE: {dataset['_spase']}")
  logger.info(f"  Master: {dataset['_master']}")

  Parameter = get_path(dataset['_spase_content'], ['Spase', 'NumericalData','Parameter'])
  if Parameter is None:
    logger.error(f"  Error - No Spase/NumericalData/Parameter node in {dataset['id']}")
    continue

  parameter_dict = array_to_dict(Parameter)

  dataset['_spase_restructured'] = {'Parameter': parameter_dict}
  parameters = dataset['_spase_restructured']['Parameter']
  variables = dataset['_master_restructured']['_variables']

  for id in list(variables.keys()):
    if 'VarAttributes' not in variables[id]:
      logger.error(f"  Error in master - No VarAttributes in {dataset['id']}")
      continue
    if 'VAR_TYPE' not in variables[id]['VarAttributes']:
      logger.error(f"  Error in master - No VarAttributes/VAR_TYPE in {dataset['id']}")
      continue

    logger.info(f" {id}")

    if id not in parameters:
      logger.error(f"  Error in SPASE - Parameter {dataset['id']}/{id} not in SPASE")
      continue

    for si_conversion in ['SI_conversion', 'SI_CONV', 'SI_conv']:
      if si_conversion in variables[id]['VarAttributes']:
        conv = variables[id]['VarAttributes'][si_conversion]
        logger.info(f"  master/SI_CONVERSION: '{conv}' (called {si_conversion})")

    if 'UNITS' in variables[id]['VarAttributes']:
      UNITS = variables[id]['VarAttributes']['UNITS']
      logger.info(f"  master/UNITS: '{UNITS}'")
      master_units.append(UNITS)
      if not UNITS in master_units_dict:
        master_units_dict[UNITS] = []
    else:
      UNITS = None
      logger.info("  master/UNITS: No UNITS attribute")

    if 'Units' in parameters[id]:
      Units = parameters[id]['Units']
      if UNITS is not None:
        master_units_dict[UNITS].append(Units)
      logger.info(f"  spase/Units:  '{Units}'")
      spase_units.append(Units)
    else:
      logger.info("  spase/Units: No <Units> element")

    if 'COORDINATE_SYSTEM' in variables[id]['VarAttributes']:
      csys = variables[id]['VarAttributes']['COORDINATE_SYSTEM']
      logger.info(f"  master/COORDINATE_SYSTEM: '{csys}'")
    else:
      logger.info(f"  master/COORDINATE_SYSTEM: No COORDINATE_SYSTEM attribute")

    if 'CoordinateSystem' in parameters[id]:
      csys = parameters[id]['CoordinateSystem']
      logger.info(f"  spase/CoordinateSystem:   '{csys}'")
    else:
      logger.info(f"  spase/CoordinateSystem:   No <CoordinateSystem> element")

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

content = ''
from collections import Counter
for key in master_units_dict:
  uniques = dict(Counter(master_units_dict[key]))
  if len(uniques) > 0:
    content += f"'{key}': {uniques}\n"

write_file(master2spase_units_file, content)
