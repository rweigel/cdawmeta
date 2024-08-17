import os
import json

import cdawmeta

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.join(root_dir, 'data')
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

import cdawmeta
datasets = cdawmeta.metadata(data_dir='../data', update=False, no_spase=False, embed_data=True)

def get_path(obj, path):
  for key in path:
    if key not in obj:
      return None
    obj = obj[key]
  return obj

def array_to_dict(array, dsid):
  obj = {}
  for element in array:
    if 'ParameterKey' in element:
      try:
        obj[element['ParameterKey']] = element
      except:
        print(f"  {array}")
    else:
      logger.error(f"  {dsid} Error - No ParameterKey for parameter with Name = {element.get('Name', None)}")
  return obj

master_units = []
spase_units = []
master_units_dict = {}
for dsid in datasets:

  print(f"Dataset: {dsid}")
  #print(datasets[dsid])
  if 'spase' not in datasets[dsid] or datasets[dsid]['spase'] is None:
    logger.error(f"  {dsid}: Error - No SPASE.")
    continue
  if 'data' not in datasets[dsid]['spase'] or datasets[dsid]['spase']['data'] is None:
    continue

  Parameter = get_path(datasets[dsid]['spase']['data'], ['Spase', 'NumericalData','Parameter'])
  if Parameter is None:
    logger.error(f"  {dsid}: Error - No Spase/NumericalData/Parameter node")
    continue

  parameter_dict = array_to_dict(Parameter, dsid)

  datasets[dsid]['spase_restructured'] = {'Parameter': parameter_dict}
  parameters = datasets[dsid]['spase_restructured']['Parameter']
  variables = datasets[dsid]['master']['data']['CDFVariables']

  for vid in list(variables.keys()):
    if 'VarAttributes' not in variables[vid]:
      logger.error(f"  {dsid}/{vid}: Error in master - No VarAttributes")
      continue
    if 'VAR_TYPE' not in variables[vid]['VarAttributes']:
      logger.error(f"  {dsid}/{vid}: Error in master - No VarAttributes/VAR_TYPE")
      continue

    if variables[vid]['VarAttributes'].get('VAR_TYPE', None) not in ['data', 'support_data']:
      continue

    logger.info(f"{vid}")

    if vid not in parameters:
      logger.error(f"  {dsid}/{vid} Error in SPASE - Parameter not in SPASE")
      continue

    for si_conversion in ['SI_conversion', 'SI_CONV', 'SI_conv']:
      if si_conversion in variables[vid]['VarAttributes']:
        conv = variables[vid]['VarAttributes'][si_conversion]
        logger.info(f"  master/SI_CONVERSION: '{conv}' (called {si_conversion})")

    if 'UNITS_PTR' in variables[vid]['VarAttributes']:
      units_var = variables[vid]['VarAttributes']['UNITS_PTR']
      print(variables[units_var])
      exit()

    if 'UNITS' in variables[vid]['VarAttributes']:
      UNITS = variables[vid]['VarAttributes']['UNITS']
      logger.info(f"  master/UNITS: '{UNITS}'")
      master_units.append(UNITS)
      if not UNITS in master_units_dict:
        master_units_dict[UNITS] = []
    else:
      UNITS = None
      logger.info("  master/UNITS: No UNITS attribute")

    if 'Units' in parameters[vid]:
      Units = parameters[vid]['Units']
      if UNITS is not None:
        master_units_dict[UNITS].append(Units)
      logger.info(f"  spase/Units:  '{Units}'")
      spase_units.append(Units)
    else:
      logger.info("  spase/Units: No <Units> element")

    if 'COORDINATE_SYSTEM' in variables[vid]['VarAttributes']:
      csys = variables[vid]['VarAttributes']['COORDINATE_SYSTEM']
      logger.info(f"  master/COORDINATE_SYSTEM: '{csys}'")
    else:
      logger.info(f"  master/COORDINATE_SYSTEM: No COORDINATE_SYSTEM attribute")

    if 'CoordinateSystem' in parameters[vid]:
      csys = parameters[vid]['CoordinateSystem']
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
