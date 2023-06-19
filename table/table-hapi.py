import os
base_dir = os.path.join(os.path.dirname(__file__), '../data')
all_input_bw = os.path.join(base_dir, 'hapi-bw.json')
all_input_nl = os.path.join(base_dir, 'hapi-nl.json')
file_body   = os.path.join(base_dir, 'tables/hapi.table.body.json')
file_header = os.path.join(base_dir, 'tables/hapi.table.header.json')

import json
with open(all_input_bw, 'r', encoding='utf-8') as f:
  datasets_bw = json.load(f)
with open(all_input_nl, 'r', encoding='utf-8') as f:
  datasets_nl = json.load(f)

def uniq_keys(datasets, ukeys=None):

  if ukeys is None:
    ukeys = {}
    ukeys['info'] = {}
    ukeys['info']['parameter'] = {}
    ukeys['info']['parameter']['bins'] = {}

  for dataset in datasets:
    dkeys = list(dataset.keys())
    for key in dkeys:
      if not key.startswith('x_') and key != 'info':
        ukeys[key] = None

    ikeys = list(dataset['info'].keys())
    for key in ikeys:
      if not key.startswith('x_') and key != 'parameters':
        ukeys['info'][key] = None

    for parameter in dataset['info']['parameters']:
      for key in parameter.keys():
        if not key.startswith('x_') and key != 'bins':
          ukeys['info']['parameter'][key] = None

      if 'bins' in parameter:
        for i in range(len(parameter['bins'])):
          for key in parameter['bins'][i].keys():
            if not key.startswith('x_'):
              ukeys['info']['parameter']['bins'][key] = None
  return ukeys

ukeys = uniq_keys(datasets_bw)
ukeys = uniq_keys(datasets_nl, ukeys=ukeys)
print(ukeys)
