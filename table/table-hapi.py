import os
base_dir = os.path.join(os.path.dirname(__file__), '../data')
all_input_bw = os.path.join(base_dir, 'hapi-bw.json')
all_input_nl = os.path.join(base_dir, 'hapi-nl.json')
file_body   = os.path.join(base_dir, 'tables/hapi.table.body.json')
file_header = os.path.join(base_dir, 'tables/hapi.table.header.json')

def uniq_keys(datasets, ukeys=None):

  if ukeys is None:
    ukeys = {}
    ukeys['info'] = {}
    ukeys['info']['parameters'] = {}
    ukeys['info']['parameters']['bins'] = {}

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
          ukeys['info']['parameters'][key] = None

      if 'bins' in parameter:
        for i in range(len(parameter['bins'])):
          for key in parameter['bins'][i].keys():
            if not key.startswith('x_'):
              ukeys['info']['parameters']['bins'][key] = None
  return ukeys

def table(datasets, ukeys, prefix):

  rows = [];
  for didx, dataset in enumerate(datasets):
    if didx == 0: heado = ['version']
    rowo = [prefix]
    for dkey in ukeys.keys():
      if dkey != 'info':
        if didx == 0: heado.append(dkey)
        if dkey in dataset:
          rowo.append(dataset[dkey])
        else:
          rowo.append(None)

    for ikey in ukeys['info'].keys():
      if ikey != 'parameters':
        if didx == 0: heado.append(ikey)
        if ikey in dataset['info']:
          rowo.append(dataset['info'][ikey])
        else:
          rowo.append(None)

    for pidx, parameter in enumerate(dataset['info']['parameters']):
      row = [pidx]
      if didx == 0 and pidx == 0: head = ['index']
      for pkey in ukeys['info']['parameters'].keys():
        if pkey != 'bins':
          if didx == 0 and pidx == 0: head.append(pkey)
          if pkey in parameter:
            row.append(parameter[pkey])
          else:
            row.append(None)

      if 'bins' in parameter:
        for bkey in ukeys['info']['parameters']['bins'].keys():
          print(bkey)

      rows.append([*rowo, *row])

  return [*heado, *head], rows

import json
with open(all_input_bw, 'r', encoding='utf-8') as f:
  datasets_bw = json.load(f)
with open(all_input_nl, 'r', encoding='utf-8') as f:
  datasets_nl = json.load(f)

ukeys = uniq_keys(datasets_bw)
ukeys = uniq_keys(datasets_nl, ukeys=ukeys)

head, rows_bw = table(datasets_bw, ukeys, 'bw')
_, rows_nl = table(datasets_nl, ukeys, 'nl')
rows = [*rows_bw, *rows_nl]

os.makedirs(os.path.dirname(file_header), exist_ok=True)
with open(file_header, 'w', encoding='utf-8') as f:
  json.dump(head, f, indent=2)
  print(f'Wrote: {file_header}')

os.makedirs(os.path.dirname(file_body), exist_ok=True)
with open(file_body, 'w', encoding='utf-8') as f:
  json.dump(rows, f, indent=2)
  print(f'Wrote: {file_body}')
