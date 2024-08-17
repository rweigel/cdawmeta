import os
import json

base_dir = os.path.join(os.path.dirname(__file__), '../data')
all_input_bw = os.path.join(base_dir, 'hapi','catalog-all.json')
all_input_nl = os.path.join(base_dir, 'hapi', 'catalog-all.nl.json')
file_body   = os.path.join(base_dir, 'tables/hapi.table.body.json')
file_header = os.path.join(base_dir, 'tables/hapi.table.head.json')

def uniq_keys(datasets, ukeys=None):

  if ukeys is None:

    # Some required keys are added manually so their position in
    # the list of keys is fixed (assumes Python 3.6+; should check).
    ukeys = {
              'id': None,
              'info': {
                      'parameters': {
                        'name': None,
                        'bins': {}
                      },
                      'startDate': None,
                      'stopDate': None,
                    }
            }

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

    if didx == 0:
      heado = ['version']

    rowo = [prefix]
    for dkey in ukeys.keys():
      if dkey != 'info':
        if didx == 0:
          heado.append(dkey)
        if dkey in dataset:
          rowo.append(dataset[dkey])
        else:
          rowo.append("")

    for ikey in ukeys['info'].keys():
      if ikey != 'parameters':
        if didx == 0:
          heado.append(ikey)
        if ikey in dataset['info']:
          rowo.append(dataset['info'][ikey])
        else:
          rowo.append("")

    for pidx, parameter in enumerate(dataset['info']['parameters']):

      row = [pidx]
      if didx == 0 and pidx == 0:
        head = ['index']

      for pkey in ukeys['info']['parameters'].keys():
        if pkey != 'bins':
          if didx == 0 and pidx == 0:
            head.append(pkey)
          if pkey in parameter:
            row.append(parameter[pkey])
          else:
            row.append("")

      for bkey in ukeys['info']['parameters']['bins'].keys():
        if didx == 0 and pidx == 0:
          head.append(f"bins[0][{bkey}]")
        if 'bins' in parameter:
          if bkey in parameter['bins'][0]:
            row.append(parameter['bins'][0][bkey])
          else:
            row.append("")
        else:
          row.append("")

      # Change id value to to id/name
      rowoc = rowo.copy()
      #rowoc[1] = rowoc[1] + "/" + row[1]

      #rows.append([*rowoc, *row]) # Default order
      rows.append([rowoc[1], rowo[0], *row, *rowoc[2:]])

  heado[1] = 'id'
  # head = [*heado, *head] Default order
  head = [heado[1], heado[0], *head, *heado[2:]]
  return head, rows

print(f'Reading: {all_input_bw}')
with open(all_input_bw, 'r', encoding='utf-8') as f:
  datasets_bw = json.load(f)
print(f'Read: {all_input_bw}')

print(f'Reading: {all_input_nl}')
with open(all_input_nl, 'r', encoding='utf-8') as f:
  datasets_nl = json.load(f)
print(f'Read: {all_input_nl}')

ukeys = uniq_keys(datasets_bw)
ukeys = uniq_keys(datasets_nl, ukeys=ukeys)

head, rows_bw = table(datasets_bw, ukeys, 'bw')
_, rows_nl = table(datasets_nl, ukeys, 'nl')
rows = [*rows_bw, *rows_nl]

os.makedirs(os.path.dirname(file_header), exist_ok=True)
print(f'Writing: {file_header}')
with open(file_header, 'w', encoding='utf-8') as f:
  json.dump(head, f, indent=2)
  print(f'Wrote: {file_header}')

os.makedirs(os.path.dirname(file_body), exist_ok=True)
print(f'Writing: {file_body}')
with open(file_body, 'w', encoding='utf-8') as f:
  json.dump(rows, f, indent=2)
  print(f'Wrote: {file_body}')
