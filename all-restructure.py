import os
import json

base_dir = os.path.dirname(__file__)
all_file = os.path.join(base_dir, 'data/all-resolved.json')
all_file_restructured = os.path.join(base_dir, 'data/all-resolved.restructured.json')

def add_variables(datasets):

  """
  Convert dict with arrays of objects to objects with objects. For example
    { "Epoch": [ 
        {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
        {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
      ]
    }
  is converted and written to _variables as
    {
      "Epoch": {
        "VarDescription": {
          "DataType": "CDF_TIME_TT2000",
          ...
        },
        "VarAttributes": {
          "CATDESC": "Default time",
          ...
        }
      }
    }
  """

  def sort_keys(obj):
    return {key: obj[key] for key in sorted(obj)}

  def array_to_dict(array):
    obj = {}
    for element in array:
      key = list(element.keys())[0]
      obj[key] = element[key]
    return obj

  for dataset in datasets:

    file = list(dataset['_master'].keys())[0]

    variables = dataset['_master'][file]['CDFVariables']
    variables_new = {}

    for variable in variables:

      variable_keys = list(variable.keys())
      if len(variable_keys) > 1:
        print(dataset["id"] + ": Expected only one variable key in variable object.")
        exit(0)

      variable_name = variable_keys[0]
      variable_array = variable[variable_name]
      variable_dict = array_to_dict(variable_array)

      for key, value in variable_dict.items():

        if key == 'VarData':
          variable_dict[key] = value
        else:
          variable_dict[key] = sort_keys(array_to_dict(value))

      variables_new[variable_name] = variable_dict

    dataset['_variables'] = variables_new

    break

def add_globals(datasets):

  for dataset in datasets:

    file = list(dataset['_master'].keys())[0]

    globals = dataset['_master'][file]['CDFglobalAttributes']
    globals_new = {}

    for _global in globals:
      gkey = list(_global.keys())
      if len(gkey) > 1:
        print("Expected only one key in _global object.")
        exit()
      gvals = _global[gkey[0]]
      text = []
      for gval in gvals:
        text.append(gval[list(gval.keys())[0]])
      print("\n".join(text))
      globals_new[gkey[0]] = "\n".join(text)

    dataset['_globals'] = globals_new

print(f'Reading: {all_file}')
with open(all_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {all_file}')

for idx, dataset in enumerate(datasets):

  if '_master' not in dataset:
    print(f'Error: No _master in {dataset["id"]}. Omitting dataset.')
    datasets[idx] = None
    continue

  if not dataset['id'].startswith('ELA_L1_STATE_PRED'):
    datasets[idx] = None
    continue

datasets = [i for i in datasets if i is not None]

# Add _variables element to each dataset
add_variables(datasets)
add_globals(datasets)
#for idx, dataset in enumerate(datasets):
  #del datasets[idx]["_master"]
  #if "_spase" in dataset:
  #  del datasets[idx]["_spase"]

# Save result to all_file_restructured; _variables node is used by hapi-bw.py
# and table-all.py.
print(f'Writing: {all_file_restructured}')
with open(all_file_restructured, 'w', encoding='utf-8') as f:
  json.dump(datasets, f, indent=2)
print(f'Wrote: {all_file_restructured}')
