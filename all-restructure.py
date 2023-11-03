import os
import json

base_dir = os.path.dirname(__file__)
all_file = os.path.join(base_dir, 'data/all-resolve.json')
all_file_restructured = os.path.join(base_dir, 'data/all-restructure.json')

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

    with open(dataset['_master'], 'r', encoding='utf-8') as f:
      dataset['_master_data'] = json.load(f)["_decoded_content"]

    #add_globals(dataset)

    file = list(dataset['_master_data'].keys())[0]

    variables = dataset['_master_data'][file]['CDFVariables']
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

    del dataset["_master_data"]

    dataset['_variables'] = variables_new


def add_globals(dataset):

  file = list(dataset['_master_data'].keys())[0]

  globals = dataset['_master_data'][file]['CDFglobalAttributes']
  globals_new = {}

  for _global in globals:
    gkey = list(_global.keys())
    if len(gkey) > 1:
      print("Expected only one key in _global object.")
      exit()
    gvals = _global[gkey[0]]
    text = []
    for gval in gvals:
      line = gval[list(gval.keys())[0]];
      text.append(str(line))

    globals_new[gkey[0]] = "\n".join(text)

  dataset['_globals'] = globals_new


def add_sample_start_stop(datasets):

  def extract_sample_start_stop(file_list):

    if isinstance(file_list["FileDescription"], dict):
      file_list["FileDescription"] = [file_list["FileDescription"]]

    num_files = len(file_list["FileDescription"])
    if num_files == 0:
      sampleFile = None
    if num_files == 1:
      sampleFile = file_list["FileDescription"][0]
    elif num_files == 2:
      sampleFile = file_list["FileDescription"][1]
    else:
      sampleFile = file_list["FileDescription"][-2]

    if sampleFile is not None:
      sampleStartDate = sampleFile["StartTime"]
      sampleStopDate = sampleFile["EndTime"]

    range = {
              "sampleStartDate": sampleStartDate,
              "sampleStopDate": sampleStopDate
            }

    return range

  for dataset in datasets:
    with open(dataset['_file_list'], 'r', encoding='utf-8') as f:
      dataset['_file_list_data'] = json.load(f)["_decoded_content"]

    if not "FileDescription" in dataset["_file_list_data"]:
      print("No file list for " + dataset["id"])
      continue
    range = extract_sample_start_stop(dataset["_file_list_data"])
    dataset["info"]["sampleStartDate"] = range["sampleStartDate"]
    dataset["info"]["sampleStopDate"] = range["sampleStopDate"]

    del dataset["_file_list_data"]

print(f'Reading: {all_file}')
with open(all_file, 'r', encoding='utf-8') as f:
  datasets = json.load(f)
print(f'Read: {all_file}')

for idx, dataset in enumerate(datasets):

  if '_master' not in dataset:
    print(f'Error: No _master in {dataset["id"]}. Omitting dataset.')
    datasets[idx] = None
    continue

datasets = [i for i in datasets if i is not None]

# Add _variables element to each dataset
add_variables(datasets)

add_sample_start_stop(datasets)

# Save result to all_file_restructured; _variables node is used by hapi-bw.py
# and table-all.py.
print(f'Writing: {all_file_restructured}')
with open(all_file_restructured, 'w', encoding='utf-8') as f:
  json.dump(datasets, f, indent=2)
print(f'Wrote: {all_file_restructured}')
