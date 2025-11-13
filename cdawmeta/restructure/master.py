import copy

import cdawmeta

def master(master, master_url, logger=None):

  """
  Restructure JSON representation^* of Master CDF to match structure returned
  by cdawmeta.io.read_cdf_meta().

  ^* https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/

  lists of the form [{"0", value0}, {"1", value1}, ...]
  converted to lists of the form [value0, value1, ...].

  dict with arrays of objects to objects with objects. For example,
    { "Epoch": [ 
        {"VarDescription": [{"DataType": "CDF_TIME_TT2000"}, ...] },
        {"VarAttributes": [{"CATDESC": "Default time"}, ...] }
      ],
      ...
    }
  is converted to
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

  master = copy.deepcopy(master)

  # TODO: Check for only one key.
  file = list(master.keys())[0]

  fileinfo_r = cdawmeta.util.array_to_dict(master[file]['CDFFileInfo'])

  variables = master[file]['CDFVariables']
  variables_r = {}

  for variable in variables:

    variable_keys = list(variable.keys())
    if len(variable_keys) > 1:
      msg = "Expected only one variable key in variable object. Exiting witih code 1."
      logger.error(msg)
      exit(1)

    variable_name = variable_keys[0]
    variable_array = variable[variable_name]
    variable_dict = cdawmeta.util.array_to_dict(variable_array)

    for key, value in variable_dict.items():

      if key == 'VarData':
        variable_dict[key] = value
      else:
        variable_dict[key] = sort_keys(cdawmeta.util.array_to_dict(value))

    variables_r[variable_name] = variable_dict

  # Why lower-case G? Inconsistent with CDFVariables.
  globals = master[file]['CDFglobalAttributes']
  globals_r = {}
  """
  Globals is an array of objects with one key each. Each object contains
  and array. For example
  CDFglobalAttributes: [
    { "TITLE": [ {"0": "My Title"} ] }
    { "TEXT": [ {"0": "Line 1"}, {"0": "Line 2"} ] }
    { "HTTP_link" : [ {"0": "http://..." }, {"2": "http://..." } ] }
    ...
  ]
  For text attributes, simplify, e.g.,
    "TITLE": [ {"0": "My Title"} ] }
    =>
    "TITLE": "My Title"
  and join, e.g.,
    "TEXT": [ {"0": "Line 1"}, {"0": "Line 2"} ]
    =>
  "TEXT": ["Line 1, "Line 2"]

  For link-related attributes (Link_text, HTTP_link, and Link_title), join into array, e.g.,
    "HTTP_link" : [ {"0": "http://..." }, {"2": "http://..." } ]
    =>
    "HTTP_link" : [ "http://...", "http://..." ]
  """
  for attr_object in globals:
    # E.g., attr_object = { "TITLE": [ {"0": "My Title"} ] }
    logger.debug(attr_object)
    attr_object_keys = list(attr_object.keys())
    if len(attr_object_keys) > 1:
      if logger is not None:
        # TODO: Determine if this is allowed, and if so, handle.
        msg = "Expected only one key in object that is an array element of CDFglobalAttributes."
        logger.error(msg)
        logger.error(f"Found {attr_object}.")
        logger.error(f"Using only first key: {attr_object_keys[0]}.")
    attr_object_key = attr_object_keys[0]
    attr_object_list = attr_object[attr_object_key]
    # E.g., [ {"0": "A"}, {"1": "B"} ]
    elements = []
    for el in attr_object_list:
      logger.debug(f"  {el}")
      if len(el.keys()) > 1:
        msg = "Expected only one key in attribute array element."
        logger.error(msg)
        logger.error(f"Found {el}.")
        logger.error(f"Using only first key: {el.keys()[0]}.")
      key = list(el.keys())[0]
      elements.append(el[key])

    if len(elements) == 1:
      globals_r[attr_object_key] = elements[0]
    else:
      globals_r[attr_object_key] = elements

  master = {
              #'CDFFileInfo': {'File': file, 'FileURL': master_url, **fileinfo_r},
              'CDFFileInfo': fileinfo_r,
              'CDFglobalAttributes': globals_r,
              'CDFVariables': variables_r
  }

  return master
