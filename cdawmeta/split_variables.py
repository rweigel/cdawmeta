import cdawmeta

def split_variables(id, variables, logger, meta_type='hapi', omit_variable=None):
  """
  Create depend_0_dict dict. Each key is the name of the DEPEND_0 variable.
  Each value is a list of variable dicts that reference that DEPEND_0, e.g.,

  {
    'Epoch': {'V1': {...}, 'V2': {...}},
    'Epoch2': {'V3': {...}, 'V4': {...}}
  }

  """

  depend_0_dict = {}

  names = variables.keys()
  for name in names:

    variable_meta = variables[name]

    if omit_variable is not None:
      if omit_variable(id, name):
        continue

    if 'DEPEND_0' in variable_meta['VarAttributes']:
      depend_0_name = variable_meta['VarAttributes']['DEPEND_0']

      if depend_0_name not in variables:
        emsg = f"  Dropping {id}/{name} b/c it has a DEPEND_0 ('{depend_0_name}') "
        emsg += "that is not in dataset"
        cdawmeta.error(meta_type, id, name, "CDF.MissingDEPEND_0", emsg, logger)
        continue

      if depend_0_name not in depend_0_dict:
        depend_0_dict[depend_0_name] = {}
      depend_0_dict[depend_0_name][name] = variable_meta

  return depend_0_dict
