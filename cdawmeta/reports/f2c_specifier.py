import os
import sys

import cdawmeta

def f2c_specifier(clargs):

  out_dir = 'reports'
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  dir_name = os.path.join(cdawmeta.DATA_DIR, out_dir)

  # TODO: This metadata is available in x_keys in HAPI metadata.
  #       Rewrite to use it.

  # Compare given FORMAT/FORM_PTR with computed FORMAT specifier.

  clargs['meta_type'] = ['master']
  meta = cdawmeta.metadata(**clargs)
  formats = []
  for id in meta.keys():
    if "master" not in meta[id]:
      continue

    master = meta[id]["master"]['data']
    if master is None or "CDFVariables" not in master:
      continue
    variables = master['CDFVariables']
    for variable_name, variable in variables.items():

      if "VarAttributes" not in variable or "VarDescription" not in variable:
        continue

      FORMAT_given, emsg, etype = cdawmeta.attrib.FORMAT(id, variable_name, variables, c_specifier=False)
      if etype is not None:
        logger.error(f"{id}: {emsg}")
      if FORMAT_given is None:
        continue

      FORMAT_computed, emsg, etype = cdawmeta.attrib.FORMAT(id, variable_name, variables)
      if etype is not None:
        logger.error(f"{id}: {emsg}")
      if FORMAT_computed is None:
        continue

      if 'DataType' in variable['VarDescription']:
        DataType = variable['VarDescription']['DataType']
      else:
        continue

      if FORMAT_computed is None:
        formats.append(f"{FORMAT_given}; {DataType}; None")
      else:
        formats.append(f"{FORMAT_given}; {DataType}; {FORMAT_computed}")

  uniques = list(set(formats))
  lines = "# CDFDataType, FortranSpecifier, CSpecifierInferred\n"
  for unique in uniques:
    unique = unique.split("; ")
    if unique[2] == 'None':
      line = f"{unique[1]}, {unique[0]}, None"
    else:
      if len(unique[0].split(",")) > 1:
        line = f"{unique[1]}, {unique[0]}, {unique[2]}"
      else:
        line = f"{unique[1]}, '{unique[0]}', '{unique[2]}'"
    logger.info(line)
    lines += line + "\n"

  fname = os.path.join(dir_name, f"{report_name}.txt")
  cdawmeta.util.write(fname, lines, logger=logger)
