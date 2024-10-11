import os
import sys
import shutil

import cdawmeta

def units(clargs):

  out_dir = 'reports'
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  dir_name = os.path.join(cdawmeta.DATA_DIR, out_dir)
  dir_additions = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-spase')

  master_units_dict = {}
  master_si_dict = {}

  clargs = {**clargs, 'meta_type': ['master', 'spase']}
  meta = cdawmeta.metadata(**clargs)
  missing_units = {}

  for dsid in meta.keys():
    logger.info(f"\n-----{dsid}-----")

    if "master" not in meta[dsid]:
      logger.info("  Master: x No Master")

    master = cdawmeta.util.get_path(meta[dsid], ['master', 'data'])
    if master is None:
      logger.info("  Master: x No Master data")
      continue

    if "CDFVariables" not in master:
      logger.info("  Master: x No CDFVariables in Master")
      continue

    spase = cdawmeta.util.get_path(meta[dsid], ['spase', 'data'])
    have_spase = True
    if not spase:
      logger.info("  SPASE: x No SPASE available")
      have_spase = False
    spase = cdawmeta.restructure.spase(spase, logger=logger)

    logger.info(f"  CDF:   {meta[dsid]['master']['request']['url']}")
    if have_spase:
      logger.info(f"  SPASE: {meta[dsid]['spase']['request']['url']}")

    variables = master['CDFVariables']
    missing_units[dsid] = {}
    for variable_name, variable in variables.items():

      if "VarAttributes" not in variable or "VarDescription" not in variable:
        continue

      VAR_TYPE = None
      if 'VAR_TYPE' in variable['VarAttributes']:
        VAR_TYPE = variable['VarAttributes']['VAR_TYPE']

      if VAR_TYPE not in ['data', 'support_data']:
        continue

      logger.info(variable_name)

      UNITS, etype, emsg = cdawmeta.attrib.UNITS(dsid, variable_name, variables)
      if UNITS is None:
        if emsg:
          msg = emsg
          missing_units[dsid][variable_name] = msg
          logger.error(f"    CDF:   x {missing_units[dsid][variable_name]}")
        else:
          msg = "cdawmeta.attrib.UNITS() returned None but no error"
          logger.info(f"    CDF:   x {missing_units[dsid][variable_name]}")
      else:
        if not isinstance(UNITS, list):
          # e.g., AC_H2_CRIS
          UNITS = [UNITS]

        for UNIT in UNITS:
          if UNIT not in master_units_dict:
            master_units_dict[UNIT] = []

        if len(UNITS) == 1:
          logger.info(f"    CDF:     {UNITS[0]}")
        else:
          logger.info(f"    CDF:     {UNITS}")

      SI_CONVERSION, etype, emsg = cdawmeta.attrib.SI_CONVERSION(variable)
      if SI_CONVERSION is not None:
        if UNIT not in master_si_dict:
          master_si_dict[UNIT] = []
        if SI_CONVERSION not in master_si_dict[UNIT]:
          master_si_dict[UNIT].append(SI_CONVERSION)

      if not have_spase:
        continue

      Parameters = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Parameter'])
      if variable_name == 'Time':
        continue

      if Parameters is not None and variable_name in Parameters:
        if 'Units' in Parameters[variable_name]:

          Units = Parameters[variable_name]['Units']
          if isinstance(Units, list):
            logger.error('    SPASE: NOT IMPLEMENTED - Units is a list')

          if UNITS is not None:
            if len(UNITS) > 1 and not isinstance(Units, list):
              logger.error(f'    SPASE has one unit string, CDF has {len(UNITS)} unit strings')
            if list(set(UNITS)) != UNITS:
              for UNIT in UNITS:
                master_units_dict[UNIT].append(Units)
            else:
              master_units_dict[UNITS[0]].append(Units)

          logger.info(f"    SPASE:   {Units}")
        else:
          logger.info("    SPASE:   x No Units attribute")
      else:
        logger.info("    SPASE:   x Parameter not found")

  fname_missing = os.path.join(dir_name, f'{report_name}-CDFvariables-with-missing.json')
  fname_report = os.path.join(dir_name, f'{report_name}-CDFUNITS_to_SPASEUnit-map.json')
  fname_vounits = os.path.join(dir_additions, 'Units.json')
  fname_si = os.path.join(dir_name, f'{report_name}-CDFUNITS-SI_CONVERSION-map.json')
  cdawmeta.util.write(fname_si, master_si_dict, logger=logger)

  # Write fname_missing
  n_missing = 0
  for key in missing_units.copy():
    n_missing += len(missing_units[key])
    if len(missing_units[key]) == 0:
      del missing_units[key]
  cdawmeta.util.write(fname_missing, missing_units, logger=logger)

  # Write fname_report
  n_same = 0
  n_diff = 0
  from collections import Counter
  for key in master_units_dict.copy():
    # Each key is a CDF unit; its value is a list of Units associated with
    # it found in SPASE records. Count the number of times each unit is found
    # and put in dict.
    uniques = dict(Counter(master_units_dict[key]))
    if len(uniques) == 0:
      # No SPASE unit found associated with this CDF unit
      del master_units_dict[key]
      continue
    master_units_dict[key] = uniques
    for ukey in uniques:
      if key == ukey:
        n_same += uniques[ukey]
      else:
        n_diff += uniques[ukey]
  cdawmeta.util.write(fname_report, master_units_dict, logger=logger)

  # Read and update fname_vounits
  unique_dict = {}
  for key in master_units_dict.keys():
    if key is None or (key is not None and key.strip() == ""):
      continue
    unique_dict[key.strip()] = None

  if clargs['id'] is not None:
    unique_dict_o = unique_dict.copy()

  if os.path.exists(fname_vounits):

    unique_dict_last = cdawmeta.util.read(fname_vounits, logger=logger)
    diff = set(unique_dict.keys()) - set(unique_dict_last.keys())
    if len(diff) > 0:
      logger.warning(f"Warning: New units in CDF metadata: {diff}")
    unique_dict = {**unique_dict, **unique_dict_last}
    unique_dict = cdawmeta.util.sort_dict(unique_dict)

  cdawmeta.util.write(fname_vounits, unique_dict, logger=logger)

  dir_path = os.path.dirname(os.path.realpath(__file__))
  units_md = os.path.join(dir_path, 'units.md')
  logger.info(f"Copying {units_md} to {dir_name}")
  shutil.copy2(units_md, dir_name)

  # Print and log summary
  coda = ""
  if clargs['id'] is not None:
    logger.info(f"{len(unique_dict_o)} unique units for id='{clargs['id']}'")
    coda = " (inluding previously found units)"
  logger.info(f"{len(unique_dict)} unique units{coda}")
  msg = "with missing a UNITS attribute or an all-whitespace UNITS value"
  logger.info(f"{n_missing} variables of VAR_TYPE = 'data' {msg}")
  logger.info(f"{n_same} CDF UNITS are the same as the SPASE Unit")
  logger.info(f"{n_diff} CDF UNITS differ from the SPASE Unit (only differences in value, not semantics, checked)")
