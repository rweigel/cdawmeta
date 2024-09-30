import os
import cdawmeta

logger = None

def f2c_specifier(report_name, dir_name, clargs):

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
    master = cdawmeta.restructure.master(master, logger=logger)
    if master is None or "CDFVariables" not in master:
      continue
    variables = master['CDFVariables']
    for variable_name, variable in variables.items():

      if "VarAttributes" not in variable or "VarDescription" not in variable:
        continue

      FORMAT_given, etype, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables, c_specifier=False)
      if FORMAT_given is None:
        continue
      if etype is not None:
        logger.error(f"{id}\n{emsg}")

      FORMAT_computed, etype, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables)
      if FORMAT_given is None:
        continue
      if etype is not None:
        logger.error(f"{id}\n{emsg}")

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

def units(report_name, dir_name, clargs):

  master_units_dict = {}

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
    master = cdawmeta.restructure.master(master, logger=logger)

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
            import pdb; pdb.set_trace()

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
          logger.info("    SPASE: x No Units attribute")
      else:
        logger.info("    SPASE: x Parameter not found")

  fname_missing = os.path.join(dir_name, f'{report_name}-CDFvariables-with-missing.json')
  fname_report = os.path.join(dir_name, f'{report_name}-CDFUNITS_to_SPASEUnit-map.json')
  fname_vounits = os.path.join(dir_name, '../', 'Units.json')

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
    for ukey in uniques:
      if key == ukey:
        n_same += uniques[ukey]
      else:
        n_diff += uniques[ukey]
    master_units_dict[key] = uniques
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

def hpde_io(report_name, dir_name, clargs):

  dir_name = os.path.join(dir_name, "..")

  meta_type = 'spase_hpde_io'
  meta = cdawmeta.metadata(id=clargs['id'], meta_type=meta_type, update=False)
  dsids = meta.keys()

  attributes = {
    'ObservedRegion': {},
    'InstrumentID': {},
    'MeasurementType': {},
    'DOI': {},
    'InformationURL': {},
  }

  n_found = attributes.copy()
  ObservedRegion = {}
  for attribute in attributes.keys():
    n_spase = 0
    n_found[attribute] = 0

    path = ['Spase', 'NumericalData']
    if attribute in ['DOI', 'InformationURL']:
      path = [*path, 'ResourceHeader', attribute]
    else:
      path = [*path, attribute]

    for dsid_spase in meta.keys():
      spase = cdawmeta.util.get_path(meta[dsid_spase], [meta_type, 'data'])
      if spase is None:
        #logger.error(f"No SPASE for {dsid_spase}")
        continue
      n_spase += 1

      attributes[attribute][dsid_spase] = None
      value = cdawmeta.util.get_path(spase, path)

      if value is not None:
        if attribute != 'ObservedRegion':
          attributes[attribute][dsid_spase] = value
        else:
          if not isinstance(value, list):
            value = [value]
          sc_id = dsid_spase.split('_')[0]
          if sc_id not in ObservedRegion:
            ObservedRegion[sc_id] = value
            logger.info(f"  {dsid_spase}: Found first ObservedRegion for s/c ID = {sc_id}: {value}")
          elif sorted(ObservedRegion[sc_id]) != sorted(value):
              logger.error(f"  {dsid_spase}: ObservedRegion for this ID differs from first found value s/c ID = {sc_id}")
              logger.error(f"  {dsid_spase}: First value = {sorted(ObservedRegion[sc_id])}")
              logger.error(f"  {dsid_spase}: This value  = {sorted(value)}")
              logger.error("  Combining values.")
              ObservedRegion[sc_id] = list(set(ObservedRegion[sc_id]) | set(value))
        n_found[attribute] += 1

  attributes['ObservedRegion'] = ObservedRegion

  URLs = {}
  for dsid in attributes['InformationURL'].keys():
    InformationURLs = attributes['InformationURL'][dsid]
    if InformationURLs is None:
      continue
    if not isinstance(InformationURLs, list):
      InformationURLs = [InformationURLs]
    for InformationURL in InformationURLs:
      if InformationURL['URL'] not in URLs:
        if dsid.startswith("BAR_"):
          URLs[InformationURL['URL']] = {"InformationURL": InformationURL, "ids": ["^BAR"]}
        else:
          URLs[InformationURL['URL']] = {"InformationURL": InformationURL, "ids": [dsid]}
      else:
        if not dsid.startswith("BAR_"):
          URLs[InformationURL['URL']]["ids"].append(dsid)

  msg = f"Number of unique URLs in InformationURL: {len(URLs)}"
  fname = f'{dir_name}/InformationURL.json'
  logger.info(f"  Writing {fname}")
  cdawmeta.util.write(fname, URLs)
  del attributes['InformationURL']

  msg = f"Number of CDAWeb datasets:  {len(dsids)}"
  logger.info(msg)
  msg = f"Number found in hpde.io:    {n_spase}"
  logger.info(msg)

  for key in attributes.keys():
    logger.info(f"Found {key} in {n_found[key]} of {n_spase} SPASE records.")
    fname = f'{dir_name}/{key}.json'
    logger.info(f"  Writing {fname}")
    cdawmeta.util.write(fname, attributes[key])

  ResourceIDs_file = os.path.join(dir_name, "ResourceID.json")

  dsids_spase = list(meta.keys())
  ResourceIDs = {}
  n_found = 0
  for dsid in dsids:
    ResourceIDs[dsid] = None
    if dsid in dsids_spase:
      p = [meta_type, 'data', 'Spase', 'NumericalData', 'ResourceID']
      ResourceID = cdawmeta.util.get_path(meta[dsid], p)
      ResourceIDs[dsid] = ResourceID
    #logger.info(f"  {dsid}: {ResourceID}")
    n_found += 1

  logger.info(f"Writing {ResourceIDs_file}")
  cdawmeta.util.write(ResourceIDs_file, ResourceIDs)

def cadence(report_name, dir_name, clargs):
  clargs = {**clargs, 'meta_type': 'cadence'}
  meta = cdawmeta.metadata(**clargs)
  for id in meta.keys():

    logger.info(f"{id}:")
    print(meta[id]['cadence'])
    depend_0_obj = cdawmeta.util.get_path(meta[id],['cadence', 'data'])
    if depend_0_obj is None:
      cadence_error = cdawmeta.util.get_path(meta[id],['cadence', 'error'])
      if cadence_error is not None:
        logger.error(f"  {cadence_error}")
      continue

    for depend_0 in depend_0_obj:
      depend_0_info = depend_0_obj[depend_0]
      logger.info(f"  {depend_0}")
      if 'error' in depend_0_info:
        logger.error(f"    {depend_0_info['error']}")
        continue
      if 'counts' in depend_0_info:
        for count in depend_0_info['counts']:
          logger.info(f"    {count}")

cldefs = cdawmeta.cli('report.py', defs=True)
report_names = cldefs['report-name']['choices']

clargs = cdawmeta.cli('report.py')
report_name = clargs['report_name']

del clargs['report_name']

dir_name = 'cdawmeta-additions/reports'

if report_name is not None:
  report_names = [report_name]

for report_name in report_names:
  if report_name == 'cadence':
    logger.error("Skipping 'cadence' report because of issue.")
    continue
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=dir_name)
  report_function = locals()[report_name]
  report_function(report_name, os.path.join(cdawmeta.DATA_DIR, dir_name), clargs)
