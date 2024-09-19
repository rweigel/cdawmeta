import os
import cdawmeta

logger = None

def f2c_specifier(report_name, dir_name, clargs):

  # TODO: This metadata is available in x_keys in HAPI metadata.
  #       Rewrite to use it.

  # Compare given FORMAT/FORM_PTR with computed FORMAT specifier.
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

      FORMAT_given, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables, c_specifier=False)
      if FORMAT_given is None:
        continue
      if emsg is not None:
        logger.error(f"{id}\n{emsg}")

      FORMAT_computed, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables)
      if FORMAT_given is None:
        continue
      if emsg is not None:
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

      #if VAR_TYPE not in ['data', 'support_data']:
      if VAR_TYPE != 'data':
        continue

      logger.info(variable_name)

      UNITS, emsg = cdawmeta.attrib.UNITS(dsid, variable_name, variables)
      if emsg is not None:
        logger.error("    " + emsg)

      if not isinstance(UNITS, list):
        UNITS = [UNITS]

      missing_units[dsid][variable_name] = []
      for UNIT in UNITS:
        if UNIT is None:
          missing_units[dsid][variable_name].append("cdawmeta.attrib.UNITS() returned None")
        elif UNIT.strip() == "":
          missing_units[dsid][variable_name].append("UNITS.strip() = ''")
        elif UNIT not in master_units_dict:
          master_units_dict[UNIT] = []

      if len(missing_units[dsid][variable_name]) > 0:
        UNITS = None
        if len(missing_units[dsid][variable_name]) == 1:
          missing_units[dsid][variable_name] = missing_units[dsid][variable_name][0]
        logger.error(f"    CDF:   x {missing_units[dsid][variable_name]}")
      else:
        del missing_units[dsid][variable_name]
        if len(UNITS) == 1:
          logger.info(f"    CDF:   '{UNITS[0]}'")
        else:
          logger.info(f"    CDF:    {UNITS}")

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
            if list(set(UNITS)) != UNITS:
              for UNIT in UNITS:
                master_units_dict[UNIT].append(Units)
            else:
              master_units_dict[UNITS[0]].append(Units)

          logger.info(f"    SPASE: '{Units}'")
        else:
          logger.info("    SPASE: x No Units attribute")
      else:
        logger.info("    SPASE: x Parameter not found")

  fname_missing = os.path.join(dir_name, f'{report_name}-CDFvariables-with-missing.json')
  fname_report = os.path.join(dir_name, f'{report_name}-CDFUNITS_to_SPASEUnit-map.json')
  fname_vounits = os.path.join(dir_name, '../', 'CDFUNITS_to_VOUNITS-map.csv')

  # Write fname_missing
  n_missing = 0
  for key in missing_units.copy():
    n_missing += len(missing_units[key])
    if len(missing_units[key]) == 0:
      del missing_units[key]
  cdawmeta.util.write(fname_missing, missing_units, logger=logger)

  # Write fname_report
  from collections import Counter
  for key in master_units_dict:
    uniques = dict(Counter(master_units_dict[key]))
    master_units_dict[key] = uniques
  cdawmeta.util.write(fname_report, master_units_dict, logger=logger)

  # Read and update fname_vounits
  unique_dict = {}
  for key in master_units_dict.keys():
    if key is None or (key is not None and key.strip() == ""):
      continue
    unique_dict[key.strip()] = "?"

  if clargs['id'] is not None:
    unique_dict_o = unique_dict.copy()

  if os.path.exists(fname_vounits):

    unique_list_last = cdawmeta.util.read(fname_vounits, logger=logger)
    unique_dict_last = {}
    for _, row in enumerate(unique_list_last[1:]):
      if row[0] is None or (row[0] is not None and row[0].strip() == ""):
        continue
      unique_dict_last[row[0].strip()] = row[1].strip()

    diff = set(unique_dict.keys()) - set(unique_dict_last.keys())
    if len(diff) > 0:
      logger.warning(f"Warning: New units in CDF metadata: {diff}")

    unique_dict = {**unique_dict, **unique_dict_last}

  header = [["CDFunits", "VOUNIT"]]
  master_units_list = header
  for key, val in unique_dict.items():
    master_units_list.append([key, val])
  cdawmeta.util.write(fname_vounits, master_units_list, logger=logger)

  # Print and log summary
  coda = ""
  if clargs['id'] is not None:
    logger.info(f"{len(unique_dict_o)} unique units for id='{clargs['id']}'")
    coda = " (inluding previously found units)"
  logger.info(f"{len(unique_dict)} unique units{coda}")
  msg = "with missing a UNITS attribute or an all-whitespace UNITS value"
  logger.info(f"{n_missing} variables of VAR_TYPE = 'data' {msg}")

def hpde_io_ids(report_name, dir_name, clargs):

  non_spdf_urls = {}
  # TODO: Count number of CDAWeb datasets with spase_DatasetResourceIDs

  spase_resource_ids_with_cdaweb_dataset_id = {}
  spase_resource_ids_no_cdaweb_dataset_id = []
  files_spase = []

  import glob
  files = glob.glob("../hpde.io/**/NumericalData/**/*.json", recursive=True)
  logger.info(f"{len(files)} Numerical Data files before removing Deprecated")
  for file in files:
    if 'Deprecated' in file:
      del files[files.index(file)]
  logger.info(f"{len(files)} Numerical Data files after removing Deprecated")

  for file in files:

    files_spase.append(file)

    data = cdawmeta.util.read(file)

    ResourceID = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'ResourceID'])
    if ResourceID is None:
      logger.error(f"  Error - No ResourceID in {file}")
      continue

    hpde_url = f'{ResourceID.replace("spase://", "http://hpde.io/")}'
    logger.info(f'\n\n{hpde_url}.json')

    # Flattens AccessInformation so is a list of objects, each with one AccessURL.
    data = cdawmeta.restructure.spase(data, logger=logger)
    AccessInformation = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'AccessInformation'])

    if AccessInformation is None:
      logger.error(f"  Error - No AccessInformation in {file}")
      continue

    s = "s" if len(AccessInformation) > 1 else ""
    logger.info(f"  {len(AccessInformation)} Repository object{s} in AccessInformation")

    found = False
    ProductKeyCDAWeb = None
    for ridx, Repository in enumerate(AccessInformation):
      AccessURL = Repository['AccessURL']
      if AccessURL is not None:
        Name = AccessURL.get('Name', None)
        if Name is None:
          logger.warning(f"  Warning - No Name in {AccessURL}")

        URL = AccessURL.get('URL', None)
        if URL is None:
          logger.error(f"  Error - No URL in {AccessURL} for {hpde_url}")
          continue

        if 'spdf' not in URL and 'cdaweb' not in URL:
          non_spdf_urls[URL.strip()] = None

        logger.info(f"    {ridx+1}. {Name}: {URL}")

        if Name is None or URL is None:
          continue

        if Name == 'CDAWeb':
          if found:
            logger.error(f"      Error - Duplicate AccessURL/Name = 'CDAWeb' in {hpde_url}")
          else:
            if 'ProductKey' in Repository['AccessURL']:
              found = True
              ProductKeyCDAWeb = Repository['AccessURL']['ProductKey']
              spase_resource_ids_with_cdaweb_dataset_id[ProductKeyCDAWeb] = ResourceID

    if ProductKeyCDAWeb is None:
      spase_resource_ids_no_cdaweb_dataset_id.append(ResourceID)
      logger.info("  x Did not find CDAWeb ProductKey in any Repository")
    else:
      logger.info(f"  + Found CDAWeb ProductKey: {ProductKeyCDAWeb}")


  cdaweb_dataset_ids = cdawmeta.ids()

  logger.info("\n")
  logger.info(f"{len(spase_resource_ids_no_cdaweb_dataset_id)} SPASE Records with no CDAWeb ProductKey")
  spase_resource_ids_no_cdaweb_dataset_id.sort()
  for rid in spase_resource_ids_no_cdaweb_dataset_id:
    logger.info(f"  {rid}")

  logger.info("\n")
  logger.info(f"{len(spase_resource_ids_with_cdaweb_dataset_id)} SPASE Records with CDAWeb ProductKey")
  spase_resource_ids_with_cdaweb_dataset_id = cdawmeta.util.sort_dict(spase_resource_ids_with_cdaweb_dataset_id)
  for key, val in spase_resource_ids_with_cdaweb_dataset_id.items():
    logger.info(f"  {val}: {key}")

  logger.info("\n")
  logger.info("----- Summary -----")
  logger.info(f"Known number of CDAWeb ProductKeys (datasets): {len(cdaweb_dataset_ids)}")
  logger.info(f"Number of hpde.io/NASA/NumericalData records:  {len(files_spase)}")
  logger.info(f"Number where CDAWeb ProductKey found:          {len(spase_resource_ids_with_cdaweb_dataset_id)}")

  n = 0
  for cdaweb_dataset_id in cdaweb_dataset_ids:
    if cdaweb_dataset_id not in spase_resource_ids_with_cdaweb_dataset_id:
      n += 1

  logger.info("\nCDAWeb dataset ID, hpde.io ResourceID")
  lines = []
  for cdaweb_dataset_id in cdaweb_dataset_ids:
    line = [cdaweb_dataset_id, "???"]
    if cdaweb_dataset_id in spase_resource_ids_with_cdaweb_dataset_id:
      line = [cdaweb_dataset_id, spase_resource_ids_with_cdaweb_dataset_id[cdaweb_dataset_id]]
    lines.append(line)
    logger.info(f"  {line[0]}: {line[1]}")

  fname = os.path.join(dir_name, "ids.csv")
  cdawmeta.util.write(fname, lines, logger=logger)

  logger.info("\n")
  logger.info(f"{len(non_spdf_urls)} non-spdf/cdaweb URLs")

  fname = os.path.join(dir_name, 'report', f"{report_name}-urls.txt")
  urls = "\n".join(list(non_spdf_urls.keys()))
  cdawmeta.util.write(fname, urls, logger=logger)


cldefs = cdawmeta.cli('report.py', defs=True)
report_names = cldefs['report-name']['choices']

clargs = cdawmeta.cli('report.py')
report_name = clargs['report_name']

clargs['embed_data'] = True
del clargs['report_name']

dir_name = 'cdawmeta-additions/reports'

if report_name is None:
  if report_name not in report_names:
    exit(f"Error: report_name = '{report_name}' not in {report_names}")
else:
  report_names = [report_name]

for report_name in report_names:
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=dir_name)
  report_function = locals()[report_name]
  report_function(report_name, os.path.join(cdawmeta.DATA_DIR, dir_name), clargs)