import os
import cdawmeta

name = 'f2c_specifier'
name = 'units'
name = 'hpde.io-ids'

logger = None

def f2c_specifier(query_name, dir_name, clargs):

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
        print(f"{id}\n{emsg}")

      FORMAT_computed, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables)
      if FORMAT_given is None:
        continue
      if emsg is not None:
        print(f"{id}\n{emsg}")

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
    print(line)
    lines += line + "\n"

  fname = os.path.join(dir_name, 'query', f"{query_name}.txt")
  cdawmeta.util.write(fname, lines)

def units(query_name, dir_name, clargs):

  master_units_dict = {}

  meta = cdawmeta.metadata(**clargs)
  for dsid in meta.keys():
    logger.info(f"\n-----{dsid}-----")

    spase = cdawmeta.util.get_path(meta[dsid], ['spase', 'data'])
    spase = cdawmeta.restructure.spase(spase, logger=logger)

    if spase is None:
      logger.info("  SPASE: x No SPASE node")
      continue

    hapi = cdawmeta.util.get_path(meta[dsid], ['hapi', 'data'])
    if hapi is None:
      logger.info("  HAPI: x No HAPI node")
      continue

    logger.info(f"  CDF:   {meta[dsid]['master']['request']['url']}")
    logger.info(f"  SPASE: {meta[dsid]['spase']['request']['url']}")

    # TODO: Draw units from CDF Master instead of HAPI metadata.
    #       Code for units and f2c_specifier queries should be merged.
    if isinstance(hapi, list):
      parameters = {}
      for _, ds in enumerate(hapi):
        logger.info(ds['info']['parameters'])
        parameters = [*parameters, *ds['info']['parameters']]
    else:
      parameters = hapi['info']['parameters']

    for parameter in parameters:

      name = parameter['name']
      description = ""
      if 'description' in parameter:
        description = " - '" + parameter['description'] + "'"
      logger.info(f"  {name}{description}")

      units = None
      if 'units' in parameter:
        units = parameter['units']
        if units not in master_units_dict:
          master_units_dict[units] = []
        logger.info(f"    CDF:   '{units}'")

      Parameters = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Parameter'])
      if name == 'Time':
        continue

      if Parameters is not None and name in Parameters:
        if 'Units' in Parameters[name]:
          if units is not None:
            Units = Parameters[name]['Units']
            master_units_dict[units].append(Units)
          logger.info(f"    SPASE: '{Units}'")
        else:
          logger.info("    SPASE: x No Units attribute")
      else:
        logger.info("    SPASE: x Parameter not found")

  from collections import Counter
  for key in master_units_dict:
    uniques = dict(Counter(master_units_dict[key]))
    master_units_dict[key] = uniques

  doc = "Each keys is a unique CDF master unit found across all variables. "
  doc += "The value is an object with keys of associated SPASE Units and "
  doc += "values of counts."

  fname = os.path.join(dir_name, 'query', f'{query_name}.md')
  cdawmeta.util.write(fname, doc, logger=logger)
  fname = os.path.join(dir_name, 'query', f'{query_name}.json')
  cdawmeta.util.write(fname, master_units_dict, logger=logger)

  master_units_list = [["CDFunits", "VOUNIT"]]
  for key in master_units_dict.keys():
    if key.strip() == "":
      continue
    master_units_list.append([key, '?'])

  fname = os.path.join(dir_name, 'CDFUNITS_to_VOUNITS.csv')
  if os.path.exists(fname):
    master_units_list_last = cdawmeta.util.read(fname, logger=logger)
    for _, row in enumerate(master_units_list.copy()[1:]):
      if row[0] not in master_units_dict:
        logger.info(f"New CDF Master unit found: {row[0]}. Adding to list.")
        master_units_list_last.append([row[0], "?"])
    cdawmeta.util.write(fname, master_units_list_last, logger=logger)
  else:
    cdawmeta.util.write(fname, master_units_list, logger=logger)

def hpde_io_ids(query_name, dir_name, clargs):

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
    print(f"  {val}: {key}")

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

  fname = os.path.join(dir_name, 'query', f"{query_name}-map.csv")
  cdawmeta.util.write(fname, lines, logger=logger)

  logger.info("\n")
  logger.info(f"{len(non_spdf_urls)} non-spdf/cdaweb URLs")

  fname = os.path.join(dir_name, 'query', f"{query_name}-urls.txt")
  urls = "\n".join(list(non_spdf_urls.keys()))
  cdawmeta.util.write(fname, urls, logger=logger)


clargs = cdawmeta.cli('query.py')
clargs['embed_data'] = True

query_name = f'query-{name}'
dir_name = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-additions')

logger = cdawmeta.logger(name=query_name, dir_name=dir_name)

if name == 'units':
  units(query_name, dir_name, clargs)

if name == 'f2c_specifier':
  f2c_specifier(query_name, dir_name, clargs)

if name == 'hpde.io-ids':
  if clargs['id'] is not None:
    exit("Error: --id given on command line, but not supported for hpde.io-ids query.")
  hpde_io_ids(query_name, dir_name, clargs)
