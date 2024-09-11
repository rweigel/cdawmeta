# TODO: Give example of using sql files.
import cdawmeta

name = 'f2c_specifier'
name = 'units'
#name = 'hpde.io-ids'

logger = None

def query(name):

  args = cdawmeta.cli('query.py')
  args['embed_data'] = True

  logger = cdawmeta.logger(f'query-{name}', dir_name='query')

  if name == 'hpde.io-ids':
    import os

    # TODO: Count number of CDAWeb datasets with spase_DatasetResourceIDs

    spase_resource_ids_with_cdaweb_dataset_id = {}
    spase_resource_ids_no_cdaweb_dataset_id = []
    files_spase = []

    import glob
    files = glob.glob("../hpde.io/**/NumericalData/**/*.json", recursive=True)
    if True:
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
        logger.error("  Error - No ResourceID")
        continue
      logger.info("\n" + ResourceID)

      # Flattens AccessInformation so is a list of objects, each with one AccessURL.
      data = cdawmeta.restructure.spase(data, logger=logger)
      AccessInformation = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'AccessInformation'])

      if AccessInformation is None:
        logger.error("  Error - No AccessInformation")
        continue

      s = "s" if len(AccessInformation) > 1 else ""
      logger.info(f"  {len(AccessInformation)} Repository object{s} in AccessInformation")

      found = False
      ProductKeyCDAWeb = None
      for ridx, Repository in enumerate(AccessInformation):
        AccessURL = Repository['AccessURL']
        if AccessURL is not None:
          if 'Name' not in AccessURL:
            logger.error(f"  Error - No Name in {AccessURL}")
            continue
          if 'URL' not in AccessURL:
            logger.error(f"  Error - No Name in {AccessURL}")
            continue
          logger.info(f"    {ridx+1}. {AccessURL['Name']}: {AccessURL['URL']}")
          if AccessURL['Name'] == 'CDAWeb':
            if found:
              logger.error("    Error - Repository with Name = 'CDAWeb' already found")
              continue
            if 'ProductKey' in Repository['AccessURL']:
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

    fname = os.path.join(cdawmeta.DATA_DIR, 'query', f"{name}.csv")
    cdawmeta.util.write(fname, lines, logger=logger)

  if name == 'units':
    meta = cdawmeta.metadata(**args)
    for dsid in meta.keys():
      logger.info(f"\n-----{dsid}-----")

      spase = cdawmeta.util.get_path(meta[dsid], ['spase', 'data'])
      if spase is None:
        logger.info("  SPASE: x No SPASE node")
        continue

      hapi = cdawmeta.util.get_path(meta[dsid], ['hapi', 'data'])
      if hapi is None:
        logger.info("  HAPI: x No HAPI node")
        continue

      logger.info(f"  CDF:   {meta[dsid]['master']['request']['url']}")
      logger.info(f"  SPASE: {meta[dsid]['spase']['request']['url']}")

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
        if 'units' in parameter:
          units = parameter['units']
          logger.info(f"    CDF:   '{units}'")

        Parameters = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Parameter'])
        if name == 'Time':
          continue

        if Parameters is not None and name in Parameters:
          if 'Units' in Parameters[name]:
            logger.info(f"    SPASE: '{Parameters[name]['Units']}'")
          else:
            logger.info("    SPASE: x No Units attribute")
        else:
          logger.info("    SPASE: x Parameter not found")

  if name == 'f2c_specifier':
    # TODO: This metadata is avaialable in x_keys in HAPI metadata.
    #       Rewrite to use it.

    # Compare given FORMAT/FORM_PTR with computed FORMAT specifier.
    meta = cdawmeta.metadata(**args)

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

    import os
    cdawmeta.util.write(os.path.join(cdawmeta.DATA_DIR, 'query', "FORMAT_parsed.txt"), lines)

query(name)
