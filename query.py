# TODO: Give example of using sql files.

import cdawmeta

name = 'FORMAT'
name = 'sos'

args = cdawmeta.cli('query.py')
args['embed_data'] = True
logger = None

def query(name):
  logger = cdawmeta.logger('query')
  if name == 'sos':
    meta = cdawmeta.metadata(**args)
    formats = []
    for id in meta.keys():
      logger.info(cdawmeta.util.print_dict(meta[id]['allxml']))

      if "master" not in meta[id]:
        logger.error(f"  {id}: Error - No master.")
        continue

      master = meta[id]["master"]['data']
      if 'CDFglobalAttributes' in master:
        logger.info(cdawmeta.util.print_dict(meta[id]['master']['data']['CDFglobalAttributes']))
      else:
        logger.error(f"  {id}: Error - No CDFglobalAttributes")

      if 'CDFVariables' in master:
        continue

      logger.info(len(master['CDFVariables'].keys()))

  if name == 'f2c_specifier':
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
        if FORMAT_given is None: continue
        if emsg is not None: print(f"{id}\n{emsg}")

        FORMAT_computed, emsg = cdawmeta.attrib.FORMAT(id, variable_name, variables)
        if FORMAT_given is None: continue
        if emsg is not None: print(f"{id}\n{emsg}")

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
    cdawmeta.util.write(os.path.join(cdawmeta.DATA_DIR, 'queries', "FORMAT_parsed.txt"), lines)

  if name == 'FORMAT':
    # Print all unique FORMAT values using table.
    body_file = "data/table/cdaweb.variable.body.json"
    header_file = "data/table/cdaweb.variable.head.json"
    import json
    with open(header_file) as f:
      print("Reading " + header_file)
      header = json.load(f)
    with open(body_file) as f:
      print("Reading " + body_file)
      body = json.load(f)

    print("Unique FORMAT values:")
    idx = header.index("FORMAT")
    formats = []
    for row in body:
      formats.append(row[idx])
    print(set(formats))

query(name)
