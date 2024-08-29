# TODO: Give example of using sql files.

import yaml

import cdawmeta

name = 'FORMAT'
name = 'sos'

args = cdawmeta.cli('query.py')
args['embed_data'] = True
logger = None

def query(name):
  logger = cdawmeta.logger('query')


  if name == 'sos':

    suppress = {
                "Spase": [
                  "AccessInformation",
                  "Parameter",
                  "ResourceHeader/PriorID",
                  "ResourceHeader/InformationURL",
                  "ResourceHeader/RevisionHistory",
                  "ResourceHeader/Contact"
                  ]
                }
    #suppress = None

    meta = cdawmeta.metadata(**args)

    for id in meta.keys():

      print(f"\n-----{id}-----")

      logger.info(yaml.dump({f"allxml/{id}": meta[id]['allxml']}))


      if "master" not in meta[id]:
        logger.error(f"  Error - No master.")
      else:
        master = meta[id]["master"]['data']
        if 'CDFglobalAttributes' in master:
          print(yaml.dump({"master/CDFglobalAttributes": meta[id]['master']['data']['CDFglobalAttributes']}))
        else:
          logger.error(f"  Error - No CDFglobalAttributes")


      if "spase" not in meta[id]:
        print(f"  Error - No spase node. Was --no_spase used?")
        continue
      if "error" in meta[id]["spase"]:
        print(f"spase: {meta[id]['spase']['error']}")
      else:
        spase = meta[id]["spase"]['data']
        if 'Spase' not in spase:
          print(f"spase: No Spase node in {spase}")
        if 'NumericalData' not in spase['Spase']:
          print(f"spase: No Spase/NumericalData.")
          continue
        resource_id = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'ResourceID'])
        numerical_data = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData'])

        suppressed = []
        if suppress is not None and 'Spase' in suppress:
          for path in suppress['Spase']:
            suppressed.append(path)
            cdawmeta.util.rm_path(numerical_data, path.split('/'))

        print(yaml.dump({resource_id: numerical_data}))
        if len(suppressed) > 0:
          suppressed = "\n    ".join(suppressed)
          print(f"  *Suppressed*:\n    {suppressed}")


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
