import os
import sys

import cdawmeta

def hpde_io(clargs):

  out_dir = 'reports'
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  dir_additions = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-spase')

  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}')

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


  msg = f"Number of CDAWeb datasets:  {len(dsids)}"
  logger.info(msg)
  msg = f"Number found in hpde.io:    {n_spase}"
  logger.info(msg)

  msg = f"Number of unique URLs in InformationURL: {len(URLs)}"
  fname = f'{dir_additions}/InformationURL.json'
  logger.info(f"  Writing {fname}")
  cdawmeta.util.write(fname, URLs)
  del attributes['InformationURL']

  for key in attributes.keys():
    logger.info(f"Found {key} in {n_found[key]} of {n_spase} SPASE records.")
    fname = f'{dir_additions}/{key}.json'
    logger.info(f"  Writing {fname}")
    cdawmeta.util.write(fname, attributes[key])

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

  ResourceIDs_file = os.path.join(dir_additions, "ResourceID.json")
  logger.info(f"Writing {ResourceIDs_file}")
  cdawmeta.util.write(ResourceIDs_file, ResourceIDs)
