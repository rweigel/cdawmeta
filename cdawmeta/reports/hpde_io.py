import os
import sys

import cdawmeta

def hpde_io(clargs):

  # 2024-10-08 1c51afbe7d13fff296a0590f0771a4cd86acd71d
  # 2023-01-01 63ab552190803d982d3f958c567313c02e1a660d
  # 2022-02-12 a57646d181a669c3285e75efa256891720818de8

  dir_additions = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-spase')
  out_dir = os.path.join('cdawmeta-spase', 'log')
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  meta_type = 'spase_hpde_io'
  meta = cdawmeta.metadata(meta_type=meta_type, **clargs, logger_=logger)
  dsids = meta.keys() # CDAWeb dataset IDs

  attributes = {
    'ObservedRegion': {},
    'InstrumentID': {},
    'MeasurementType': {},
    'DOI': {},
    'InformationURL': {},
  }

  n_found = attributes.copy()
  ObservedRegion = {}
  ObservedRegionIDMap = {}
  for attribute in attributes.keys():
    n_spase = 0
    n_found[attribute] = 0

    path = ['Spase', 'NumericalData']
    if attribute in ['DOI', 'InformationURL']:
      path = [*path, 'ResourceHeader', attribute]
    else:
      path = [*path, attribute]

    for dsid_spase in meta.keys():
      path_rid = [meta_type, 'data', 'Spase', 'NumericalData', 'ResourceID']
      ResourceID = cdawmeta.util.get_path(meta[dsid_spase], path_rid)
      spase = cdawmeta.util.get_path(meta[dsid_spase], [meta_type, 'data'])
      if spase is None:
        #logger.error(f"No SPASE for {dsid_spase}")
        continue
      n_spase += 1

      attributes[attribute][dsid_spase] = None
      value = cdawmeta.util.get_path(spase, path)
      first = None
      if value is not None:
        if attribute != 'ObservedRegion':
          attributes[attribute][dsid_spase] = value
        else:
          if not isinstance(value, list):
            value = [value]
          sc_id = dsid_spase.split('_')[0]
          if sc_id not in ObservedRegion:
            first = dsid_spase
            ObservedRegion[sc_id] = value
            ObservedRegionIDMap[sc_id] = []
            logger.info(f"  {dsid_spase} ({ResourceID}):")
            logger.info(f"     Found first ObservedRegion in {first} for s/c ID = {sc_id}: {value}")
          elif sorted(ObservedRegion[sc_id]) != sorted(value):
            if not _merge_observed_regions(dsid_spase):
              logger.info(f"  {dsid_spase} ({ResourceID}):")
              logger.info(f"    Not merging ObservedRegion for s/c ID = {dsid_spase}")
            else:
              logger.error(f"  {dsid_spase} ({ResourceID}):")
              logger.info(f"    ObservedRegion for this ID differs from first found value s/c ID = {sc_id}")
              logger.error(f"      First value = {sorted(ObservedRegion[sc_id])}")
              logger.error(f"      This value  = {sorted(value)}")
              logger.error("       Combining values.")
              ObservedRegion[sc_id] = list(set(ObservedRegion[sc_id]) | set(value))
          ObservedRegionIDMap[sc_id].append(dsid_spase)
        n_found[attribute] += 1

  attributes['ObservedRegion'] = ObservedRegion

  n_found['ObservedRegionIDMap'] = n_found['ObservedRegion']
  attributes['ObservedRegionIDMap'] = ObservedRegionIDMap

  ResourceIDs = {}
  n_parameters = 0
  n_found['ResourceID'] = 0
  for dsid in dsids:
    p = [meta_type, 'data', 'Spase', 'NumericalData', 'ResourceID']
    ResourceID = cdawmeta.util.get_path(meta[dsid], p)
    if ResourceID is not None:
      n_found['ResourceID'] += 1

    p = [meta_type, 'data', 'Spase', 'NumericalData', 'Parameter']
    Parameters = cdawmeta.util.get_path(meta[dsid], p)
    if Parameters is not None:
      n_parameters += 1

    ResourceIDs[dsid] = ResourceID
  attributes['ResourceID'] = ResourceIDs

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
          URLs[InformationURL['URL']] = {"InformationURL": InformationURL, "_cdaweb_ids": ["^BAR"]}
        else:
          URLs[InformationURL['URL']] = {"InformationURL": InformationURL, "_cdaweb_ids": [dsid]}
      else:
        if not dsid.startswith("BAR_"):
          URLs[InformationURL['URL']]["_cdaweb_ids"].append(dsid)
  attributes['InformationURL'] = URLs

  for key in attributes.keys():
    fname = f'{dir_additions}/{key}.json'
    logger.info(f"Writing {fname}")
    cdawmeta.util.write(fname, attributes[key])

  msgs = []
  msgs.append(f"Number of CDAWeb datasets:  {len(dsids)}")
  logger.info(msgs[-1])
  msgs.append(f"Number found in hpde.io:    {n_spase}")
  logger.info(msgs[-1])

  msgs.append(f"Number of SPASE records with a Parameter node: {n_parameters}")
  logger.info(msgs[-1])

  for key in attributes.keys():
    msgs.append(f"Found {key} in {n_found[key]} of {n_spase} SPASE records.")
    logger.info(msgs[-1])

  cdawmeta.util.write(f'{dir_additions}/statistics.txt', "\n".join(msgs))

def _merge_observed_regions(dsid):
  # TODO: Use start/stop to determine possibility that merge should not be
  # done. This would have caught the VOYAGER{1,2}_PLS cases.
  if dsid.startswith("VOYAGER1_PLS"):
    return False
  if dsid.startswith("VOYAGER2_PLS"):
    return False
  return True
