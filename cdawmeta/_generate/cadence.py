import os
from datetime import datetime
from collections import Counter

import numpy
import cdflib
from timedelta_isoformat import timedelta

import cdawmeta

dependencies = ['orig_data', 'master']

# Special cases: C1_CP_EFW_L3_E3D_INERT, THA_L2_ESA, PSP_FLD_L3_RFS_HFR

def cadence(metadatum, logger):

  use_cache = True
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to use use_cache=update and have
  # read_file() handle headers to determine if content has change and a
  # re-download is needed.

  id = metadatum['id']

  orig_data, master, emsg = _extract_and_check_metadata(id, metadatum, logger)
  if emsg is not None:
    return {"error": emsg}

  url = orig_data['FileDescription'][0]['Name']

  # FORMOSAT5_AIP_IDN
  if url.endswith('.nc'):
    # Use HAPI client instead (CDAWeb service could be used, but can be up to ~5x slower).
    emsg = f"{id}: Computing cadence for file type '.nc' is not implemented"
    cdawmeta.error("cadence", id, None, "NoImplementedNetCDF", emsg, logger)
    return {"error": emsg}

  logger.info(f"{id}: Computing cadence for {url}")
  logger.info(f"{id}: Extracting DEPEND_0 names")

  try:
    depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache)
  except Exception as e:
    emsg = f"{id}: cdawmeta.io.read_cdf_depend_0s('{url}') failed with error: {e}"
    cdawmeta.error("cadence", id, None, "FailedCDFRead", emsg, logger)
    return {"error": emsg}

  if depend_0_names is None:
    emsg = f"{id}: cdawmeta.io.read_cdf_depend_0s('{url}') failed."
    cdawmeta.error("cadence", id, None, "NoDEPEND_0s", emsg, logger)
    return {"error": emsg}

  if len(depend_0_names) > 1:
    logger.info(f"{id}: {len(depend_0_names)} DEPEND_0s: {depend_0_names}")
  else:
    logger.info(f"{id}: {len(depend_0_names)} DEPEND_0: {depend_0_names}")

  depend_0_counts = {}

  for depend_0_name in depend_0_names:

    if depend_0_name not in master['CDFVariables']:
      emsg = f"{id}: '{depend_0_name}' in CDF is not in master['CDFVariables']"
      cdawmeta.error("cadence", id, None, "CDFDEPEND_0NotInCDFMaster", emsg, logger)
      depend_0_counts[depend_0_name] = {"url": url, "error": emsg}
      continue

    DataTypeMaster = master['CDFVariables'][depend_0_name]['VarDescription']['DataType']

    msg = f"  Computing cadence for {id}/DEPEND_0 = '{depend_0_name}' with DataType in Master CDF = '{DataTypeMaster}'"
    logger.info(msg)

    depend_0_counts[depend_0_name] = {"url": url, "note": "", "counts": []}

    emsg = _check_data_types(id, master, depend_0_name, logger)

    if emsg is not None:
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['note']
      del depend_0_counts[depend_0_name]['counts']
      continue

    try:
      logger.info(f"  Reading '{depend_0_name}'")
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, logger=logger, iso8601=False)
      logger.info(f"  Read '{depend_0_name}'")
    except Exception as e:
      emsg = f"{id}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) raised: \n{e}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['note']
      del depend_0_counts[depend_0_name]['counts']
      continue

    DataType, emsg = _check_data(id, depend_0_name, data, url)
    if emsg is not None:
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['note']
      del depend_0_counts[depend_0_name]['counts']
      continue

    if DataType != DataTypeMaster:
      emsg = f"  {id}/{depend_0_name}: DataType in Master CDF = '{DataTypeMaster}' != '{DataType}' in {url}"
      cdawmeta.error("cadence", id, None, "DataTypeMissMatch", emsg, logger)

    try:
      epoch = data[depend_0_name]['VarData']
      if epoch.size == 1 or len(epoch) < 2:
        emsg = f"{id}/{depend_0_name}: Could not determine cadence because "
        if epoch.size == 1:
          emsg += f"Returned {depend_0_name} value is a scalar in {url}"
        else:
          emsg += f"len({depend_0_name}) = {len(epoch)} in {url}"
        logger.error("  " + emsg)
        depend_0_counts[depend_0_name]['error'] = emsg
        del depend_0_counts[depend_0_name]['note']
        del depend_0_counts[depend_0_name]['counts']
        continue

      if DataType == 'CDF_EPOCH16':
        # C1_CP_EFW_L3_E3D_INERT
        # Blocked from executing by _check_data_types() and not well tested.
        # See reason in note in _check_data_types().
        diff = _diff_cdf_epoch16(epoch)
      else:
        diff = numpy.diff(epoch)
        diff = diff.astype(int)

    except Exception as e:
      emsg = f"{url}: numpy.diff({depend_0_name}['VarData']) error: {e}"
      raise Exception(emsg)

    sf = 1e3 # CDF_EPOCH is in milliseconds
    duration_unit = "ms"
    if DataType == 'CDF_TIME_TT2000':
      duration_unit = "ns"
      sf = 1e9 # CDF_TIME_TT2000 is in nanoseconds
    if DataType == 'CDF_EPOCH16':
      duration_unit = "ps"
      sf = 1e12 # CDF_EPOCH16 is in picoseconds

    counts = Counter(diff)
    total = sum(counts.values())
    for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
      fraction = count / total
      if value != round(value):
        logger.warning(f"  Cadence {metadatum['id']}/{depend_0_name} {value} [ms] != {value/sf} [ms] in {url}{meta}")
      value_s = value/sf
      if value_s >= 1e-3:
        t = timedelta(seconds=value_s)
        duration_iso8601 = t.isoformat()
      else: # timedelta() returns 0 for less than microsecond, so handle manually.
        # TODO: When there are fractional seconds, we should be rendering
        # with a fixed number of significant digits and then trimming extra trailing
        # zeros. Move this into a function and write tests. Also modify diffs 
        # to be to three significant digits so that there are fewer bins.
        if value_s > 1e-9:
          duration_iso8601 = f"PT{value_s:.9f}S"
        elif value_s > 1e-12:
          duration_iso8601 = f"PT{value_s:.12f}S"
        else:
          duration_iso8601 = f"PT{value_s:.15f}S"
      count_dict = {
        "count": count,
        "duration": int(value), # Int for JSON serialization (so not numpy type)
        "duration_unit": duration_unit,
        "duration_iso8601": duration_iso8601,
        "fraction": fraction
      }
      depend_0_counts[depend_0_name]['counts'].append(count_dict)
      logger.info(f"  {count_dict}")

    if 0 == len(depend_0_counts[depend_0_name]['counts']):
      emsg = f"{id}/{depend_0_name}: Could not determine cadence in {url}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    duration = depend_0_counts[depend_0_name]['counts'][0]['duration']
    duration_unit = depend_0_counts[depend_0_name]['counts'][0]['duration_unit']
    iso = depend_0_counts[depend_0_name]['counts'][0]['duration_iso8601']
    cnt = depend_0_counts[depend_0_name]['counts'][0]['count']
    pct = 100*depend_0_counts[depend_0_name]['counts'][0]['fraction']

    note = f"Cadence based on variable '{depend_0_name}' in {url}. "
    note += f"This most common cadence occured for {pct:0.4f}% of the {cnt} timesteps. "
    note += f"Cadence = {duration} [{duration_unit}] = {iso}."
    depend_0_counts[depend_0_name]['note'] = note

  return [{"id": id, "cadence": depend_0_counts}]

def _diff_cdf_epoch16(epoch):

  # diffs only to ns precision because cdflib returns datetime64.
  epoch = cdflib.cdfepoch.to_datetime(epoch)
  diff_ns = numpy.diff(epoch)

  ps_diffs = False
  if ps_diffs:
    # Not tested. Compute diffs to picosecond precision.
    epoch_parts = cdflib.cdfepoch.breakdown(epoch)
    ps = epoch_parts[:,9]
    diff_ps = numpy.diff(ps)
    diff_ps = 1e3*diff_ns + diff_ps
  else:
    diff_ps = 1e3*diff_ns

  diff_ps = diff_ps.astype(int)

  return diff_ps

def _extract_and_check_metadata(id, metadatum, logger):
  orig_data = cdawmeta.util.get_path(metadatum, ['orig_data', 'data'])
  if orig_data is None:
    emsg = f"{id}: No orig_data"
    cdawmeta.error("cadence", id, None, "NoOrigData", emsg, logger)
    return None, None, {"error": emsg}

  master = cdawmeta.util.get_path(metadatum, ['master', 'data'])
  if master is None:
    emsg = f"{id}: No master"
    cdawmeta.error("cadence", id, None, "NoMaster", emsg, logger)
    return None, None, {"error": emsg}
  master = cdawmeta.restructure.master(master, logger=logger)

  if 'FileDescription' not in orig_data:
    emsg = f"{id}: No FileDescription in orig_data"
    cdawmeta.error("cadence", id, None, "FileDescriptionInOrigData", emsg, logger)
    return None, None, {"error": emsg}

  if len(orig_data['FileDescription']) == 0:
    emsg = f"{id}: Empty FileDescription in orig_data"
    cdawmeta.error("cadence", id, None, "FileDescriptionInOrigDataEmpty", emsg, logger)
    return None, None, {"error": emsg}

  if 'Name' not in orig_data['FileDescription'][0]:
    emsg = f"{id}: No 'Name' attribute in orig_data['FileDescription'][0]"
    cdawmeta.error("cadence", id, None, "NoNameInFileDescriptionInOrigData", emsg, logger)
    return None, None, {"error": emsg}

  return orig_data, master, None

def _check_data_types(id, master, depend_0_name, logger):

  if depend_0_name not in master['CDFVariables']:
    emsg = f"{id}: '{depend_0_name}' in CDF is not in master['CDFVariables']"
    cdawmeta.error("cadence", id, None, "NoDEPEND_0", emsg, logger)
    return emsg

  if 'VarDescription' not in master['CDFVariables'][depend_0_name]:
    emsg = f"{id}: '{depend_0_name}['VarDescription']' is not in master['CDFVariables']['{depend_0_name}']"
    cdawmeta.error("cadence", id, None, "NoVarDescription", emsg, logger)
    return emsg

  if 'DataType' not in master['CDFVariables'][depend_0_name]['VarDescription']:
    # Catch this error before attempting to read depend_0_name b/c cdflib
    # takes a long time to return no data.
    emsg = f"{id}: '{depend_0_name}['VarAttributes']['DataType']' is not in master['CDFVariables']['{depend_0_name}']. Not attempting a read."
    cdawmeta.error("cadence", id, None, "NoDataTypeInMaster", emsg, logger)
    return emsg

  DataType = master['CDFVariables'][depend_0_name]['VarDescription']['DataType']
  if DataType == 'CDF_EPOCH16':
    # e.g., C1_CP_EFW_L3_E3D_INERT
    # 99 DEPEND_0s have this according to
    # https://hapi-server.org/meta/cdaweb/variable/#DataType=CDF_EPOCH16
    # Put constraint on number of records returned to speed up?
    emsg = f"{id}/{depend_0_name}: Skipping CDF_EPOCH16 needed call to cdflib.cdfepoch.to_datetime() is slow. Use alternative method."
    cdawmeta.error("cadence", id, None, "NoImplementedCDF_EPOCH16", emsg, logger)
    logger.error("  " + emsg)
    return emsg

  if 'VarAttributes' not in master['CDFVariables'][depend_0_name]:
    emsg = f"{id}: '{depend_0_name}['VarAttributes']' is not in master['CDFVariables']"
    cdawmeta.error("cadence", id, None, "NoVarAttributesInMaster", emsg, logger)
    return emsg

  # e.g, THA_L2_ESA
  VarAttributes = master['CDFVariables'][depend_0_name]['VarAttributes']
  if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
    emsg = f"{id}/{depend_0_name}: Not Implemented: VIRTUAL DEPEND_0"
    cdawmeta.error("cadence", id, None, "NotImplementedVirtualDEPEND_0", emsg, logger)
    return emsg

  return None

def _check_data(id, depend_0_name, data, url):

  if data is None:
    emsg = f"{id}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None."
    return None, emsg

  emsg_coda = f"in {url}; CDF metadata = {data}"

  VarAttributes = data[depend_0_name].get('VarAttributes', None)
  if VarAttributes is None:
    emsg = f"{id}/{depend_0_name}['VarAttributes'] = None {emsg_coda}"
    return None, emsg

  if 'VarData' not in data[depend_0_name]:
    emsg = f"{id}/{depend_0_name}: No 'VarData' in {emsg_coda}"
    return None, emsg

  # e.g., PSP_FLD_L3_RFS_HFR
  if data[depend_0_name]['VarData'] is None:
    emsg = f"{id}/{depend_0_name}['VarData'] = None in {emsg_coda}"
    return None, emsg

  DataType = cdawmeta.util.get_path(data[depend_0_name],['VarDescription', 'DataType'])
  if DataType is None:
    emsg = f"  {id}/{depend_0_name}['VarDescription']['DataType'] = None in {emsg_coda}"
    return None, emsg

  return DataType, None