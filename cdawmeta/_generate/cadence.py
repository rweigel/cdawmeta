import os
from datetime import datetime
from collections import Counter

import numpy
import cdflib
from timedelta_isoformat import timedelta

import cdawmeta

dependencies = ['orig_data']

def cadence(metadatum, logger):

  id = metadatum['id']

  orig_data = metadatum['orig_data']['data']

  url = orig_data['FileDescription'][0]['Name']
  cache_dir = cdawmeta.DATA_DIR

  use_cache = True
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to use use_cache=update and have
  # read_file() handle headers to determine if content has change and a
  # re-download is needed.

  # FORMOSAT5_AIP_IDN
  if url.endswith('.nc'):
    # Use HAPI client instead (CDAWeb service could be used, but can be up to ~5x slower).
    emsg = f"{id}: Computing cadence for file type '.nc' is not implemented"
    logger.error(emsg)
    return [{"error": emsg}]

  logger.info(f"{id}: Computing cadence for {url}")
  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache, cache_dir=cache_dir)

  if depend_0_names is None:
    emsg = f"{id}: cdawmeta.io.read_cdf_depend_0s('{url}') failed."
    logger.error(emsg)
    return [{"error": emsg}]

  depend_0_counts = {}

  for depend_0_name in depend_0_names:
    logger.info(f"  Computing cadence for {id}/DEPEND_0 = '{depend_0_name}'")

    depend_0_counts[depend_0_name] = {"url": url, 'note': "", 'counts': []}

    try:
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, logger=logger, iso8601=False)
    except Exception as e:
      emsg = f"{id}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) raised: \n{e}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    if data is None:
      emsg = f"{id}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None."
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    meta = f"; CDF metadata = {data}"

    emsg = None
    VarAttributes = data[depend_0_name].get('VarAttributes', None)
    if VarAttributes is None:
      emsg = f"{id}/{depend_0_name}['VarAttributes'] = None in {url}{meta}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    # THA_L2_ESA
    if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
      emsg = f"{id}/{depend_0_name}: Not Implemented: VIRTUAL DEPEND_0 in {url}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    if 'VarData' not in data[depend_0_name]:
      emsg = f"{id}/{depend_0_name}: No 'VarData'"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    # PSP_FLD_L3_RFS_HFR
    if data[depend_0_name]['VarData'] is None:
      emsg = f"{id}/{depend_0_name}['VarData'] = None"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

    DataType = cdawmeta.util.get_path(data[depend_0_name],['VarDescription', 'DataType'])
    if DataType is None:
      emsg = f"  {id}/{depend_0_name}['VarDescription']['DataType'] = None in {url}{meta}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['error'] = emsg
      del depend_0_counts[depend_0_name]['counts']
      del depend_0_counts[depend_0_name]['note']
      continue

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
        continue

      if DataType == 'CDF_EPOCH16':
        # C1_CP_EFW_L3_E3D_INERT
        #diff = _diff_cdf_epoch16(epoch)
        emsg = f"{id}/{depend_0_name}: Skipping CDF_EPOCH16 diff because cdflib.cdfepoch.to_datetime() is slow. Use alternative method."
        logger.error("  " + emsg)
        depend_0_counts[depend_0_name]['error'] = emsg
        del depend_0_counts[depend_0_name]['counts']
        del depend_0_counts[depend_0_name]['note']
        continue
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

  return [depend_0_counts]

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
