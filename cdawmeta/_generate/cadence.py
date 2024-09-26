import os
from datetime import datetime

from timedelta_isoformat import timedelta

import cdawmeta

dependencies = ['orig_data']

# TODO: Like sample_start_stop(), there should be an option to not update this
# information. The requests take a long time and once a sample start and stop
# date is created it is unlikely to need updating.

def cadence(metadatum, logger):

  import numpy
  from collections import Counter

  orig_data = metadatum['orig_data']['data']

  url = orig_data['FileDescription'][-1]['Name']
  cache_dir = cdawmeta.DATA_DIR

  use_cache = True
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to use use_cache=update and have
  # read_file() handle headers to determine if content has change and a
  # re-download is needed.
  #import pdb; pdb.set_trace()

  # FORMOSAT5_AIP_IDN
  if url.endswith('.nc'):
    # Use read_ws() instead.
    raise NotImplementedError("'.nc' not handled for {url}")

  logger.info(f"Computing cadence for {url}")
  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache, cache_dir=cache_dir)

  if depend_0_names is None:
    raise Exception(f"cdawmeta.io.read_cdf_depend_0s('{url}') failed.")

  depend_0_counts = {}

  for depend_0_name in depend_0_names:
    logger.info(f"Computing cadence for {metadatum['id']}/DEPEND_0 = '{depend_0_name}'")

    depend_0_counts[depend_0_name] = {'counts': []}

    try:
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, iso8601=False)
    except Exception as e:
      emsg = f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) raised: \n{e}"
      raise Exception(emsg)

    if data is None:
      emsg = f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None."
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

    meta = f"; CDF metadata = {data}"

    emsg = None
    VarAttributes = data[depend_0_name].get('VarAttributes', None)
    if VarAttributes is None:
      emsg = f"{depend_0_name}['VarAttributes'] = None in {url}{meta}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

    # THA_L2_ESA
    if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
      emsg = f"Not Implemented: VIRTUAL DEPEND_0 ({metadatum['id']}/{depend_0_name})"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

    if 'VarData' not in data[depend_0_name]:
      emsg = f"Error: {depend_0_name} has no 'VarData'"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

    # PSP_FLD_L3_RFS_HFR
    if data[depend_0_name]['VarData'] is None:
      emsg = f"{depend_0_name}['VarData'] = None"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

    try:
      diff = numpy.diff(data[depend_0_name]['VarData'])
    except Exception as e:
      emsg = f"{url}: numpy.diff({depend_0_name}['VarData']) error: {e}"
      raise Exception(emsg)

    DataType = cdawmeta.util.get_path(data[depend_0_name],['VarDescription', 'DataType'])
    if DataType is None:
      emsg = f"  {depend_0_name}['VarDescription']['DataType'] = None in {url}{meta}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
      continue

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
      # TODO: When there are fractional seconds, we should be rendering
      # with a fixed number of significant digits and then trimming trailing
      # zeros. Move this into a function and write tests. We should also 
      # modify diffs to be to three significant digits so that there are fewer
      # bins.
      value_s = value/sf
      if value_s >= 1:
        t = timedelta(seconds=value_s)
        duration_iso8601 = t.isoformat()
      else:
        # timedelta() returns 0 for less than microsecond.
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
      emsg = f"Error: Could not determie cadence for {metadatum['id']}/{depend_0_name}"
      logger.error("  " + emsg)
      depend_0_counts[depend_0_name]['counts'] = None
      depend_0_counts[depend_0_name]['error'] = emsg
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
    depend_0_counts[depend_0_name]['url'] = url

  return [depend_0_counts]

