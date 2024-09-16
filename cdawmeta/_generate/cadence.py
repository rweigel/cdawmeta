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
  #cache_dir = os.path.join(cdawmeta.DATA_DIR, url.split("://")[1])
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

  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache, cache_dir=cache_dir)

  if depend_0_names is None:
    raise Exception(f"cdawmeta.io.read_cdf_depend_0s('{url}') failed.")

  depend_0_counts = {}
  logger.info(f"DEPEND_0s: {depend_0_names}")

  for depend_0_name in depend_0_names:

    try:
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, iso8601=False)
    except Exception as e:
      emsg = f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) raised: \n{e}"
      raise Exception(emsg)

    if data is None:
      emsg = f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None."
      raise Exception(emsg)

    meta = f"; CDF metadata = {data}"

    emsg = None
    VarAttributes = data[depend_0_name].get('VarAttributes', None)
    if VarAttributes is None:
      emsg = f"{depend_0_name}['VarAttributes'] = Nonein {url}{meta}"
      raise NotImplementedError(emsg)

    if 'VarData' not in data[depend_0_name]:
      emsg = f"Error: {depend_0_name} has no 'VarData' in {url}{meta}"
      raise NotImplementedError(emsg)

    if data[depend_0_name]['VarData'] is None:
      emsg = f"{depend_0_name}['VarData'] = None in {url}{meta}"
      raise NotImplementedError(emsg)

    # THA_L2_ESA
    if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
      emsg = f"VIRTUAL DEPEND_0 ({depend_0_name}) not handled in {url}{meta}"
      raise NotImplementedError(emsg)

    try:
      diff = numpy.diff(data[depend_0_name]['VarData'])
    except Exception as e:
      emsg = f"{url}: numpy.diff({depend_0_name}['VarData']) error: {e}"
      raise Exception(emsg)

    counts = Counter(diff)
    total = sum(counts.values())
    depend_0_counts[depend_0_name] = {'counts': [], 'note': None}
    for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
      fraction = count / total
      value = int(value)
      t = timedelta(milliseconds=value)
      duration = t.isoformat()
      count_dict = {
        "count": count,
        "duration_ms": value,
        "duration_iso8601": duration,
        "fraction": fraction
      }
      depend_0_counts[depend_0_name]['counts'].append(count_dict)

    percent = 100*depend_0_counts[depend_0_name]['counts'][0]['fraction']
    depend_0_counts[depend_0_name]['note'] = f"Cadence based on variable '{depend_0_name}' in {url}. This most common cadence occured for {percent:0.4f}% of the timesteps."

  return [depend_0_counts]

