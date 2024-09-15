import os
from datetime import datetime

from timedelta_isoformat import timedelta

import cdawmeta

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
  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache, cache_dir=cache_dir)

  if depend_0_names is None:
    logger.error(f"cdawmeta.io.read_cdf_depend_0s('{url}') failed.")
    return None

  depend_0_counts = {}
  logger.info(f"DEPEND_0s: {depend_0_names}")

  for depend_0_name in depend_0_names:

    if url.endswith('.nc'):
      # Use read_ws() instead.
      msg = "'.nc' not handled for {url}"
      msg = logger.error(f"Error: {msg}")
      raise NotImplementedError(msg)

    try:
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, iso8601=False)
    except Exception as e:
      logger.error(f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) raised:")
      logger.error(f"{e}")
      raise e

    if data is None:
      logger.error(f"Error: {url}: cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None.")
      return None

    meta = f"; CDF metadata = {data}"

    emsg = None
    VarAttributes = data[depend_0_name].get('VarAttributes', None)
    if VarAttributes is None:
      emsg = f"{depend_0_name}['VarAttributes'] = Nonein {url}{meta}"

    if 'VarData' not in data[depend_0_name]:
      emsg = f"Error: {depend_0_name} has no 'VarData' in {url}{meta}"

    if data[depend_0_name]['VarData'] is None:
      emsg = f"{depend_0_name}['VarData'] = None in {url}{meta}"

    # THA_L2_ESA
    if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
      emsg = f"VIRTUAL DEPEND_0 ({depend_0_name}) not handled in {url}{meta}"

    if emsg is not None:
      #logger.error(f"Error: {emsg}")
      raise NotImplementedError(emsg)

    try:
      diff = numpy.diff(data[depend_0_name]['VarData'])
    except Exception as e:
      emsg = f"{url}: numpy.diff({depend_0_name}['VarData']) error: {e}"
      logger.error(f"Error: {emsg}")
      raise e

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

