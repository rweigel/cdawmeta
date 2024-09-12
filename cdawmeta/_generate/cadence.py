import os
from datetime import datetime

from timedelta_isoformat import timedelta

import cdawmeta

def cadence(metadatum, logger):

  import numpy
  from collections import Counter

  orig_data = metadatum['orig_data']['data']

  url = orig_data['FileDescription'][-1]['Name']
  cache_dir = os.path.join(cdawmeta.DATA_DIR, url.split("://")[1])

  use_cache = True
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to use use_cache=update and have
  # read_file() handle headers to determine if content has change and a
  # re-download is needed.
  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache, cache_dir=cache_dir)

  if depend_0_names is None:
    logger.error(f"cdawmeta.io.read_cdf_depend_0s('{url}') failed.")
    return None

  depend_0_counts = {}
  for depend_0_name in depend_0_names:

    try:
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, iso8601=False)
    except Exception as e:
      logger.error(f"Error: {url}: {e}")
      return None

    try:
      # TODO: Can fail b/c VIRTUAL DEPEND_0 (!). Use read_ws() instead.
      data[depend_0_name]['VarData']
    except Exception as e:
      logger.error(f"Error: {url}: {e}")
      return None

    counts = Counter(numpy.diff(data[depend_0_name]['VarData']))
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

