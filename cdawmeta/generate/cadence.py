import os

import cdawmeta

logger = None

def cadence(id=None, update=True, diffs=False, max_workers=None, log_level='info', skip=None):
  from .generate import generate
  global logger
  if logger is None:
    logger = cdawmeta.logger('cadence')
    logger.setLevel(log_level.upper())

  return generate(id, _cadence, logger, update=update, diffs=diffs, max_workers=max_workers, skip=skip)

def _cadence(metadatum):

  import numpy
  from collections import Counter

  orig_data = metadatum['orig_data']['data']

  url = orig_data['FileDescription'][-1]['Name']
  cache_dir = os.path.join(cdawmeta.DATA_DIR, url.split("://")[1])
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to use use_cache=update and have
  # read_file() handle headers to determine if content has change and a
  # re-download is needed.
  depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=True, cache_dir=cache_dir)

  if depend_0_names is None:
    logger.error(f"cdawmeta.io.read_cdf_depend_0s('{url}') failed.")
    return None

  depend_0_name = depend_0_names[0]

  data = cdawmeta.io.read_cdf(url, variables=depend_0_name, iso8601=False)
  counts = Counter(numpy.diff(data[depend_0_name]['VarData']))
  total = sum(counts.values())
  count_dict = {}
  for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
    fraction = count / total
    value = int(value)
    count_dict[value] = {"count": count, "fraction": fraction}

  return [{"id": metadatum['id'], "cadence": count_dict}]

if False:
  args = {'max_workers': 1}
  if args['max_workers'] == 1:
    for dataset in datasets:
      counts = cadence(dataset, update=False, logger=logger)
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(dataset):
      try:
        cadence(dataset, update=False, logger=logger)
      except Exception as e:
        import traceback
        logger.error(f"Error: {dataset['id']}: {traceback.print_exc()}")
    with ThreadPoolExecutor(max_workers=args['max_workers']) as pool:
      pool.map(call, datasets)
