import os

import cdawmeta

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__), '..'))
base_dir = os.path.join(root_dir, 'data')
out_dir  = os.path.join(base_dir, 'cadence')
in_file  = os.path.join(base_dir, f'cdaweb.json')

log_config = {
  'file_log': os.path.join(base_dir, f'cadence.log'),
  'file_error': os.path.join(base_dir, f'cadence.error'),
  'format': '%(message)s',
  'rm_string': root_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

def cadence(dataset, update=False, logger=logger):

  import cdflib
  import numpy
  from collections import Counter

  count_file = os.path.join(out_dir, f"{dataset['id']}.json")
  if update == False and os.path.exists(count_file):
    logger.info(f"Not updating {dataset['id']}")
    return None

  if '_file_list' in dataset:
    file_list = os.path.join(root_dir, dataset['_file_list'])
  else:
    return None

  try:
    file_list = cdawmeta.util.read(file_list, logger=logger)["_decoded_content"]
  except Exception as e:
    msg = f"Error: Could not open {dataset['id']} master file: {e}"
    logger.error(msg)
    return None

  url = file_list['FileDescription'][-1]['Name']

  url = "https://cdaweb.gsfc.nasa.gov/sp_phys/data/cluster/c2/jp/pmp/2024/c2_jp_pmp_20240901_v07.cdf"
  depend_0s = cdawmeta.read_cdf_depend_0s(url, _return='data', logger=logger, cache_dir='.')

  if depend_0s is None:
    logger.error(f"Error: {dataset['id']}: Could not read {url}")
    return None

  keys = list(depend_0s.keys())
  depend_0 = depend_0s[keys[0]]

  try:
    epoch = cdflib.cdfepoch.to_datetime(depend_0)
  except Exception as e:
    logger.error(f"Error: {dataset['id']}: {e}")
    return None

  counts = Counter(numpy.diff(epoch))
  total = sum(counts.values())
  count_dict = {}
  print(dataset['id'])
  for value, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
    fraction = count / total
    value = int(value)
    count_dict[value] = {"count": count, "fraction": fraction}
    print(f"Cadence: {value/1e9}s, Count: {count}, Fraction: {fraction}")

  count_result = {}
  count_result[url.split('/')[-1]] = count_dict
  cdawmeta.util.write(count_file, count_result, logger=logger)

try:
  datasets = cdawmeta.util.read(in_file, logger=logger)
except Exception as e:
  logger.error(f'Error reading {in_file}: {e}')
  exit(1)

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
