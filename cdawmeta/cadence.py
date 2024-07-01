import os

import cdawmeta

root_dir = os.path.normpath(os.path.join(os.path.dirname(__file__)))
base_dir = os.path.join(root_dir, 'data')
in_file  = os.path.join(base_dir, f'cdaweb.json')

log_config = {
  'file_log': os.path.join(base_dir, f'cadence.log'),
  'file_error': False,
  'format': '%(message)s',
  'rm_string': root_dir + '/'
}
logger = cdawmeta.util.logger(**log_config)

try:
  datasets = cdawmeta.util.read(in_file, logger=logger)
except Exception as e:
  logger.error(f'Error reading {in_file}: {e}')
  exit(1)
n_cdaweb = len(datasets)

dataset = datasets[0]

if '_file_list' in dataset:
  file_list = os.path.join(root_dir, dataset['_file_list'])

try:
  print(file_list)
  file_list = cdawmeta.util.read(file_list, logger=logger)["_decoded_content"]
except Exception as e:
  msg = f"Error: Could not open {dataset['id']} master file: {e}"
  logger.error(msg)
  #continue
print(file_list)

