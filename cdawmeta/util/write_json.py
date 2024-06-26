import os
import json
import logging

def write_json(_dict, file_name, logger=None):

  if logger is not None:
    logger.info(f'Writing {file_name}')

  file_dir = os.path.dirname(file_name)
  if not os.path.exists(file_dir):
    if logger is not None:
      logger.info(f'Creating {file_dir}')
    os.makedirs(file_dir, exist_ok=True)

  with open(file_name, 'w', encoding='utf-8') as f:
    json.dump(_dict, f, indent=2)

  if logger is not None:
    logger.info(f'Wrote {file_name}')
