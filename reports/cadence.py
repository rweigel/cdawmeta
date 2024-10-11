import os
import sys

import cdawmeta

def cadence(clargs):

  out_dir = 'reports'
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  dir_name = os.path.join(cdawmeta.DATA_DIR, out_dir)

  clargs = {**clargs, 'meta_type': 'cadence'}
  meta = cdawmeta.metadata(**clargs)
  cadences = {}
  lines = [['id', 'depend_0', 'cadence', 'cadence_unit', 'cadence_iso8601', 'fraction']]

  for id in meta.keys():
    logger.info(f"{id}:")
    cadence = cdawmeta.util.get_path(meta[id],['cadence'])

    cadences[id] = {}

    if 'error' in cadence:
      logger.error(f"  {cadence['error']}")
      cadences[id] = cadence['error']
      lines.append([id, '', -1, '', '', -1])
      continue

    depend_0_obj = cdawmeta.util.get_path(meta[id],['cadence', 'data', 'cadence'])

    for depend_0 in depend_0_obj:
      depend_0_info = depend_0_obj[depend_0]
      if 'error' in depend_0_info:
        logger.error(f" {depend_0}: {depend_0_info['error']}")
        cadences[id][depend_0] = depend_0_info['error']
        lines.append([id, depend_0, -1, '', '', -1])
        continue
      if 'counts' in depend_0_info:
        cadences[id][depend_0] = depend_0_info
        count = depend_0_info['counts'][0]
        logger.info(f"  {depend_0}: {count}")
        line = [id, depend_0, count['duration'], count['duration_unit'], count['duration_iso8601'], count['fraction']]
        lines.append(line)

  cdawmeta.util.write(os.path.join(dir_name, f'{report_name}.csv'), lines, logger=logger)
  cdawmeta.util.write(os.path.join(dir_name, f'{report_name}.json'), cadences, logger=logger)
