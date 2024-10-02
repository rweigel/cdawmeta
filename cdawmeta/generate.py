import os
import cdawmeta

def generate(metadatum, gen_name, logger, update=True, regen=False, diffs=False):

  sub_dir = 'info'

  id = metadatum['id']
  base_path = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir)
  file_name_pkl = os.path.join(base_path, f'{id}.pkl')
  file_name_json = os.path.join(base_path, f'{id}.json')
  file_name_error = os.path.join(base_path, f'{id}.error.txt')

  if not update and not regen:
    if os.path.exists(file_name_pkl):
      msg = "Using cache because update = regen = False and found cached file."
      logger.info(msg)
      data = cdawmeta.util.read(file_name_pkl, logger=logger)
      if isinstance(data, list) and len(data) == 1:
        data = data[0]
      return {'id': id, 'log': msg, 'data-file': file_name_json, 'data': data}

    if os.path.exists(file_name_error):
      msg = "Using cached error response because update = regen = False."
      logger.info(msg)
      emsg = cdawmeta.util.read(file_name_error, logger=logger)
      return {'id': id,'log': msg, 'error': emsg, 'data-file': None, 'data': None}

  try:
    gen_func = getattr(cdawmeta._generate, gen_name)
    datasets = gen_func(metadatum, logger)
    if isinstance(datasets, dict):
      logger.info(f"Writing {file_name_error}")
      cdawmeta.util.write(file_name_error, datasets['error'])
      return {'id': id, 'log': None, 'error': datasets['error'], 'data-file': None, 'data': None}
  except Exception as e:
    import traceback
    trace = traceback.format_exc()
    home_dir = os.path.expanduser("~")
    trace = trace.replace(home_dir, "~")
    emsg = f"{id}:\n{trace}"
    cdawmeta.error("metadata", id, None, "UnHandledException", emsg, logger)
    logger.info(f"Writing {file_name_error}")
    cdawmeta.util.write(file_name_error, datasets['error'])
    import pdb;pdb.set_trace()
    return {'id': id, 'log': None, 'error': emsg, 'data-file': None, 'data': None}

  if os.path.exists(file_name_error):
    logger.info(f"Removing {file_name_error}")
    os.remove(file_name_error)

  if len(datasets) == 1:
    # Write pkl file with all datasets associated with a CDAWeb dataset.
    logger.info(f"Writing {file_name_pkl}")
    cdawmeta.util.write(file_name_pkl, datasets[0])
    # JSON file not used internally, but useful for visual debugging
    logger.info(f"Writing {file_name_json}")
    cdawmeta.util.write(file_name_json, datasets[0])
    return {"id": id, "data-file": file_name_json, "data": datasets[0]}
  else:
    logger.info(f"Writing {file_name_pkl}")
    cdawmeta.util.write(file_name_pkl, datasets)
    logger.info(f"Writing {file_name_json}")
    cdawmeta.util.write(file_name_json, datasets)

  data = []
  data_files = []

  for dataset in datasets:
    data.append(dataset)
    sid = dataset['id'] # Sub dataset id

    file_name_pkl = os.path.join(base_path, f"{sid}.pkl")
    file_name_json = os.path.join(base_path, f"{sid}.json")
    cdawmeta.util.write(file_name_pkl, dataset, logger=logger)
    cdawmeta.util.write(file_name_json, dataset, logger=logger)

    data_files.append(file_name_json)

  return {"id": id, "data-file": data_files, "data": data}
