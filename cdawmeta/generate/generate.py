import os
import cdawmeta

def _write_errors(name, logger):
  # Write all errors to a single file if all datasets were requested. Errors
  # were already written to log file, but here we need to do additional formatting
  # that is more difficult if errors were written as they occur.

  errors = ""
  fname = os.path.join(cdawmeta.DATA_DIR, name, f'{name}.errors.log')
  for dsid, vars in cdawmeta.error.errors.items():
    if isinstance(vars, str):
      errors += f"{dsid}: {vars}\n"
      continue
    errors += f"{dsid}:\n"
    for vid, msgs in vars.items():
      errors += f"  {vid}:\n"
      for msg in msgs:
        errors += f"    {msg}\n"
  cdawmeta.util.write(fname, errors, logger=logger)

def generate(metadatum, gen_fn, logger, update=True, regen=False, diffs=False):

  meta_type = gen_fn.__name__.replace("_", "")
  sub_dir = 'info'

  id = metadatum['id']

  if not update and not regen:
    if id is not None and not id.startswith('^'):
      # This will not catch case when there is and id@0, id@1, etc. Need to read all
      # files that match pattern id@*. and loop over.
      file_name = os.path.join(cdawmeta.DATA_DIR, meta_type, sub_dir, f'{id}.pkl')
      if os.path.exists(file_name):
        msg = 'Using cache because update = regen = False and found cached file.'
        logger.info(msg)
        data = {}
        data['data'] = cdawmeta.util.read(file_name, logger=logger)
        data['log'] = msg
        data['data-file'] = file_name
        return data

  dsid = metadatum['id']

  datasets = None
  try:
    datasets = gen_fn(metadatum)
  except Exception as e:
    import traceback
    logger.error(f"Error: {dsid}: {e}")
    print(traceback.format_exc())

  data = []
  data_file = []
  if datasets is not None:

    for dataset in datasets:
      data.append(dataset)
      file_name = os.path.join(cdawmeta.DATA_DIR, meta_type, sub_dir, f'{id}.pkl')
      data_file.append(file_name)
      cdawmeta.util.write(file_name, dataset, logger=logger)

      # JSON file not used internally, but useful for debugging
      file_name = os.path.join(cdawmeta.DATA_DIR, meta_type, sub_dir, f'{id}.json')
      cdawmeta.util.write(file_name, dataset, logger=logger)

    if len(data) == 1:
      data = data[0]

  if regen or update:
    # This should be moved into metadata.py. It is re-writing file for
    # each dataset, each time with additional information.
    _write_errors(meta_type, logger)

  return {"id": id, "data": data, "data-file": data_file}
