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

def generate(metadatum, gen_name, logger, update=True, regen=False, diffs=False):

  # Remove the leading underscore. Need a better way to do this.
  sub_dir = 'info'

  id = metadatum['id']

  # Special case for cadence is because generation is slow; it requires reading
  # data file. Will need a special keyword such as regen_cadence=True to
  # pass to generate() to force regeneration.
  if (not update and not regen) or gen_name == 'cadence':
    if id is not None and not id.startswith('^'):
      # This will not catch case when there is and id@0, id@1, etc. Need to read all
      # files that match pattern id@*. and loop over.
      file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f'{id}.pkl')
      if os.path.exists(file_name):
        msg = "Using cache because update = regen = False or gen_name = 'cadence' and found cached file."
        logger.info(msg)
        return {'data': cdawmeta.util.read(file_name, logger=logger),
                'log': msg,
                'data-file': file_name
              }

  dsid = metadatum['id']

  datasets = None
  try:
    gen_func = getattr(cdawmeta._generate, gen_name)
    datasets = gen_func(metadatum, logger)
  except Exception as e:
    import traceback
    logger.error(f"Error: {dsid}: {e}")
    print(traceback.format_exc())
    return None

  if datasets is None:
    return None

  data = []
  data_file = []
  if gen_name == 'hapi':
    # Write pkl file with all HAPI datasets associated with a CDAWeb dataset.
    file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f"{id}.pkl")
    cdawmeta.util.write(file_name, datasets, logger=logger)

  for dataset in datasets:
    data.append(dataset)

    if gen_name == 'hapi':
      id = dataset['id']

    file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f"{id}.pkl")
    cdawmeta.util.write(file_name, dataset, logger=logger)

    # JSON file not used internally, but useful for debugging
    file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f'{id}.json')
    data_file.append(file_name)
    cdawmeta.util.write(file_name, dataset, logger=logger)

  if len(data) == 1:
    data = data[0]

  if regen or update:
    # This should be moved into metadata.py. It is re-writing file for
    # each dataset, each time with additional information.
    _write_errors(gen_name, logger)

  return {"id": id, "data": data, "data-file": data_file}
