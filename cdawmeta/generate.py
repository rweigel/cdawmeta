import os
import cdawmeta

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
      file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f'{id}.error.txt')
      if os.path.exists(file_name):
        msg = "Using cached error response because update = regen = False or gen_name = 'cadence' and found cached error file."
        logger.info(msg)
        return {
                'log': msg,
                'data': None,
                'data-file': None,
                'error': cdawmeta.util.read(file_name, logger=logger)
              }

  dsid = metadatum['id']

  datasets = None
  try:
    gen_func = getattr(cdawmeta._generate, gen_name)
    datasets = gen_func(metadatum, logger)
  except Exception as e:
    import traceback
    trace = traceback.format_exc()
    emsg = f"{dsid}:\n{trace}"
    logger.error(f"Error: {emsg}")
    file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f"{id}.error.txt")
    cdawmeta.util.write(file_name, trace, logger=logger)
    return {
              'id': id,
              'log': None,
              'data': None,
              'data-file': None,
              'error': emsg
            }

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

  return {"id": id, "data": data, "data-file": data_file}
