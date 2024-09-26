import os
import cdawmeta

def generate(metadatum, gen_name, logger, update=True, regen=False, diffs=False):

  # Remove the leading underscore. Need a better way to do this.
  sub_dir = 'info'

  id = metadatum['id']
  file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f'{id}.pkl')

  if not update and not regen:
    if id is not None and not id.startswith('^'):
      if os.path.exists(file_name):
        msg = "Using cache because update = regen = False or gen_name = "
        msg += "'cadence' and found cached file."
        logger.info(msg)
        return {
                'id': id,
                'log': msg,
                'data-file': file_name,
                'data': cdawmeta.util.read(file_name, logger=logger)
              }
      file_name = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir, f'{id}.error.txt')
      if os.path.exists(file_name):
        msg = "Using cached error response because update = regen = False "
        msg += "or gen_name = 'cadence' and found cached error file."
        logger.info(msg)
        return {
                'id': id,
                'log': msg,
                'error': cdawmeta.util.read(file_name, logger=logger),
                'data-file': None,
                'data': None
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
    cdawmeta.util.write(file_name, trace, logger=logger)
    return {
              'id': id,
              'log': None,
              'error': emsg,
              'data-file': None,
              'data': None,
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

    # JSON file not used internally, but useful for visual debugging
    file_name = file_name.replace('.pkl', '.json')
    data_file.append(file_name)
    cdawmeta.util.write(file_name, dataset, logger=logger)

  if len(data) == 1:
    data = data[0]

  return {"id": id, "data-file": data_file, "data": data}
