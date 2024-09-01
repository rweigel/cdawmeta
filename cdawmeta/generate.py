import os
import cdawmeta

def _write_errors(name, logger):
  # Write all errors to a single file if all datasets were requested. Errors
  # were already written to log file, but here we need to do additional formatting
  # that is more difficult if errors were written as they occur.

  errors = ""
  for dsid, vars in cdawmeta.error.errors.items():
    if isinstance(vars, str):
      errors += f"{dsid}: {vars}\n"
      continue
    errors += f"{dsid}:\n"
    for vid, msgs in vars.items():
      errors += f"  {vid}:\n"
      for msg in msgs:
        errors += f"    {msg}\n"
  cdawmeta.util.write(os.path.join(cdawmeta.DATA_DIR, name, f'{name}.errors.log'), errors, logger=logger)

def generate(id, gen_fn, logger, update=True, diffs=False, max_workers=None, orig_data=False, skip=None):

  if gen_fn.__name__ == '_hapi':
    meta_type = 'hapi'
    sub_dir = 'info'
  if gen_fn.__name__ == '_soso':
    meta_type = 'soso'
    sub_dir = 'info'

  if not update and id is not None and not id.startswith('^'):
    file_name = os.path.join(cdawmeta.DATA_DIR, meta_type, sub_dir, f'{id}.json')
    if os.path.exists(file_name):
      logger.info(f'Using cache because update = False and found cached file {file_name}')
      return cdawmeta.util.read(file_name, logger=logger)

  dsids = cdawmeta.ids(id=id, update=update, skip=skip)

  # Loop over dataset ids and call _hapi() for each id
  metadata_hapi = []
  for dsid in dsids:
    try:
      metadatum = cdawmeta.metadata(id=dsid, embed_data=True, update=update, diffs=diffs, max_workers=max_workers, orig_data=orig_data)
      if meta_type == 'hapi':
        datasets = gen_fn(metadatum[dsid], orig_data)
      if meta_type == 'soso':
        datasets = gen_fn(metadatum[dsid])
    except Exception as e:
      import traceback
      logger.error(f"Error: {dsid}: {e}")
      print(traceback.format_exc())
      continue

    if datasets is None:
      continue

    for dataset in datasets:
      metadata_hapi.append(dataset)

  _write_errors(meta_type, logger)

  if id is None:

    # Write catalog-all.json and catalog.json
    fname = os.path.join(cdawmeta.DATA_DIR, meta_type, 'catalog-all')
    cdawmeta.util.write(fname + ".json", metadata_hapi, logger=logger)
    cdawmeta.util.write(fname + ".pkl", metadata_hapi, logger=logger)
    from copy import deepcopy
    metadata_hapi_copy = deepcopy(metadata_hapi)
    for metadatum in metadata_hapi_copy:
      del metadatum['info']
    fname = os.path.join(cdawmeta.DATA_DIR, meta_type, 'catalog.json')
    cdawmeta.util.write(fname, metadata_hapi_copy, logger=logger)

  return metadata_hapi
