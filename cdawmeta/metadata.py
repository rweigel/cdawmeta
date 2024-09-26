import os
import re

import cdawmeta

# Can't call logger = cdawmeta.logger(...) here because it calls cdawmeta.DATA_DIR
# which is set to a default. If user modifies using cdawmeta.DATA_DIR = ...,
# logger does not know about the change.
# TODO: Find a better way to handle this. 
logger = None

def _logger(log_level='info'):
  global logger
  if logger is None:
    logger = cdawmeta.logger(name='metadata', log_level=log_level)
  return logger

def ids(id=None, skip=None, update=False):
  '''Generate list of CDAWeb dataset IDs.

  IDs are generated from all.xml.

  If `id` is `None`, all IDs are returned. `id` is treated a regular expression
  if it starts with `^`.

  `skip` is a regular expression string; IDs that match `skip` are not returned.

  `update` is a boolean. If `True`, all.xml is updated before generating IDs.
  '''

  # Needed to set logger for any called underscore functions.
  # TODO: Find a better way to handle this.
  logger = _logger()

  def _remove_skips(ids_reduced):
    if skip is None:
      return ids_reduced
    regex = re.compile(skip)
    return [id for id in ids_reduced if not regex.match(id)]

  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)
  ids_all = datasets_all.keys()

  if id is None:
    return _remove_skips(list(ids_all))

  if isinstance(id, str):
    if id.startswith('^'):
      regex = re.compile(id)
      ids_reduced = [id for id in ids_all if regex.match(id)]
      if len(ids_reduced) == 0:
        raise ValueError(f"Error: id = {id}: No matches.")
    elif id not in ids_all:
      raise ValueError(f"Error: id = {id}: Not found.")
    else:
      ids_reduced = [id]

  if skip is None:
    return ids_reduced

  return _remove_skips(ids_reduced)

def metadata(meta_type=None, id=None, skip="AIM_CIPS_SCI_3A", embed_data=True,
             write_catalog=False, update=False, regen=False, regen_cadence=False, max_workers=3,
             diffs=False, log_level='info'):
  '''
  Options are documented in cli.py.

  Returns a dict of metadata. Keys are dataset IDs and values are dicts of
  metadata types.
  '''

  logger = _logger(log_level=log_level)

  meta_type_requested = meta_type
  logger.info(f"Requested meta_type: {meta_type}")

  if meta_type_requested is None:
    meta_types = cdawmeta.dependencies['all']
  else:
    choices = cdawmeta.cli('metadata.py', defs=True)['meta-type']['choices']
    if isinstance(meta_type, list):
      meta_types = []
      for _type in meta_type:
        if _type not in choices:
          raise ValueError(f"Error: {meta_type}: Not in {choices}")
        deps = cdawmeta.dependencies[_type]
        if deps is None:
          meta_types.append(_type)
          continue
        for dep in deps:
          if dep not in meta_types:
            meta_types.append(dep)
    else:
      if meta_type_requested not in choices:
        raise ValueError(f"Error: {meta_type}: Not in {choices}")
      if meta_type in cdawmeta.dependencies:
        if cdawmeta.dependencies[meta_type] is None:
          meta_types = [meta_type]
        else:
          meta_types = [*cdawmeta.dependencies[meta_type], meta_type]
      meta_type_requested = [meta_type_requested]

  logger.info(f"Given requested meta_type = {meta_type_requested}, need to create: {meta_types}")

  if not update and not regen:
    # TODO: Read cached catalog files.
    pass

  if update:
    regen = True

  if diffs and not update:
    logger.warning("diffs=True but update=False. No diffs can be computed.")

  dsids = ids(id=id, skip=skip, update=update)

  # Create base datasets using info in all.xml
  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)

  not_generated = ['master', 'orig_data', 'spase', 'spase_hpde_io']

  mloggers = {}
  for meta_type in meta_types:
    if meta_type in not_generated:
      continue
    mloggers[meta_type] = cdawmeta.logger(meta_type, log_level=log_level)

  if update and 'spase_hpde_io' in meta_types:
    logger.info("Finding CDAWeb SPASE/Numerical data records in hpde.io repository.")
    # This case is special because we need to determine the mapping from CDAWeb
    # dataset id to SPASE record.
    _spase_hpde_io(update=True, diffs=diffs)

  def get_one(dataset, mloggers):

    if 'master' in meta_types or 'spase' in meta_types:
      # 'spase' needs master to get spase_DatasetResourceID, so this must be before.
      dataset['master'] = _master(dataset, update=update, diffs=diffs)
    if 'orig_data' in meta_types:
      dataset['orig_data'] = _orig_data(dataset, update=update, diffs=diffs)
    if 'spase' in meta_types:
      dataset['spase'] = _spase(dataset, update=update, diffs=diffs)
    if 'spase_hpde_io' in meta_types:
      # Here we never update b/c update is done in earlier call to _spase_hpde_io
      dataset['spase_hpde_io'] = _spase_hpde_io(dataset['id'], update=False)

    for meta_type in meta_types:
      if meta_type in not_generated:
        continue
      logger.info(f"Generating: {meta_type} for {dataset['id']}")
      regen_ = regen
      if meta_type == 'cadence':
        # Cadence takes much longer to generate, so given as special option.
        regen_ = regen_cadence
      dataset[meta_type] = cdawmeta.generate(dataset, meta_type, mloggers[meta_type], update=update, regen=regen_)

    logger.debug("Removing metadata that was used to generate requested metadata.")
    for meta_type in meta_types:
      if meta_type_requested is not None and meta_type not in meta_type_requested:
        if meta_type in dataset:
          logger.debug(f"  Removing {meta_type} from {dataset['id']} metadata")
          del dataset[meta_type]
          continue
      if not embed_data and 'data' in dataset[meta_type]:
        logger.debug(f"  embed_data=False; removing 'data' node from {meta_type} for {dataset['id']}")
        del dataset[meta_type]['data']

  if max_workers == 1 or len(dsids) == 1:
    for dsid in dsids:
      get_one(datasets_all[dsid], mloggers)
  else:
    def call(dsid):
      try:
        get_one(datasets_all[dsid], mloggers)
      except Exception as e:
        import traceback
        logger.error(f"Error: {datasets_all[dsid]}: {traceback.print_exc()}")

      return dsid

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call, dsids)

  metadata_ = {key: datasets_all[key] for key in dsids}

  if regen or update:
    _write_errors()

  if write_catalog:
    if meta_type_requested is None:
      _write_catalog(metadata_, id, meta_types)
    else:
      _write_catalog(metadata_, id, [meta_type_requested])

  return metadata_

def _datasets(allxml):
  '''
  Returns dict of datasets. Keys are dataset IDs and values are dicts 
  with keys 'id' and 'allxml'. The value of 'allxml' is
  all.xml/data/'sites//datasite/0/dataset
  '''

  restructure = True

  datasites = cdawmeta.util.get_path(allxml, ['data', 'sites', 'datasite'])
  if datasites is None:
    raise Exception("Error[all.xml]: No 'sites/datasite' node in all.xml")

  datasets_allxml = None
  for datasite in datasites:
    if datasite['@ID'] == 'CDAWeb_HTTPS':
      datasets_allxml = datasite['dataset']
      break
  if datasets_allxml is None:
    raise Exception("Error[all.xml]: No 'sites/datasite' with ID=CDAWeb_HTTPS")

  datasets_ = {}
  for dataset_allxml in datasets_allxml:

    id = dataset_allxml['@serviceprovider_ID']
    dataset = {'id': id, 'allxml': dataset_allxml}

    if 'mastercdf' not in dataset_allxml:
      msg = f'Error[all.xml]: {id}: No mastercdf node in all.xml'
      logger.error(msg)
      continue

    if isinstance(dataset_allxml['mastercdf'], list):
      if not id.endswith('_MOVIES'):
        msg = f'Warning[all.xml]: Not implemented: {id}: More than one mastercdf referenced in all.xml mastercdf node.'
        logger.warning(msg)
      continue

    if '@ID' not in dataset_allxml['mastercdf']:
      msg = f'Error[all.xml]: {id}: No ID attribute in all.xml mastercdf node'
      logger.error(msg)
      continue

    if restructure:
      dataset['allxml'] = cdawmeta.restructure.allxml(dataset['allxml'], logger=logger)

    datasets_[id] = dataset

  return datasets_

def _allxml(update=False, diffs=False):

  timeout = cdawmeta.CONFIG['metadata']['timeouts']['allxml']

  if hasattr(_allxml, 'allxml'):
    # Use curried result (So update only updates all.xml once per execution of main program)
    return _allxml.allxml

  allurl = cdawmeta.CONFIG['urls']['all.xml']
  allxml = _fetch(allurl, 'all', 'all', timeout=timeout, update=update, diffs=diffs)

  # Curry result
  _allxml.allxml = allxml

  return allxml

def _master(dataset, update=False, diffs=False):

  restructure = True
  timeout = cdawmeta.CONFIG['metadata']['timeouts']['master']

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  master = _fetch(url, dataset['id'], 'master', timeout=timeout, update=update, diffs=diffs)
  if restructure and 'data' in master:
    master['data'] = cdawmeta.restructure.master(master['data'], logger=logger)
  return master

def _spase(dataset, update=True, diffs=False):

  master = dataset['master']
  timeout = cdawmeta.CONFIG['metadata']['timeouts']['spase']

  id = master['id']
  if 'data' not in master:
    msg = 'No spase_DatasetResourceID because no master'
    return {'id': id, 'error': msg, 'data-file': None, 'data': None}

  global_attributes = master['data']['CDFglobalAttributes']
  if 'spase_DatasetResourceID' not in global_attributes:
    msg = f"Error[master]: {master['id']}: Missing or invalid spase_DatasetResourceID attribute in master"
    logger.error(msg)
    return {'id': id, 'error': msg, 'data-file': None, 'data': None}

  if 'spase_DatasetResourceID' in global_attributes:
    spase_id = global_attributes['spase_DatasetResourceID']
    if spase_id and not spase_id.startswith('spase://'):
      msg = f"Error[master]: {master['id']}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://'"
      logger.error(msg)
      return {'id': id, 'error': msg, 'data-file': None, 'data': None}

    url = spase_id.replace('spase://', 'https://hpde.io/') + '.json'

  spase = _fetch(url, id, 'spase', timeout=timeout, diffs=diffs, update=update)

  return spase

def _spase_hpde_io(id=None, update=True, diffs=False):

  out_dir = os.path.join(cdawmeta.DATA_DIR, 'spase_hpde_io', 'info')
  if id is not None:
    pkl_file = os.path.join(out_dir, f"{id}.pkl")
    if not update and os.path.exists(pkl_file):
      return cdawmeta.util.read(pkl_file, logger=logger)
    else:
      logger.warn(f"{id}: No SPASE record in hpde.io repository.")
      return

  import glob
  if not os.path.exists('data/hpde.io'):
    import subprocess
    cmd = ['git', 'clone', '--depth 1', 'https://github.com/hpde/hpde.io']
    logger.info(f'Executing: {" ".join(cmd)}')
    result = subprocess.run(cmd, stdout=subprocess.PIPE)
    logger.info(result.stdout)

  pattern = "data/hpde.io/**/NumericalData/**/*.json"
  logger.info(f"Getting list of files that match '{pattern}'")
  files = glob.glob(pattern, recursive=True)
  logger.info(f"{len(files)} NumericalData SPASE records before removing Deprecated")
  for file in files:
    if 'Deprecated' in file:
      del files[files.index(file)]
  logger.info(f"{len(files)} NumericalData SPASE records after removing Deprecated")

  for file in files:

    logger.info(f'  Reading {file}')
    data = cdawmeta.util.read(file)

    ResourceID = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'ResourceID'])
    if ResourceID is None:
      logger.error(f"  Error - No ResourceID in {file}")
      continue

    hpde_url = f'{ResourceID.replace("spase://", "http://hpde.io/")}'

    # Flattens AccessInformation so is a list of objects, each with one AccessURL.
    data = cdawmeta.restructure.spase(data, logger=logger)
    AccessInformation = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'AccessInformation'])

    if AccessInformation is None:
      logger.error(f"  Error - No AccessInformation in {file}")
      continue

    s = "s" if len(AccessInformation) > 1 else ""
    logger.debug(f"  {len(AccessInformation)} Repository object{s} in AccessInformation")

    found = False
    ProductKeyCDAWeb = None
    for ridx, Repository in enumerate(AccessInformation):
      AccessURL = Repository['AccessURL']
      if AccessURL is not None:
        Name = AccessURL.get('Name', None)
        if Name is None:
          logger.warning(f"  Warning - No Name in {AccessURL}")

        URL = AccessURL.get('URL', None)
        if URL is None:
          logger.error(f"  Error - No URL in {AccessURL} for {hpde_url}")
          continue

        logger.debug(f"    {ridx+1}. {Name}: {URL}")

        if Name is None or URL is None:
          continue

        if Name == 'CDAWeb':
          if found:
            logger.error(f"      Error - Duplicate AccessURL/Name = 'CDAWeb' in {hpde_url}")
          else:
            if 'ProductKey' in Repository['AccessURL']:
              found = True
              ProductKeyCDAWeb = Repository['AccessURL']['ProductKey']
              if ProductKeyCDAWeb.strip() == '':
                logger.error("      Error - Empty ProductKey")
              else:
                json_file = os.path.join(out_dir, f"{ProductKeyCDAWeb}.json")
                pkl_file = os.path.join(out_dir, f"{ProductKeyCDAWeb}.pkl")
                data = {
                        'id': ProductKeyCDAWeb,
                        'url': hpde_url,
                        'data-file': json_file,
                        'data': data
                      }
                logger.debug(f"      Writing {json_file.replace('.json', '')}.{{json,pkl}}")
                cdawmeta.util.write(json_file, data)
                cdawmeta.util.write(pkl_file, data)

    if ProductKeyCDAWeb is None:
      logger.debug("  x Did not find CDAWeb ProductKey in any Repository")
    else:
      logger.debug(f"  + Found CDAWeb ProductKey: {ProductKeyCDAWeb}")

def _orig_data(dataset, update=True, diffs=False):

  timeout = cdawmeta.CONFIG['metadata']['timeouts']['orig_data']

  wsbase = cdawmeta.CONFIG['urls']['cdasr']
  start = dataset['allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  stop = dataset['allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  headers = {'Accept': 'application/json'}
  return _fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, diffs=diffs)

def _write_catalog(metadata_, id, meta_types):

  logger.info("----")
  logger.info(f"Writing catalog files for: {meta_types}")

  for meta_type in meta_types:
    if meta_type == 'orig_data':
      logger.info("Not creating orig_data catalog file.")
      continue

    data = []
    for dsid in metadata_.keys():
      logger.debug(f"Preparing catalog file for: {dsid}/{meta_type}")

      if meta_type not in metadata_[dsid]:
        # Should not happen. Was happening with threads.
        logger.error("Error: {dsid}: No {meta_type}?. Skipping.")
        continue

      datum = metadata_[dsid][meta_type].get('data', None)

      if datum is None:
        datum_file = metadata_[dsid][meta_type].get('data-file', None)
        if datum_file is None:
          logger.error(f"Error: {dsid}: No data or data-file in metadata")
          continue

        if not isinstance(datum_file, list):
          dataum_files = [datum_file]
        else:
          dataum_files = datum_file

        datum = []
        for dataum_file in dataum_files:
          d = cdawmeta.util.read(dataum_file.replace('.json', '.pkl'), logger=logger)
          datum.append(d)

      if isinstance(datum, list):
        # This is for HAPI metadata, which can have multiple datasets
        for d in datum:
          data.append(d)
      else:
        data.append(datum)

    subdir = ''
    qualifier = ''
    if id is not None:
      subdir = 'catalog-partial'
      qualifier = f'-{id}'

    fname = os.path.join(cdawmeta.DATA_DIR, meta_type, subdir, f'catalog-all{qualifier}')
    cdawmeta.util.write(fname + ".json", data, logger=logger)
    cdawmeta.util.write(fname + ".pkl", data, logger=logger)

    if meta_type == 'hapi':
      from copy import deepcopy
      data_copy = deepcopy(data)
      for datum in data_copy:
        if datum is not None and 'data' in datum:
          # The non-"all" HAPI catalog does not have "info" nodes.
          del datum['data']['info']

      fname = os.path.join(cdawmeta.DATA_DIR, meta_type, subdir, f'catalog{qualifier}.json')
      cdawmeta.util.write(fname, data_copy, logger=logger)

def _write_errors(name=None):
  '''
  Write all errors to a single file if all datasets were requested. Errors
  were already written to log file, but here we need to do additional formatting
  that is more difficult if errors were written as they occur.
  '''

  if name is None:
    for key in cdawmeta.error.errors.keys():
      # If generator used calls to cdawmeta.error(), the generator will have
      # a key.
      _write_errors(name=key)
    return

  errors = ""
  fname = os.path.join(cdawmeta.DATA_DIR, name, f'{name}.errors.log')
  for dsid, vars in cdawmeta.error.errors.copy().items():
    if isinstance(vars, str):
      errors += f"{dsid}: {vars}\n"
      continue
    errors += f"{dsid}:\n"
    for vid, msgs in vars.items():
      errors += f"  {vid}:\n"
      for msg in msgs:
        errors += f"    {msg}\n"
  cdawmeta.util.write(fname, errors, logger=logger)

def _fetch(url, id, what, headers=None, timeout=20, diffs=False, update=False):

  cache_dir = os.path.join(cdawmeta.DATA_DIR, 'CachedSession', what)
  subdir = '' if what == 'all' else 'info'
  json_file = os.path.join(cdawmeta.DATA_DIR, what, subdir, f"{id}.json")
  pkl_file = os.path.join(cdawmeta.DATA_DIR, what, subdir, f"{id}.pkl")

  if not update and os.path.exists(pkl_file):
    msg = "update = False; bypassing CachedSession request and using last result."
    logger.info(msg)
    data = cdawmeta.util.read(pkl_file, logger=logger)
    data['request']["log"] = msg
    return data

  logger.info(f'Getting using requests-cache: {url}')

  result = {'id': id, 'url': url, 'data-file': None, 'data': None}

  get = cdawmeta.util.get_json(url, cache_dir=cache_dir, headers=headers, timeout=timeout, diffs=diffs)

  if get['emsg']:
    emsg = f"Error[{what}]: {id}: {get['emsg']}"
    logger.error(emsg)
    result['error'] = emsg
    return result

  cache_file = os.path.join(cache_dir, get['response'].cache_key + ".json")

  result['id'] = id
  result['url'] = url
  result['data-file'] = json_file
  result['data'] = get['data']

  result['request'] = {}
  result['request']["url"] = url
  result['request']["log"] = _fetch_log(get['response'], get['diff'])
  result['request']["diff"] = get['diff']
  result['request']["file"] = cache_file
  result['request']["file-header"] = dict(get['response'].headers)

  logger.info(result['request']["log"])

  if not os.path.exists(json_file):
    try:
      cdawmeta.util.write(json_file, result, logger=logger)
    except Exception as e:
      logger.error(f"Error writing {json_file}: {e}")

  if not os.path.exists(pkl_file):
    try:
      cdawmeta.util.write(pkl_file, result, logger=logger)
    except Exception as e:
      logger.error(f"Error writing {pkl_file}: {e}")

  return result

def _fetch_log(resp, diff):
  # Combine into single string to deal with parallel processing

  req_cache_headers = {k: v for k, v in resp.request.headers.items() if k in ['If-None-Match', 'If-Modified-Since']}
  res_cache_headers = {k: v for k, v in resp.headers.items() if k in ['ETag', 'Last-Modified', 'Cache-Control', 'Vary']}
  msg = "\n"
  msg += f"  Status code: {resp.status_code}\n"
  msg += f"  From cache: {resp.from_cache}\n"
  if diff and 'diff' in diff:
    msg += f"  Current cache file: {diff['file_now']}\n"
    if 'file_last' in diff:
      msg += f"  Last cache file:    {diff['file_last']}\n"
  msg += "  Request Cache-Related Headers:\n"
  for k, v in req_cache_headers.items():
    print(k,v)
    msg += f"    {k}: {v}\n"
  msg += "  Response Cache-Related Headers:\n"
  for k, v in res_cache_headers.items():
    msg += f"    {k}: {v}\n"
  if diff and 'diff' in diff:
    if diff['diff'] is None or len(diff['diff']) == 0:
      msg += "  Cache diff: None\n"
    else:
      msg += "  Cache diff:\n    "
      json_indented = "\n    ".join(diff['diff'].to_json(indent=2).split('\n'))
      msg += f"{json_indented}\n"
  return msg
