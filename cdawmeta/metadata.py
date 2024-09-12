import os
import re
import shutil
import deepdiff

import cdawmeta

# Can't call logger = cdawmeta.logger('cdaweb') here because it calls cdawmeta.DATA_DIR
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

def metadata(meta_type=None, id=None, skip=None, embed_data=False, write_catalog=True, update=False, regen=False, max_workers=1, diffs=False, log_level='info'):

  if not update and not regen:
    # TODO: Read cached catalog files.
    #       Need to account for fact that full catalog for orig_data was not
    #       created and allxml is a single file and there is no info/ subdir.
    pass

  if update:
    regen = True

  logger = _logger(log_level=log_level)

  if diffs and not update:
    logger.warning("diffs=True but update=False. No diffs can be computed.")

  dsids = ids(id=id, skip=skip, update=update)

  # Create base datasets using info in all.xml
  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)

  meta_types_generated = cdawmeta._generate.generators
  meta_types = ['master', 'orig_data', 'spase', *meta_types_generated]

  def get_one(dataset):
    dataset['master'] = _master(dataset, update=update, diffs=diffs)
    dataset['orig_data'] = _orig_data(dataset, update=update, diffs=diffs)
    dataset['spase'] = _spase(dataset, update=update, diffs=diffs)

    # TODO: Here we generate all metadata types. This function has an unused
    #       input of meta_type, which could be a string or an array of strings.
    #       In this case, we should only generate the metadata types in meta_type.
    #       The complication is that some metadata types depend on others, so
    #       we would need to determine the order and which ones to generate.
    for meta_type in meta_types_generated:
      logger.info("Generating: " + meta_type)
      _logger = cdawmeta.logger(meta_type, log_level=log_level)
      dataset[meta_type] = cdawmeta.generate(dataset, meta_type, _logger, update=update, regen=regen)

    if not write_catalog and not embed_data:
      # If no catalog is being written and data is not being embedded, remove
      # data from metadata and keep only id and data-file in object.
      for meta_type in meta_types:
        if 'data' in dataset[meta_type]:
          del dataset[meta_type]['data']

  if max_workers == 1 or len(dsids) == 1:
    for dsid in dsids:
      get_one(datasets_all[dsid])
  else:
    from concurrent.futures import ThreadPoolExecutor
    def call(dsid):
      try:
        get_one(datasets_all[dsid])
      except Exception as e:
        import traceback
        logger.error(f"Error: {datasets_all['id']}: {traceback.print_exc()}")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call, dsids)

  metadata_ = {key: datasets_all[key] for key in dsids}

  if write_catalog:
    meta_types.remove('orig_data')
    _write_catalog(metadata_, id, meta_types)
    if not embed_data:
      for key in metadata_.keys():
        datum = cdawmeta.util.get_path(metadata_[key], ['meta_type', 'data'])
        if datum is not None:
          del datum

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

  allurl = cdawmeta.CONFIG['metadata']['allurl']
  allxml = _fetch(allurl, 'all', 'all', timeout=timeout, update=update, diffs=diffs)

  # Curry result
  _allxml.allxml = allxml

  return allxml

def _master(dataset, update=False, diffs=False):

  restructure=True
  timeout = cdawmeta.CONFIG['metadata']['timeouts']['master']

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  master = _fetch(url, dataset['id'], 'master', timeout=timeout, update=update, diffs=diffs)
  if restructure and 'data' in master:
    master['data'] = cdawmeta.restructure.master(master['data'], logger=logger)
  return master

def _spase(dataset, update=True, diffs=False):

  master = dataset['master']
  restructure = True
  timeout = cdawmeta.CONFIG['metadata']['timeouts']['spase']

  id = master['id']
  if 'data' not in master:
    return {'error': 'No spase_DatasetResourceID because no master'}

  global_attributes = master['data']['CDFglobalAttributes']
  if 'spase_DatasetResourceID' not in global_attributes:
    msg = f"Error[master]: {master['id']}: Missing or invalid spase_DatasetResourceID attribute in master"
    logger.error(msg)
    return {'error': msg}

  if 'spase_DatasetResourceID' in global_attributes:
    spase_id = global_attributes['spase_DatasetResourceID']
    if spase_id and not spase_id.startswith('spase://'):
      msg = f"Error[master]: {master['id']}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://'"
      logger.error(msg)
      return {'error': msg}
    url = spase_id.replace('spase://', 'https://hpde.io/') + '.json'

  spase = _fetch(url, id, 'spase', timeout=timeout, diffs=diffs, update=update)

  #if restructure and 'data' in spase:
  #  spase['data'] = cdawmeta.restructure.spase(spase['data'], logger=logger)
  return spase

def _orig_data(dataset, update=True, diffs=False):

  timeout = cdawmeta.CONFIG['metadata']['timeouts']['orig_data']

  wsbase = cdawmeta.CONFIG['metadata']['wsbase']
  start = dataset['allxml']['@timerange_start'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  stop = dataset['allxml']['@timerange_stop'].replace(" ", "T").replace("-","").replace(":","") + "Z"
  url = wsbase + dataset["id"] + "/orig_data/" + start + "," + stop

  headers = {'Accept': 'application/json'}
  return _fetch(url, dataset['id'], 'orig_data', headers=headers, timeout=timeout, update=update, diffs=diffs)

def _write_catalog(metadata_, id, meta_types):

  for meta_type in meta_types:
    data = []
    for dsid in metadata_.keys():
      datum = cdawmeta.util.get_path(metadata_[dsid], ['meta_type', 'data'])

      if data is None:
        logger.error(f"Error: {dsid}: No {meta_type} metadata")
        continue

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

  result = {'id': id, 'url': url}

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
