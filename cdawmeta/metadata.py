import os
import re
import glob
import traceback

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

def ids(id=None, id_skip=None, update=False):
  '''Generate list of CDAWeb dataset IDs.

  IDs are generated from all.xml.

  If `id` is `None`, all IDs are returned. `id` is treated a regular expression
  if it starts with `^`.

  `id_skip` is a regular expression string; IDs that match `id_skip` are not returned.

  `update` is a boolean. If `True`, all.xml is updated before generating IDs.
  '''

  # Needed to set logger for any called underscore functions.
  # TODO: Find a better way to handle this.
  logger = _logger()

  def _remove_skips(id_skip, ids):
    id_skip_default = cdawmeta.util.get_path(cdawmeta.CONFIG, ['hapi', 'id_skip'])
    if id_skip is None and id_skip_default is None:
      return ids

    # TODO: Repeated code.
    ids_original = ids
    if id_skip is not None:
      logger.info(f"Removing ids that match id_skip {id_skip}")
      regex = re.compile(id_skip)
      ids = [id for id in ids if not regex.match(id)]
      logger.info(f"# of ids removed: {len(ids_original) - len(ids)}")

    ids_original = ids
    if id_skip_default is not None:
      logger.info(f"Removing ids that match id_skip in hapi.conf: {id_skip_default}")
      regex = re.compile(id_skip_default)
      ids = [id for id in ids if not regex.match(id)]
      logger.info(f"# of ids removed: {len(ids_original) - len(ids)}")

    return ids

  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)
  ids_all = datasets_all.keys()

  if id is None:
    return _remove_skips(id_skip, list(ids_all))

  if isinstance(id, str):
    if id.startswith('^'):
      regex = re.compile(id)
      ids_reduced = [id for id in ids_all if regex.match(id)]
      if len(ids_reduced) == 0:
        raise ValueError(f"Error: id = {id}: No matches.")
      else:
        logger.info(f"# of id regex matches to {id}: {len(ids_reduced)}")
    elif id not in ids_all:
      raise ValueError(f"Error: id = {id}: Not found.")
    else:
      ids_reduced = [id]

  if id_skip is None:
    return ids_reduced

  return _remove_skips(id_skip, ids_reduced)

def metadata(id=None, id_skip=None, meta_type=None, embed_data=True,
             write_catalog=False,
             update=False, update_skip='',
             regen=False, regen_skip='',
             max_workers=3,
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

    if 'allxml' not in meta_types:
      meta_types = ['allxml', *meta_types]

  logger.info(f"Given requested meta_type = {meta_type_requested}, need to create: {meta_types}")

  if not update and not regen:
    # TODO: Read cached catalog files.
    pass

  if update:
    if regen:
      logger.warning("update=True => regen=True. No need to set regen=True.")
    regen = True
    regen_skip = update_skip

  if diffs and not update:
    logger.warning("diffs=True but update=False. No diffs can be computed.")

  dsids = ids(id=id, id_skip=id_skip, update=update)

  # Create base datasets using info in all.xml
  allxml = _allxml(update=update)
  datasets_all = _datasets(allxml)

  not_generated = ['allxml', 'master', 'orig_data', 'spase', 'spase_hpde_io']
  mloggers = {}
  for meta_type in meta_types:
    if meta_type in not_generated:
      continue
    mloggers[meta_type] = cdawmeta.logger(meta_type, log_level=log_level)

  if 'spase_hpde_io' in meta_types:
    import git
    repo_path = os.path.join(cdawmeta.DATA_DIR, 'hpde.io')
    up_to_date = False
    repo_url = cdawmeta.CONFIG['urls']['hpde.io']
    if not os.path.exists(repo_path):
      logger.info(f"Cloning {repo_url} into {repo_path}")
      logger.info("Initial clone takes ~30s")
      git.Repo.clone_from(repo_url, repo_path, depth=1)
      _spase_hpde_io(update=True, diffs=diffs)
      up_to_date = True

    if update and not up_to_date:
      repo = git.Repo(repo_path)
      origin = repo.remotes.origin
      origin.fetch()
      logger.info(f"Pulling from {repo_url}")
      repo.git.merge('origin/master')
      _spase_hpde_io(update=True, diffs=diffs)

  def step_needed(meta_type, step, update, update_skips):
    if meta_type in update_skips:
      if update:
        logger.info(f"Setting regen=False for {meta_type} {step} because in {step}_skip.")
      return False
    return update

  def get_one(dataset, mloggers):

    if 'master' in meta_types or 'spase' in meta_types:
      # 'spase' needs 'master' to get spase_DatasetResourceID, so this must be before.
      update_ = step_needed('master', 'update', update, update_skip)
      update_ = update_ or step_needed('spase', 'update', update, update_skip)
      dataset['master'] = _master(dataset, update=update_, diffs=diffs)

    if 'orig_data' in meta_types:
      update_ = step_needed('orig_data', 'update', update, update_skip)
      dataset['orig_data'] = _orig_data(dataset, update=update_, diffs=diffs)

    if 'spase' in meta_types:
      update_ = step_needed('spase', 'update', update, update_skip)
      dataset['spase'] = _spase(dataset, update=update_, diffs=diffs)

    if 'spase_hpde_io' in meta_types:
      # Here we never update b/c update is done in earlier call to _spase_hpde_io
      dataset['spase_hpde_io'] = _spase_hpde_io(id=dataset['id'], update=False)

    for meta_type in meta_types:
      if meta_type in not_generated:
        continue

      logger.info(f"Generating: {meta_type} for {dataset['id']}")
      update_ = step_needed(meta_type, 'update', update, update_skip)
      regen_ = step_needed(meta_type, 'regen', regen, regen_skip)

      dataset[meta_type] = cdawmeta.generate(dataset, meta_type, mloggers[meta_type],
                                             update=update_, regen=regen_)

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
      try:
        get_one(datasets_all[dsid], mloggers)
      except:
        msg = f"{dsid}: {traceback.print_exc()}"
        cdawmeta.error('metadata', dsid, None, 'UnHandledException', msg, logger)
  else:
    def call_get_one(dsid):
      try:
        get_one(datasets_all[dsid], mloggers)
      except:
        msg = f"{dsid}: {traceback.print_exc()}"
        cdawmeta.error('metadata', dsid, None, 'UnHandledException', msg, logger)

      return dsid

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(call_get_one, dsids)

  metadata_ = {key: datasets_all[key] for key in dsids}

  if id is None:
    # Don't write error logs if id = None because not full run.
    if regen or update:
      # Only write error logs if regenerating or updating. If not, cache is
      # used and errors are not encountered because no metadata is generated.
      cdawmeta.write_errors(logger, update)
  else:
    logger.info("Not writing errors because id is not None (not full run).")

  if write_catalog:
    if meta_type_requested is None:
      _write_catalog(metadata_, id, meta_types)
    else:
      _write_catalog(metadata_, id, meta_type_requested)

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
      msg = f'{id}: No mastercdf node in all.xml'
      cdawmeta.error('metadata', id, None, 'allxml.NoMasterCDF', msg, logger)
      continue

    if isinstance(dataset_allxml['mastercdf'], list):
      if not id.endswith('_MOVIES'):
        msg = f'Warning[all.xml]: Not implemented: {id}: More than one mastercdf referenced in all.xml mastercdf node.'
        logger.warning(msg)
      continue

    if '@ID' not in dataset_allxml['mastercdf']:
      msg = f"{id}: No @ID attribute in all.xml 'mastercdf' node"
      cdawmeta.error('metadata', id, None, 'allxml.No@IDAttribute', msg, logger)
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
  allxml = _fetch(allurl, 'all', 'all', referrer='config.json', timeout=timeout, update=update, diffs=diffs)

  # Curry result
  _allxml.allxml = allxml

  return allxml

def _master(dataset, update=False, diffs=False):

  timeout = cdawmeta.CONFIG['metadata']['timeouts']['master']

  mastercdf = dataset['allxml']['mastercdf']['@ID']
  url = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

  master = _fetch(url, dataset['id'], 'master', referrer=url, timeout=timeout, update=update, diffs=diffs)

  master['data'] = cdawmeta.restructure.master(master['data'], url, logger=logger)

  return master

def _spase(dataset, update=True, diffs=False):

  id = dataset['id']

  master = dataset['master']
  if 'data' not in master:
    msg = 'No spase_DatasetResourceID because no master'
    return {'id': id, 'error': msg, 'data-file': None, 'data': None}

  timeout = cdawmeta.CONFIG['metadata']['timeouts']['spase']

  global_attributes = master['data']['CDFglobalAttributes']
  if 'spase_DatasetResourceID' not in global_attributes:
    msg = f"{id}: No spase_DatasetResourceID attribute in {master['url']}."
    cdawmeta.error('metadata', id, None, 'master.NoSpaseDatasetResourceID', msg, logger)
    return {'id': id, 'error': msg, 'data-file': None, 'data': None}

  if 'spase_DatasetResourceID' in global_attributes:
    spase_id = global_attributes['spase_DatasetResourceID']
    if spase_id and not spase_id.startswith('spase://'):
      msg = f"{id}: spase_DatasetResourceID = '{spase_id}' does not start with 'spase://' in {metadatum['url']}"
      cdawmeta.error('metadata', id, None, 'master.InvalidSpaseDatasetResourceID', msg, logger)
      return {'id': id, 'error': msg, 'data-file': None, 'data': None}

    url = spase_id.replace('spase://', 'https://hpde.io/') + '.json'

  spase = _fetch(url, id, 'spase', referrer=master['url'], timeout=timeout, diffs=diffs, update=update)

  return spase

def _spase_hpde_io(id=None, update=True, diffs=False):

  out_dir = os.path.join(cdawmeta.DATA_DIR, 'spase_hpde_io', 'info')

  if id is not None and not update:
    pkl_file = os.path.join(out_dir, f"{id}.pkl")
    if os.path.exists(pkl_file):
      return cdawmeta.util.read(pkl_file, logger=logger)
    else:
      logger.info(f"No SPASE record for {id} in ")
      return None

  pattern = "data/hpde.io/**/NumericalData/**/*.json"
  logger.info(f"Getting list of files that match '{pattern}'")
  files = glob.glob(pattern, recursive=True)
  logger.info(f"{len(files)} NumericalData SPASE records before removing Deprecated")
  for file in files:
    if 'Deprecated' in file:
      del files[files.index(file)]
  logger.info(f"{len(files)} NumericalData SPASE records after removing Deprecated")

  logger.info(f"Reading {len(files)} NumericalData SPASE records.")
  n_found = 0
  for file in files:

    logger.debug(f'  Reading {file}')
    data = cdawmeta.util.read(file)

    ResourceID = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'ResourceID'])
    if ResourceID is None:
      cdawmeta.error('metadata', id, None, 'spase.hpde_io.NoResourceID', f"No ResourceID in {file}", logger)
      continue

    hpde_url = f'{ResourceID.replace("spase://", "http://hpde.io/")}'

    # Flattens AccessInformation so is a list of objects, each with one AccessURL.
    data = cdawmeta.restructure.spase(data, logger=logger)
    AccessInformation = cdawmeta.util.get_path(data, ['Spase', 'NumericalData', 'AccessInformation'])

    if AccessInformation is None:
      cdawmeta.error('metadata', id, None, 'spase.hpde_io.AccessInformation', f"No AccessInformation in {file}", logger)
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
          logger.warning(f"  No AccessURL/Name in {hpde_url}")

        URL = AccessURL.get('URL', None)
        if URL is None:
          msg = f"  No URL in AccessURL with Name '{Name}' in {hpde_url}"
          cdawmeta.error('metadata', id, None, 'spase.hpde_io.NoURLInAccessURL', msg, logger)
          continue

        logger.debug(f"    {ridx+1}. {Name}: {URL}")

        if Name is None or URL is None:
          continue

        if Name == 'CDAWeb':
          if found:
            msg = f"      Duplicate AccessURL/Name = 'CDAWeb' in {hpde_url}"
            cdawmeta.error('metadata', id, None, 'spase.hpde_io.DuplicateAccessURLName', msg, logger)
          else:
            n_found += 1
            if 'ProductKey' in Repository['AccessURL']:
              found = True
              ProductKeyCDAWeb = Repository['AccessURL']['ProductKey']
              if ProductKeyCDAWeb.strip() == '':
                msg = f"      ProductKey.strip() = '' in AccessURL with Name '{Name}' in {hpde_url}"
                cdawmeta.error('metadata', id, None, 'spase.hpde_io.EmptyProductKey', msg, logger)
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

  logger.info(f"Found {n_found} NumericalData SPASE for CDAWeb.")

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
  logger.info(f"Writing catalog files for meta types: {meta_types}")

  for meta_type in meta_types:
    if meta_type == 'orig_data':
      logger.info("Not creating orig_data catalog file.")
      continue

    data = []
    for dsid in metadata_.keys():
      logger.debug(f"Preparing catalog file for: {dsid}/{meta_type}")

      if meta_type not in metadata_[dsid]:
        # Should not happen.
        msg = f"No metadatum/{meta_type} for '{dsid}'."
        cdawmeta.error('metadata', dsid, None, 'UnHandledException', msg, logger)
        continue

      datum = metadata_[dsid][meta_type].get('data', None)

      if datum is None:
        datum_file = metadata_[dsid][meta_type].get('data-file', None)
        if datum_file is None:
          msg = f"No data and no data-file in metadatum/{meta_type} for '{dsid}'."
          logger.warning(msg)
          continue

        if not isinstance(datum_file, list):
          dataum_files = [datum_file]
        else:
          dataum_files = datum_file

        datum = []
        for dataum_file in dataum_files:
          datum_file = dataum_file.replace('.json', '.pkl')
          logger.debug(f"  Reading {dataum_file}")
          d = cdawmeta.util.read(datum_file)
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
    logger.info(f'Writing {fname}.json')
    cdawmeta.util.write(fname + ".json", data)
    logger.info(f'Writing {fname}.pkl')
    cdawmeta.util.write(fname + ".pkl", data)

    if meta_type == 'hapi':
      from copy import deepcopy
      data_copy = deepcopy(data)
      for datum in data_copy:
        if datum is not None:
          # The non-"all" HAPI catalog does not have "info" nodes.
          if 'info' not in datum:
            msg = f"No 'info' node in HAPI metadata: {datum}"
            cdawmeta.error('metadata', datum['id'], None, 'HAPI.NoInfo', msg, logger)
          else:
            del datum['info']

      fname = os.path.join(cdawmeta.DATA_DIR, meta_type, subdir, f'catalog{qualifier}')
      logger.info(f'Writing {fname}.json')
      cdawmeta.util.write(fname + ".json", data_copy)
      logger.info(f'Writing {fname}.pkl')
      cdawmeta.util.write(fname + ".pkl", data_copy)

def _fetch(url, id, meta_type, referrer=None, headers=None, timeout=20, diffs=False, update=False):

  cache_dir = os.path.join(cdawmeta.DATA_DIR, 'CachedSession', meta_type)
  subdir = '' if meta_type == 'all' else 'info'
  json_file = os.path.join(cdawmeta.DATA_DIR, meta_type, subdir, f"{id}.json")
  pkl_file = os.path.join(cdawmeta.DATA_DIR, meta_type, subdir, f"{id}.pkl")

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
    emsg = f"{id}: {get['emsg']}"
    if referrer is not None:
      emsg += f"; Referring document: {referrer}"
    cdawmeta.error('metadata', id, None, f'{meta_type}.FetchError', emsg, logger)
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
      msg = f"Error writing {json_file}: {e}"
      cdawmeta.error('metadata', id, None, 'WriteError', msg, logger)

  if not os.path.exists(pkl_file):
    try:
      cdawmeta.util.write(pkl_file, result, logger=logger)
    except Exception as e:
      msg = f"Error writing {pkl_file}: {e}"
      cdawmeta.error('metadata', id, None, 'WriteError', msg, logger)

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
