import os
import cdflib
import cdawmeta

def url2file(url):
  return url.replace("https://cdaweb.gsfc.nasa.gov/", "")

def open_cdf(file, logger=None, use_cache=True, cache_dir=None):

  if file.startswith('http'):
    kwargs = {
      'url2file': url2file,
      'logger': logger,
      'use_cache': use_cache,
      'cache_dir': cache_dir
    }
    file_path = cdawmeta.util.get_file(file, **kwargs)
    if file_path is None:
      return None

  try:
    cdffile = cdflib.CDF(file_path)
    return cdffile
  except Exception as e:
    if logger is not None:
      logger.error(f"Error opening {file_path}: {e}")
    return None

def read_cdf_depend_0s(file, _return='names', logger=None, use_cache=True, cache_dir=None):

  cdffile = open_cdf(file, logger=logger, use_cache=use_cache, cache_dir=None)
  if cdffile is None:
    return None

  meta = {}
  depend_0s = []
  info = cdffile.cdf_info()
  rVariables = info.rVariables
  zVariables = info.zVariables
  for variable in rVariables + zVariables:
    meta = cdffile.varattsget(variable=variable)
    if 'DEPEND_0' in meta:
      depend_0s.append(meta['DEPEND_0'])

  depend_0s = list(set(depend_0s))
  if _return == 'names':
    return depend_0s

  if _return == 'data':
    data = {}
    for depend_0 in depend_0s:
      try:
        data[depend_0] = cdffile.varget(variable=depend_0)
      except Exception as e:
        if logger is not None:
          logger.error(f"Error reading {depend_0}: {e}")
        data[depend_0] = None
    return data

def read_cdf_meta(file, subset=False, logger=None, use_cache=True):

  cdffile = open_cdf(file, logger=logger, use_cache=use_cache)
  if cdffile is None:
    return None

  meta = {}
  depend_0s = []
  info = cdffile.cdf_info()
  rVariables = info.rVariables
  zVariables = info.zVariables
  for variable in rVariables + zVariables:
    meta[variable] = cdffile.varattsget(variable=variable)
    if subset and 'DEPEND_0' in meta[variable]:
      depend_0s.append(meta[variable]['DEPEND_0'])

  if subset:
    depend_0s = list(set(depend_0s))
    meta_subsetted = {}
    for depend_0 in depend_0s:
      meta_subsetted[depend_0] = {}
      for variable in rVariables + zVariables:
        if 'DEPEND_0' in meta[variable] and meta[variable]['DEPEND_0'] == depend_0:
          meta_subsetted[depend_0][variable] = meta[variable]
          for key in meta[variable]:
            if 'PTR' in key:
              meta_subsetted[depend_0][variable][key] = cdffile.varget(variable=meta[variable][key])
    return meta_subsetted

  return meta

def read_cdf(file, parameters, start=None, stop=None, logger=None, use_cache=True):

  meta = []
  data = []

  cdffile = open_cdf(file, logger=logger, use_cache=use_cache)
  if cdffile is None:
    return None

  depend_0s = []
  parameters = parameters.split(",")
  for parameter in parameters:
    data.append(cdffile.varget(variable=parameter))
    meta.append(cdffile.varattsget(variable=parameter))
    depend_0s.append(meta[-1]['DEPEND_0'])

  udepend_0s = list(set(depend_0s)); # Unique depend_0s
  assert len(udepend_0s) == 1, 'Multiple DEPEND0s not implemented. Found: ' + ", ".join(udepend_0s)

  epoch = cdffile.varget(variable=depend_0s[0])
  time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

  if isinstance(time, str):
    time = [time]

  start_idx = 0
  stop_idx = len(time)

  if start is not None or stop is not None:
    n = len(time)
    startr = start[0:n-1]
    stopr = stop[0:n-1]
    start_found = False
    for idx, t in enumerate(time):
      if start_found == False and t >= startr:
        start_found = True
        start_idx = idx
      if t >= stopr:
        stop_idx = idx + 1
        break

    time = time[start_idx:stop_idx]

  return time, data, meta
