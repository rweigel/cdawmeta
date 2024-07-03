def get_file(url, logger=None, url2file=None, use_cache=True, cache_dir=None):

  import os
  import secrets
  from urllib.request import urlopen
  from shutil import copyfileobj

  import cdawmeta

  length=16*1024

  if url2file is not None:
    file_name = url2file(url)
  else:
    file_name = url.split('/')[-1]

  if cache_dir is not None:
    file_name = os.path.join(cache_dir, file_name)

  if use_cache and os.path.exists(file_name):
    if logger is not None:
      logger.info(f"Using cached file: {file_name}")

  cdawmeta.util.mkdir(os.path.dirname(file_name), logger=logger)

  if logger is not None:
    logger.info(f"Downloading {url} to {file_name}")

  file_name_tmp = file_name + "." + secrets.token_hex(4) + ".tmp"

  begin = cdawmeta.util.tick()

  try:
    req = urlopen(url)
  except Exception as e:
    if logger is not None:
      logger.error(f"Error: {url}: {e}")
    return None

  try:
    with open(file_name_tmp, 'wb') as fp:
      copyfileobj(req, fp, length)
  except Exception as e:
    if logger is not None:
      logger.error(f"Error: {url}: {e}")
    os.remove(file_name_tmp)
    return None

  if logger is not None:
    logger.info(f"Got: {cdawmeta.util.tock(begin):.2f}s {url}")

  try:
    os.rename(file_name_tmp, file_name)
  except Exception as e:
    if logger is not None:
      logger.error(f"Error: {url}: {e}")
    os.remove(file_name_tmp)
    return None

  headers = dict(req.getheaders())
  cdawmeta.util.write(file_name + ".headers.json", headers, logger=logger)

  return file_name
