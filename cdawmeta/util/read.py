def read(fname, logger=None):
  import os
  import json

  if logger is not None:
    logger.info(f"Reading {fname}")

  if '.pkl' == os.path.splitext(fname)[1]:
    import pickle
    try:
      f = open(fname, 'rb')
      data = pickle.load(f)
      if logger is not None:
        logger.info(f"Read and parsed {fname}")
      return data
    except Exception as e:
      msg = f"pickle.load({fname}) raised: {e}"
      if logger is not None:
        logger.info(msg)
      raise e

  try:
    f = open(fname, encoding='utf-8')
  except Exception as e:
    msg = f"Error opening {fname}: {e}"
    if logger is not None:
      logger.error(msg)
    raise e

  if '.json' == os.path.splitext(fname)[1]:
    try:
      data = json.load(f)
      if logger is not None:
        logger.info(f"Read and parsed {fname}")
    except Exception as e:
      msg = f"json.load({fname}) raised: {e}"
      if logger is not None:
        logger.info(msg)
      raise e
  else:
    try:
      data = f.readlines()
      if logger is not None:
        logger.info(f"Read {fname}")
    except:
      msg = f"Error reading {fname}: {e}"
      if logger is not None:
        logger.error(msg)
      raise e

  f.close()

  return data
