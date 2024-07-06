def write(fname, data, logger=None):
  import os

  import cdawmeta
  cdawmeta.util.mkdir(os.path.dirname(fname), logger=logger)

  if logger is not None:
    logger.info(f"Writing {fname}")

  if '.pkl' == os.path.splitext(fname)[1]:
    import pickle
    try:
      with open(fname, 'wb') as f:
        pickle.dump(data, f)
      if logger is not None:
        logger.info(f"Wrote {fname}")
    except Exception as e:
      msg = f"pickle.dump() raised: {e}"
      if logger is not None:
        logger.error(msg)
      raise e
    return

  if '.json' == os.path.splitext(fname)[1]:
    import json
    try:
      data = json.dumps(data, indent=2, ensure_ascii=False)
    except Exception as e:
      msg = f"json.dumps() raised: {e}"
      if logger is not None:
        logger.error(msg)
      raise e

  try:
    f = open(fname, 'w', encoding='utf-8')
  except Exception as e:
    os.remove(fname)
    msg = f"Error opening {fname}: {e}"
    if logger is not None:
      logger.error(msg)
    raise e

  try:
    f.write(data)
  except Exception as e:
    os.remove(fname)
    msg = f"Error writing {fname}: {e}"
    if logger is not None:
      logger.error(msg)
    raise e

  if logger is not None:
    logger.info(f"Wrote {fname}")
