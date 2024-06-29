def write(fname, data, logger=None):
  import os

  import cdawmeta
  cdawmeta.util.mkdir(os.path.dirname(fname), logger=logger)

  if '.json' == os.path.splitext(fname)[1]:
    import json
    try :
      data = json.dumps(data, indent=2, ensure_ascii=False)
    except Exception as e:
      msg = f"json.dumps({data}) raised: {e}"
      if logger is not None:
        logger.error(msg)
      raise e

  if logger is not None:
    logger.info(f"Writing {fname}")

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
    msg = f"Error writing {fname}: {e}"
    if logger is not None:
      logger.error(msg)
    raise e

  if logger is not None:
    logger.info(f"  Wrote {fname}")
