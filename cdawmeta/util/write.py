import os
import csv
import json
import pickle

def write(fname, data, logger=None):
  import cdawmeta

  #if not os.path.isabs(fname):
  #  fname = os.path.abspath(fname)

  cdawmeta.util.mkdir(os.path.dirname(fname), logger=logger)

  if logger is not None:
    logger.info(f"Writing {fname}")

  ext = os.path.splitext(fname)[1]
  exception = None

  if '.pkl' == ext:
    try:
      with open(fname, 'wb') as f:
        pickle.dump(data, f)
      _finish(fname, logger=logger)
      return
    except Exception as e:
      emsg = f"pickle.dump() raised: {e}"
      _finish(fname, logger=logger, e=e, emsg=emsg)

  iscsv = isinstance(data, list) or isinstance(data, tuple)
  if '.csv' == ext and iscsv:
    try :
      with open(fname, 'w', newline='') as f:
        # https://github.com/python/cpython/issues/97503 for escapechar need
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerows(data)
        _finish(fname, logger=logger)
        return
    except:
      try:
        with open(fname, 'w', newline='') as f:
          # https://github.com/python/cpython/issues/97503
          writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL, escapechar='\\')
          writer.writerows(data)
        _finish(fname, logger=logger)
        return
      except Exception as e:
        exception = e
        emsg = f"csv.writerows() raised: {e}"
        with open(fname, 'w', newline='') as f:
          writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
          for row in data:
            try:
              writer.writerow(row)
            except Exception as e:
              print(row)
              raise e

  if '.json' == ext:
    try:
      data = json.dumps(data, indent=2, ensure_ascii=False)
    except Exception as e:
      emsg = f"json.dumps() raised: {e}"
      _finish(fname, logger=logger, e=e, emsg=emsg)

  try:
    f = open(fname, 'w', encoding='utf-8')
  except Exception as e:
    os.remove(fname)
    emsg = f"f.open() raised: {e}"
    _finish(fname, logger=logger, e=e, emsg=emsg)

  try:
    f.write(data)
    _finish(fname, logger=logger)
  except Exception as e:
    os.remove(fname)
    emsg = f"Error writing {fname}: {e}"
    _finish(fname, logger=logger, e=e, emsg=emsg)

def _finish(fname, logger=None, e=None, emsg=None):
  if e is not None:
    if logger is not None:
      logger.error(emsg)
    else:
      print(emsg)
    raise e

  if logger is not None:
    logger.info(f"Wrote {fname}")
