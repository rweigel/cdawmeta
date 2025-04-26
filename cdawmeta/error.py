import os

def exception(dsid, logger, exit_on_exception=False):
  import traceback
  trace = traceback.format_exc().strip()
  home_dir = os.path.expanduser("~")
  trace = trace.replace(home_dir, "~")
  msg = f"{dsid}:\n{trace}"
  error('metadata', dsid, None, 'UnHandledException', msg, logger)
  if exit_on_exception:
    logger.error("Exiting due to exit_on_exception command line argument.")
    os._exit(1)

def error(generator, id, name, etype, msg, logger):

  logger.error(msg)

  if generator not in error.errors:
    error.errors[generator] = {}
  if id not in error.errors[generator]:
    error.errors[generator][id] = {}
  if etype not in error.errors[generator][id]:
    error.errors[generator][id][etype] = {}

  if name is None:
    name = "_"

  if name not in error.errors[generator][id][etype]:
    error.errors[generator][id][etype][name] = []
  error.errors[generator][id][etype][name].append(msg.lstrip())

error.errors = {}

def write_errors(logger, update, id=None, name=None):
  '''
  Write all errors to a single file if all datasets were requested. Errors
  were already written to log file, but here we need to do additional formatting
  that is more difficult if errors were written as they occur.
  '''
  import os
  import cdawmeta

  if name is None:
    for key in error.errors.keys():
      write_errors(logger, update, id=id, name=key)
    return

  output = _create_log(cdawmeta.error.errors[name])

  if name == "metadata":
    subdir = ''
  else:
    subdir = name

  if id is not None:
    if name == "metadata":
      subdir = os.path.join(subdir, 'metadata-partial', id)
    else:
      subdir = os.path.join(subdir, 'partial', id)

  if name == "metadata" and not update:
    # If not updating, there will not be name = "metadata" errors because
    # cache will have been used. We don't want to over-write errors that
    # occurred during the last update.
    logger.info("Not removing or writing errors for 'metadata' because update = False")
    return
  else:
    _remove_errors(subdir, logger)

  for key in output.keys():
    fname = os.path.join(cdawmeta.DATA_DIR, subdir, f'{name}.errors.{key}.log')
    logger.info(f"Writing {fname}")
    cdawmeta.util.write(fname, "\n".join(output[key]))

def _remove_errors(subdir, logger):
  import glob
  import cdawmeta

  dir_name = os.path.join(cdawmeta.DATA_DIR, subdir)
  pattern = f"{dir_name}/*.errors.*log"
  files = glob.glob(pattern)
  for file in files:
    logger.info(f"Removing {file}")
    os.remove(file)

def _create_log(errors):
  output = {"all": []}
  for dsid, etypes in errors.copy().items():
    for etype, variables in etypes.items():
      if etype not in output:
        output[etype] = []

      output['all'].append(f"{dsid}:")
      output[etype].append(f"{dsid}:")

      for vid, msgs in variables.items():
        if len(msgs) == 1:
          line = f"  {vid}: {etype}: {msgs[0]}"
          output['all'].append(line)
          output[etype].append(line)
        else:
          line = f"  {vid} {etype}:"
          output['all'].append(line)
          output[etype].append(line)
          for msg in msgs:
            line = f"    {msg}"
            output['all'].append(line)
            output[etype].append(line)
  return output