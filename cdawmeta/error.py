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

def write_errors(logger, update, name=None):
  '''
  Write all errors to a single file if all datasets were requested. Errors
  were already written to log file, but here we need to do additional formatting
  that is more difficult if errors were written as they occur.
  '''
  import os
  import glob
  import cdawmeta

  if name is None:
    for key in error.errors.keys():
      # If generator used calls to cdawmeta.error(), the generator will have
      # a key.
      write_errors(logger, update, name=key)
    return

  subdir = name
  if name == "metadata":
    subdir = ''
  if (name == "metadata" and not update):
    logger.info("Not removing errors for 'metadata' because update = False")
  else:
    dir_name = os.path.join(cdawmeta.DATA_DIR, subdir)
    pattern = f"{dir_name}/*.errors.*log"
    files = glob.glob(pattern)
    for file in files:
      logger.info(f"Removing {file}")
      #os.remove(file)

  errors = cdawmeta.error.errors[name]
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

  for key in output.keys():
    subdir = name
    if name == "metadata":
      subdir = ''
      if not update:
        logger.info("Not writing errors for 'metadata' because update = False")
        # If not updating, there will not be name = "metadata" errors because
        # cache will have been used. We don't want to over-write errors that
        # occurred during the last update.
        continue
    fname = os.path.join(cdawmeta.DATA_DIR, subdir, f'{name}.errors.{key}.log')
    logger.info(f"Writing {fname}")
    cdawmeta.util.write(fname, "\n".join(output[key]))

