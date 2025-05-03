import os

def exception(dsid, logger, exit_on_exception=False):
  import traceback
  trace = traceback.format_exc().strip()
  trace = trace.replace(os.path.expanduser("~"), "~")
  msg = f"{dsid}:\n{trace}"
  error('metadata', dsid, None, 'UnHandledException', msg, logger)
  if exit_on_exception:
    logger.error("Exiting due to exit-on-exception command line argument.")
    os._exit(1)
  return msg

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

def write_errors(logger, update, id, ids, meta_type=None):
  '''
  Writes errors for a given generated metadata type, META_TYPE (e.g., cadence,
  master_resolved, hapi). Files written are:

  * META_TYPE/errors/all.log, containing all errors for all datasets.

  * META_TYPE/errors/ERRORID1.log, errors/ERRORID2.log, etc. Each file contains errors
  for all datasets. If keyword `id` is given (`id` can be a dataset id or a
  a dataset id pattern), then a partial run was performed and the errors are
  written to errors/partial/ID.

  * META_TYPE/info/DATASETID1.errors.log, META_TYPE/info/DATASETID2.errors.log, etc.
  containing all errors for that dataset.

  '''
  import os
  import cdawmeta

  """
  At this point, error.errors has the form
  meta_type1
    dataset_id1
      error_id1
        variable_id1: [error_id1_message]
        ...
      ...
  ...
  """

  if meta_type is None:
    for key in error.errors.keys():
      write_errors(logger, update, id=id, ids=ids, meta_type=key)
    return

  if meta_type == "metadata":
    subdir = ''
  else:
    subdir = os.path.join(meta_type, 'errors')

  if id is not None:
    # Partial run - id is a pattern or a single dataset id.
    if meta_type == "metadata":
      subdir = os.path.join(subdir, 'metadata-partial', id)
    else:
      subdir = os.path.join(subdir, 'partial', id)

  if meta_type == "metadata" and not update:
    # If not updating, there will not be meta_type = "metadata" errors because
    # cache will have been used. We don't want to over-write errors that
    # occurred during the last update.
    msg = "Not removing or writing errors for 'metadata' because update = False"
    logger.info(msg)
    return
  else:
    # Remove errors in META_TYPE/errors
    _remove_errors(subdir, logger)

  errors_ = cdawmeta.error.errors[meta_type]
  """
  errors_ has the form
    dataset_id
      error_id1
        variable_id1: [error_id1_message]
        ...
      ...
  """
  if meta_type != "metadata":
    for dsid in ids:
      fname = os.path.join(cdawmeta.DATA_DIR, meta_type, 'info', f'{dsid}.errors.json')
      if dsid not in errors_ and os.path.exists(fname):
        logger.info(f"Removing previous error file {fname}.")
        os.remove(fname)
      if dsid in errors_:
        logger.info(f"Writing {fname}")
        output = _errors_by_variable(dsid, errors_, meta_type, logger)
        logger.debug("File content:\n" + cdawmeta.util.format_dict(output))
        cdawmeta.util.write(fname, output)

  output = _errors_by_errorid(errors_)

  for key in output.keys():
    # Keys are "all", "error_id1", "error_id2", etc.
    # Write files with keys as names.
    fname = os.path.join(cdawmeta.DATA_DIR, subdir, f'{key}.log')
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

def _errors_by_variable(dsid, errors_, meta_type, logger):
  """Errors indexed by variable id."""

  def read_skt_errors(file_path):
    errors_dict = {"_": []}  # Global errors will be stored under the "_" key
    current_variable = None
    if not os.path.exists(file_path):
      logger.info(f"File {file_path} does not exist.")
      return None

    with open(file_path, "r") as file:
      lines = file.readlines()

    for line in lines:
      indent_level = len(line) - len(line.lstrip())

      # Check for global errors
      if line.startswith("Global errors:"):
        current_variable = "_"  # Use "_" as the key for global errors

      # Check for non-ISTP-compliant variables
      elif line.startswith("The following variables are not ISTP-compliant:"):
        current_variable = None  # Reset current variable

      # Detect variable names (e.g., Epoch, RADIUS)

      elif indent_level == 1 and current_variable != '_':
        current_variable = line.strip()
        errors_dict[current_variable] = []  # Initialize a list for this variable

      # Add errors or warnings to the current variable
      elif current_variable is not None:
        errors_dict[current_variable].append(line.strip())

    return errors_dict

  if meta_type == "master_resolved":
    # These files are created by etc/skterrors.sh
    import cdawmeta
    fname_master = os.path.join(cdawmeta.DATA_DIR, "master", 'info', f'{dsid}.errors.skt.log')
    skt = read_skt_errors(fname_master)

  result = {}

  if dsid not in errors_:
    return result

  for error_id, variables in errors_[dsid].items():
    for variable_id, messages in variables.items():
      if variable_id not in result:
        result[variable_id] = {}
      # Assuming there's only one message per list
      result[variable_id][error_id] = messages[0]
      if meta_type == "master_resolved":
        # Add SKT errors if present
        if skt is not None and variable_id in skt:
          result[variable_id]["SKT"] = skt[variable_id]

  return result

def _errors_by_errorid(errors):
  """
  Converts input error dict from
  dsid1
    error_id1
      varid1: error_id1msg
      varid2: error_id1msg
      ...
    ...
  dsid2
    ...

  to

  all
    dsid1 | etypeA:
      varid1: etypeAmsg
      varid1: etypeBmsg
      ...
    ...
  etypeA
    dsid1
      varid1: error_id1msg
      varid1: error_id1msg
    ...
  ...
  """

  output = {"all": []}
  # errors.copy().items() = 
  #   [ ('dsid1', 'etypeA'), ('dsid1', 'etypeC'), 
  #     ('dsid2', 'etypeA'), ('dsid2', 'etypeB')
  #   ]
  for dsid, etypes in errors.copy().items():
    for etype, variables in etypes.items():
      if etype not in output:
        output[etype] = []

      output['all'].append(f"{dsid} | {etype}:")
      output[etype].append(f"{dsid} | {etype}:")

      for vid, msgs in variables.items():
        if len(msgs) == 1:
          line = f"  {vid}: {msgs[0]}"
          output['all'].append(line)
          output[etype].append(line)
        else:
          line = f"  {vid}:"
          output['all'].append(line)
          output[etype].append(line)
          for msg in msgs:
            line = f"    {msg}"
            output['all'].append(line)
            output[etype].append(line)

  return output