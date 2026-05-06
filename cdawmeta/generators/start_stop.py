import cdawmeta

dependencies = ['cdfmetafile']
# The above dependency can be modified to 'orig_data'
# in the call to metadata.py/metadata() or by a command-line option.

def start_stop(metadatum, logger):

  id = metadatum['id']
  file_list_name = dependencies[0]

  # Check if dependency was switched to orig_data.
  deps = cdawmeta.generators.dependencies['start_stop']
  if 'orig_data' in deps:
    file_list_name = 'orig_data'

  if file_list_name == 'cdfmetafile':
    source = cdawmeta.CONFIG['urls']['cdfmetafile']
  else:
    source = f"{cdawmeta.CONFIG['urls']['cdasr']} orig_data endpoint"

  file_list = metadatum[file_list_name].get('data', None)
  if file_list is None:
    emsg = f"{file_list_name} has no 'data' attribute"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)
    return {"error": emsg}

  if "FileDescription" not in file_list:
    emsg = f"{file_list_name} has no FileDescription attribute"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)
    return {"error": emsg}

  if not isinstance(file_list["FileDescription"], list):
    emsg = f"{file_list_name}['data']['FileDescription'] is not a list of objects"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)

  num_files = len(file_list["FileDescription"])
  if num_files == 0:
    emsg = f"No files in {file_list_name}['data']['FileDescription']. Will use all.xml for start/stop."
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)
  else:
    if num_files == 1:
      file_pos = "first"
      sampleFile = file_list["FileDescription"][0]
    elif num_files == 2:
      file_pos = "second"
      sampleFile = file_list["FileDescription"][1]
    else:
      file_pos = "penultimate"
      sampleFile = file_list["FileDescription"][-2]

  startDate_allxml = _all_timestamp(id, metadatum['allxml'], 'start', logger)
  stopDate_allxml = _all_timestamp(id, metadatum['allxml'], 'stop', logger)

  if num_files > 0:
    if cdawmeta.util.time.pad_iso8601(sampleFile["StartTime"]) > cdawmeta.util.time.pad_iso8601(sampleFile["EndTime"]):
      emsg = f"StartTime ({sampleFile['StartTime']}) > EndTime ({sampleFile['EndTime']}) in {source}"
      cdawmeta.error('start_stop', id, None, "CDF.StartTimeDateBeforeEndTime", emsg, logger)

    startDate_file_list = file_list["FileDescription"][0]["StartTime"]
    startDate, startDateSource = _update_timestamp(id, file_list_name, startDate_file_list, startDate_allxml, "start", logger)

    stopDate_file_list = file_list["FileDescription"][-1]["EndTime"]
    stopDate, stopDateSource = _update_timestamp(id, file_list_name, stopDate_file_list, stopDate_allxml, "stop", logger)
  else:
    startDate = startDate_allxml
    startDateSource = 'all.xml'
    stopDate = stopDate_allxml
    stopDateSource = 'all.xml'

  range = {
    "startDate": startDate,
    "startDateSource": startDateSource,
    "stopDate": stopDate,
    "stopDateSource": stopDateSource
  }

  if num_files > 0:
    url = metadatum[file_list_name]["url"]
    range["sampleStartDate"] = sampleFile["StartTime"]
    range["sampleStopDate"] = sampleFile["EndTime"]
    note = "sample{Start,Stop}Date corresponds to the time range spanned "
    note += f"by the content of the {file_pos} file listed for this dataset in {url}."
    range["note"] = note

  return [range]

def _all_timestamp(id, allxml, val, logger):
  timestamp = allxml.get(f'@timerange_{val}', None)
  if timestamp is None:
    emsg = f"all.xml has no '@timerange_{val}' attribute"
    cdawmeta.error('start_stop', id, None, f"CDAWeb.No_timerange_{val}", emsg, logger)
  else:
    timestamp = allxml[f'@timerange_{val}'].replace(' ', 'T') + 'Z'
  return timestamp


def _update_timestamp(id, file_list_name, from_file_list, from_allxml, which, logger):
  if from_allxml is None:
    emsg = f"all.xml has no {which} information. Using {file_list_name} value."
    logger.warning(emsg)
    return from_file_list, file_list_name

  translates = str.maketrans({'T': '', 'Z': '', '.': '', ':': '', '-': ''})
  from_file_list_x = from_file_list.translate(translates)
  from_allxml_x = from_allxml.translate(translates)
  min_len = min(len(from_file_list_x), len(from_allxml_x))

  emsg_start = f"{which}Date ({from_file_list}) determined from {file_list_name}"
  emsg_end = f"{which}Date ({from_allxml}) from all.xml. Using {file_list_name} value."
  if which == 'start' and from_file_list_x[0:min_len] < from_allxml_x[0:min_len]:
    emsg = f"{emsg_start} < {emsg_end}"
    cdawmeta.error('start_stop', id, None, "CDAWeb.StartMismatch", emsg, logger)
    return from_file_list, file_list_name

  if which == 'stop' and from_file_list_x[0:min_len] > from_allxml_x[0:min_len]:
    emsg = f"{emsg_start} > {emsg_end}"
    cdawmeta.error('start_stop', id, None, "CDAWeb.StopMismatch", emsg, logger)
    return from_file_list, file_list_name

  return from_allxml, f'all.xml and {file_list_name}'
