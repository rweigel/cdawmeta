import cdawmeta

#FILE_LIST = cdawmeta.config['metadata']['file_list']
FILE_LIST = 'cdfmetafile'
dependencies = [FILE_LIST]

def start_stop(metadatum, logger):

  id = metadatum['id']

  file_list = metadatum[FILE_LIST].get('data', None)
  if file_list is None:
    emsg = f"{FILE_LIST} has no 'data' attribute"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)
    return {"error": emsg}

  if "FileDescription" not in file_list:
    emsg = f"{FILE_LIST} has no FileDescription attribute"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)
    return {"error": emsg}

  if not isinstance(file_list["FileDescription"], list):
    emsg = f"{FILE_LIST}['data']['FileDescription'] is not a list of objects"
    cdawmeta.error('start_stop', id, None, "CDAWeb.NoFiles", emsg, logger)

  num_files = len(file_list["FileDescription"])
  if num_files == 0:
    emsg = f"No files in {FILE_LIST}['data']['FileDescription']. Will use all.xml for start/stop."
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

  startDate = _all_timestamp(metadatum['allxml'], 'start')
  stopDate = _all_timestamp(metadatum['allxml'], 'stop')

  if num_files > 0:
    if cdawmeta.util.pad_iso8601(sampleFile["StartTime"]) > cdawmeta.util.pad_iso8601(sampleFile["EndTime"]):
      emsg = f"StartTime ({sampleFile['StartTime']}) > EndTime ({sampleFile['EndTime']}) in {FILE_LIST}['data']['FileDescription']"
      cdawmeta.error('start_stop', id, None, "CDF.NoFiles", emsg, logger)

    startDate_files = file_list["FileDescription"][0]["StartTime"]
    startDate, startDateSource = _update_timestamp(id, startDate_files, startDate, "start", logger)
    stopDate_files = file_list["FileDescription"][-1]["EndTime"]
    stopDate, stopDateSource = _update_timestamp(id, stopDate_files, stopDate, "stop", logger)

  range = {
    "startDate": startDate,
    "startDateSource": startDateSource,
    "stopDate": stopDate,
    "stopDateSource": stopDateSource
  }

  if num_files > 0:
    url = metadatum[FILE_LIST]["url"]
    range["sampleStartDate"] = sampleFile["StartTime"]
    range["sampleStopDate"] = sampleFile["EndTime"]
    note = "sample{Start,Stop}Date corresponds to the time range spanned "
    note += f"by the {file_pos} file in the reponse from {url}."
    range["note"] = note

  return [range]

def _all_timestamp(allxml, val):
  timestamp = allxml.get(f'@timerange_{val}', None)
  if timestamp is None:
    emsg = f"all.xml has no '@timerange_{val}' attribute"
    cdawmeta.error('start_stop', id, None, f"CDAWeb.No_timerange_{val}", emsg, logger)
  else:
    timestamp = allxml[f'@timerange_{val}'].replace(' ', 'T') + 'Z'
  return timestamp

def _update_timestamp(id, date_file_list, date_allxml, which, logger):
  if date_allxml is None:
    emsg = f"all.xml has no {which} information. Using {FILE_LIST} value."
    logger.warning(emsg)
    return date_file_list

  translates = str.maketrans({'T': '', 'Z': '', '.': '', ':': '', '-': ''})
  date_file_list_x = date_file_list.translate(translates)
  date_allxml_x = date_allxml.translate(translates)
  min_len = min(len(date_file_list_x), len(date_allxml_x))

  if which == 'start' and date_file_list_x[0:min_len] < date_allxml_x[0:min_len]:
    emsg = f"{which}Date ({date_file_list}) determined from {FILE_LIST} < {which}Date ({date_allxml}) from all.xml. Using {FILE_LIST} value."
    cdawmeta.error('start_stop', id, None, "HAPI.StartStopMismatch", emsg, logger)
    return date_file_list, FILE_LIST
  else:
    return date_allxml, 'all.xml'

  if which == 'stop' and date_file_list_x[0:min_len] > date_allxml_x[0:min_len]:
    emsg = f"{which}Date ({date_file_list}) determined from {FILE_LIST} > {which}Date ({date_allxml}) from all.xml. Using {FILE_LIST} value."
    cdawmeta.error('start_stop', id, None, "HAPI.StartStopMismatch", emsg, logger)
    return date_file_list, FILE_LIST
  else:
    return date_allxml, 'all.xml'

