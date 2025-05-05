import os
from collections import Counter

import numpy
from timedelta_isoformat import timedelta

import cdawmeta

#FILE_LIST = cdawmeta.config['metadata']['file_list']
FILE_LIST = 'cdfmetafile'
dependencies = [FILE_LIST, 'start_stop', 'master']

# Special cases:
#   C1_CP_EFW_L3_E3D_INERT (CDF_EPOCH16)
#   THA_L2_ESA (VIRTUAL DEPEND_0)
#   PSP_FLD_L3_RFS_HFR (issue with file)
#   FORMOSAT5_AIP_IDN (NetCDF file)

def cadence(metadatum, logger):

  use_cache = True
  # N.B. use_cache=True is set. This assumes that content for a given file
  # name is constant. If not, would need to pass the keyword argument update
  # and set use_cache = not update.

  id = metadatum['id']

  file_list, master, emsg = _extract_and_check_metadata(id, metadatum, logger)
  if emsg is not None:
    return {"error": emsg}

  all_file_counts = []
  depend_0s = []
  success = False # True if at least one file was processed successfully.
  for file_idx in [0, -1]:
    url = file_list['FileDescription'][file_idx]['Name']

    # FORMOSAT5_AIP_IDN
    if url.endswith('.nc'):
      # Use HAPI client instead (CDAWeb service could be used, but can be up to ~5x slower).
      emsg = f"{id}: Computing cadence for file type '.nc' is not implemented"
      cdawmeta.error("cadence", id, None, "HAPI.NotImplementedNetCDF", emsg, logger)
      return {"error": emsg}

    file_counts = _depend_0s_counts(id, url, metadatum, file_idx, logger, use_cache)

    if 'error' in file_counts:
      # Failed to read file or no DEPEND_0s found.
      all_file_counts.append(file_counts)
      continue

    if len(depend_0s) == 0:
      depend_0s = file_counts.keys()
    elif success:
      new_depend_0s = set(file_counts.keys()) - set(depend_0s)
      missing_depend_0s = set(depend_0s) - set(file_counts.keys())

      if new_depend_0s:
        emsg = f"New DEPEND_0(s) found in file {file_idx}: {new_depend_0s}"
        cdawmeta.error("cadence", id, None, "CDF.NewDEPEND_0s", emsg, logger)

      if missing_depend_0s:
        emsg = f"Missing DEPEND_0(s) in file {file_idx}: {missing_depend_0s}"
        cdawmeta.error("cadence", id, None, "CDF.MissingDEPEND_0s", emsg, logger)

    if 'error' not in file_counts:
      success = True

    all_file_counts.append(file_counts)

  """
  all_file_counts = [
    {
      depend_0_name1: {'url': url1, 'start': start, 'stop': stop, 'note': note, 'counts': counts11}},
      depend_0_name2: {'url': url1, 'start': start, 'stop': stop, 'note': note, 'counts': counts21}},
      ...
    },
      depend_0_name1: {'url': url2, 'start': start, 'stop': stop, 'note': note, 'counts': counts12}},
      depend_0_name2: {'url': url2, 'start': start, 'stop': stop, 'note': note, 'counts': counts22}},
      ...
  ]
  """

  counts_restructured = {}
  for depend_0 in depend_0s:
    counts_restructured[depend_0] = []
    for file_counts in all_file_counts:
      if depend_0 in file_counts:
        counts_restructured[depend_0].append(file_counts[depend_0])
  """
    counts_restructured = {
      depend_0_name1: [
        {'url': url1, 'start': start, 'stop': stop, 'note': note, 'counts': counts11}},
        {'url': url2, 'start': start, 'stop': stop, 'note': note, 'counts': counts12}},
        ...
      ],
      depend_0_name2: [
        {'url': url1, 'start': start, 'stop': stop, 'note': note, 'counts': counts21}},
        {'url': url2, 'start': start, 'stop': stop, 'note': note, 'counts': counts22}},
        ...
      ],
      ...
    }
  """

  return [counts_restructured]

def _depend_0s_counts(id, url, metadatum, file_idx, logger, use_cache):

  master = metadatum['master']['data']

  logger.info(f"{id}")
  logger.info(f"  Extracting DEPEND_0 names from {url}")

  try:
    depend_0_names = cdawmeta.io.read_cdf_depend_0s(url, logger=logger, use_cache=use_cache)
  except Exception as e:
    emsg = f"  cdawmeta.io.read_cdf_depend_0s('{url}') failed with error: {e}"
    emsg  = emsg + "\n" + _trace()
    cdawmeta.error("cadence", id, None, "CDF.FailedCDFRead", emsg, logger)
    return {"error": emsg}

  if depend_0_names is None:
    emsg = f"  cdawmeta.io.read_cdf_depend_0s('{url}') returned no DEPEND_0s."
    cdawmeta.error("cadence", id, None, "CDF.NoDEPEND_0s", emsg, logger)
    return {"error": emsg}

  if len(depend_0_names) > 1:
    logger.info(f"  {len(depend_0_names)} DEPEND_0s: {depend_0_names}")
  else:
    logger.info(f"  {len(depend_0_names)} DEPEND_0: {depend_0_names}")

  depend_0_counts = {}

  # TODO: Check that all DEPEND_0 data variable names in Master CDF match those
  # found in data CDF. Mismatches have been found for DEPEND_0 names, so
  # expect this to happen for other variables. Should also check that DataTypes
  # match.
  for depend_0_name in depend_0_names:

    msg = f"  Computing cadence for DEPEND_0 = '{depend_0_name}'"
    logger.info(msg)

    depend_0_meta = {"url": url, "start": None, "stop": None, "note": None, "counts": []}
    depend_0_counts[depend_0_name] = depend_0_meta

    emsg = _check_data_types(id, master, depend_0_name, logger)

    if emsg is not None:
      _update_for_error(depend_0_meta, emsg)
      continue

    try:
      logger.info(f"    Reading '{depend_0_name}' from CDF file")
      data = cdawmeta.io.read_cdf(url, variables=depend_0_name, logger=logger, iso8601=False)
      logger.info(f"    Read '{depend_0_name}'")
    except Exception as e:
      emsg = f"{id}: cdawmeta.io.read_cdf("
      emsg += f"'{url}', variables='{depend_0_name}', iso8601=False) raised: \n{e}"
      #emsg += "\n" + _trace()
      _update_for_error(depend_0_meta, emsg, logger=logger)
      cdawmeta.error("cadence", id, None, "CDF.FailedCDFRead", emsg, logger)
      continue

    DataType, emsg = _check_data(id, depend_0_name, data, master, url, logger)
    if emsg is not None:
      _update_for_error(depend_0_meta, emsg)
      continue

    epoch = data[depend_0_name]
    epoch_values = epoch['VarData']

    try:
      start, stop = _check_start_stop(id, depend_0_name, epoch_values, metadatum, file_idx, logger)
    except Exception as e:
      emsg = f"    _check_start_stop() failed: {e}"
      emsg += "\n" + _trace()
      _update_for_error(depend_0_meta, emsg, logger=logger)
      cdawmeta.error("cadence", id, None, "CDF.FailedStartStop", emsg, logger)
      continue

    depend_0_meta['start'] = start
    depend_0_meta['stop'] = stop

    if epoch_values.size == 1 or len(epoch_values) < 2:
      emsg = "Could not determine cadence because "
      if epoch_values.size == 1:
        emsg += f"Returned {depend_0_name} value is a scalar in {url}"
      else:
        emsg += f"len({depend_0_name}) = {len(epoch_values)} in {url}"
      _update_for_error(depend_0_meta, emsg, logger)
      continue

    if DataType == 'CDF_TIME_TT2000':
      FILL= -9223372036854775808
    if DataType == 'CDF_EPOCH16':
      FILL = -1e31j - 1e31j
    if DataType == 'CDF_EPOCH':
      FILL = 1e31

    try:
      if DataType == 'CDF_EPOCH16':
        # e.g., C1_CP_EFW_L3_E3D_INERT
        diff = _diff_cdf_epoch16(epoch_values)
      else:
        diff = numpy.diff(epoch_values)
        diff = diff.astype(int)
    except Exception as e:
      emsg = f"{url}: numpy.diff({depend_0_name}['VarData']) error: {e}"
      raise Exception(emsg)

    epoch_values = epoch_values[epoch_values != FILL]

    if numpy.any(diff) <= 0:
      n = numpy.sum(diff <= 0)
      emsg = f"{n} negative or zero time difference(s) found."
      cdawmeta.error("cadence", id, depend_0_name, "CDF.NegativeTimeStep", emsg, logger)
      continue

    depend_0_meta['counts'] = _count_dicts(diff, depend_0_name, DataType, logger)

    if 0 == len(depend_0_meta['counts']):
      emsg = f"Could not determine cadence in {url}"
      _update_for_error(depend_0_meta, emsg, logger=logger)
      continue

    depend_0_meta['note'] = _note(depend_0_name, depend_0_counts, url)

  return depend_0_counts

def _count_dicts(diff, depend_0_name, DataType, logger):

  sf = 1e3 # CDF_EPOCH is in milliseconds
  duration_unit = "ms"
  if DataType == 'CDF_TIME_TT2000':
    duration_unit = "ns"
    sf = 1e9 # CDF_TIME_TT2000 is in nanoseconds
  if DataType == 'CDF_EPOCH16':
    duration_unit = "ps"
    sf = 1e12 # CDF_EPOCH16 is in picoseconds

  counts = Counter(diff)
  total = sum(counts.values())
  ucounts = sorted(counts.items(), key=lambda item: item[1], reverse=True)
  s = "s" if len(ucounts) + 1 > 1 else ""
  logger.info(f"    {total} Δt{s}; {len(ucounts)} unique. Top 10 by count:")

  idx = 0
  count_dicts = []
  for value, count in ucounts:
    fraction = count / total
    if value != round(value):
      wmsg = f"    Cadence {value} [ms] != {value/sf} [ms]"
      logger.warning(wmsg)
    value_s = value/sf
    if value_s >= 1e-3:
      t = timedelta(seconds=value_s)
      duration_iso8601 = t.isoformat()
    else: # timedelta() returns 0 for less than microsecond, so handle manually.
      # TODO: When there are fractional seconds, we should be rendering
      # with a fixed number of significant digits and then trimming extra trailing
      # zeros. Move this into a function and write tests. Also modify diffs 
      # to be to three significant digits so that there are fewer bins.
      if value_s > 1e-9:
        duration_iso8601 = f"PT{value_s:.9f}S"
      elif value_s > 1e-12:
        duration_iso8601 = f"PT{value_s:.12f}S"
      else:
        duration_iso8601 = f"PT{value_s:.15f}S"

    if value <= 0:
      # Error message created before call to this function.
      logger.info("    Skipping negative or zero cadence.")
      continue

    count_dict = {
      "count": count,
      "duration": int(value), # Int for JSON serialization (so not numpy type)
      "duration_unit": duration_unit,
      "duration_iso8601": duration_iso8601,
      "fraction": fraction
    }
    count_dicts.append(count_dict)

    idx = idx + 1
    if idx < 11:
      logger.info(f"      {idx}. {count_dict}")

  return count_dicts

def _update_for_error(depend_0_meta, emsg, logger=None):
  if logger is not None:
    logger.error("  " + emsg)
  depend_0_meta['error'] = emsg.strip()

  del depend_0_meta['start']
  for key in depend_0_meta.copy().keys():
    if depend_0_meta[key] is None:
      del depend_0_meta[key]

def _note(depend_0_name, depend_0_counts, url):
  counts = depend_0_counts[depend_0_name]['counts'][0]

  duration = counts['duration']
  duration_unit = counts['duration_unit']
  iso = counts['duration_iso8601']
  cnt = counts['count']
  pct = 100*counts['fraction']

  note = f"Counts based on variable '{depend_0_name}' in {url}. "
  note += f"The most common cadence, {duration} [{duration_unit}] = {iso}, occurred for "
  if pct == 100:
    note += f"all {cnt} timesteps. "
  else:
    note += f"{pct:0.4f}% of the {cnt} timesteps. "

  return note

def _trace():
  import traceback
  trace = traceback.format_exc()
  home_dir = os.path.expanduser("~")
  trace = trace.replace(home_dir, "~")
  return f"\n{trace}"

def _diff_cdf_epoch16(epoch):

  # Real is seconds, imaginary is picoseconds. See
  #   https://github.com/MAVENSDC/cdflib/blob/main/cdflib/epochs.py#L1119
  # TODO: They check for negative values. Why?
  # TODO: Test for fill values. See how epochs.py (but they assume a fill value
  # instead of checking that it is the ISTP recommended fill value).

  diff_s = numpy.diff(numpy.real(epoch))
  diff_ps = numpy.diff(numpy.imag(epoch))
  diff_ps = 1e12*diff_s + diff_ps
  return diff_ps

def _extract_and_check_metadata(id, metadatum, logger):
  file_list = cdawmeta.util.get_path(metadatum, [FILE_LIST, 'data'])
  if file_list is None:
    emsg = f"{id}: No {FILE_LIST} result"
    cdawmeta.error("cadence", id, None, f"CDF.No{FILE_LIST}", emsg, logger)
    return None, None, emsg

  master = cdawmeta.util.get_path(metadatum, ['master', 'data'])
  if master is None:
    emsg = "No master"
    cdawmeta.error("cadence", id, None, "CDAWeb.NoMaster", emsg, logger)
    return None, None, emsg

  if 'FileDescription' not in file_list:
    emsg = f"No FileDescription in {FILE_LIST}"
    cdawmeta.error("cadence", id, None, f"CDAWeb.NoFileDescriptionIn{FILE_LIST}", emsg, logger)
    return None, None, emsg

  if len(file_list['FileDescription']) == 0:
    emsg = f"Empty FileDescription in {FILE_LIST}"
    cdawmeta.error("cadence", id, None, f"CDAWeb.FileDescriptionIn{FILE_LIST}Empty", emsg, logger)
    return None, None, emsg

  if 'Name' not in file_list['FileDescription'][0]:
    emsg = f"No 'Name' attribute in {FILE_LIST}['FileDescription'][0]"
    cdawmeta.error("cadence", id, None, f"CDAWeb.NoNameInFileDescriptionIn{FILE_LIST}", emsg, logger)
    return None, None, emsg

  return file_list, master, None

def _check_start_stop(id, epoch_name, epoch_values, metadatum, file_idx, logger):
  import cdflib

  def handle_nat(timestamp, epoch, which):
    if timestamp.lower() == "nat":
      all_timestamps = cdflib.cdfepoch.to_datetime(epoch)
      isnat = numpy.isnat(all_timestamps)
      idx = numpy.where(~isnat)[0]
      if len(idx) > 0:
        if which == 'first':
          timestamp = str(all_timestamps[idx[0]])
          logger.info(f"    First non-NaT in first CDF:   {timestamp}")
          return timestamp
        else:
          timestamp = str(all_timestamps[idx[-1]])
          logger.info(f"    Last non-NaT in first CDF:    {timestamp}")
          return timestamp
      else:
        logger.info("  All timestamps are 'NaT'")
        return None

    return timestamp

  startDate = metadatum['start_stop']['data']['startDate']
  startDateSource = metadatum['start_stop']['data']['startDateSource']
  stopDate = metadatum['start_stop']['data']['stopDate']
  stopDateSource = metadatum['start_stop']['data']['stopDateSource']

  first_timestamp = str(cdflib.cdfepoch.to_datetime(epoch_values[0])[0])
  logger.info(f"    First timestamp in CDF:\t{first_timestamp}")
  logger.info(f"    Start date from {startDateSource}:\t{startDate}")
  first_timestamp = handle_nat(first_timestamp, epoch_values, "first")
  first_timestamp += "Z"
  if file_idx == 0:
    startDate_pad = cdawmeta.util.pad_iso8601(startDate)
    first_timestamp_pad = cdawmeta.util.pad_iso8601(first_timestamp)
    if first_timestamp_pad < startDate_pad:
      emsg = f"    Start date from {startDateSource} ({startDate}) is after first non-NaT timestamp in first file: {first_timestamp} from {FILE_LIST}"
      #logger.error("cadence", id, epoch_name, "CDF.StartDateAfterFirstTimestamp", emsg, logger)
      logger.warn(emsg)
    if first_timestamp[0:19] < startDate[0:19]:
      emsg = f"    Start date from {startDateSource} ({startDate}) is after first non-NaT timestamp in first file ({first_timestamp}) from {FILE_LIST} rounded down to 1 s"
      cdawmeta.error("cadence", id, epoch_name, "CDF.StartDateMismatch", emsg, logger)

  last_timestamp = str(cdflib.cdfepoch.to_datetime(epoch_values[-1])[0])
  logger.info(f"    Last timestamp in CDF:\t{last_timestamp}")
  logger.info(f"    Stop date from {stopDateSource}:\t{stopDate}")
  last_timestamp = handle_nat(last_timestamp, epoch_values, "last")
  last_timestamp += "Z"
  if file_idx == -1:
    stopDate_pad = cdawmeta.util.pad_iso8601(stopDate)
    last_timestamp_pad = cdawmeta.util.pad_iso8601(last_timestamp)
    if last_timestamp_pad > stopDate_pad:
      emsg = f"    Stop date from {stopDateSource} ({stopDate}) is before last non-NaT timestamp in last file in {FILE_LIST}: {last_timestamp}"
      logger.warn(emsg)
    if last_timestamp[0:19] > stopDate[0:19]:
      emsg = f"    Stop date from {stopDateSource} ({stopDate}) is before last non-NaT timestamp in last file ({last_timestamp}) in {FILE_LIST} rounded down to 1 s"
      cdawmeta.error("cadence", id, epoch_name, "CDF.StopDateMismatch", emsg, logger)

  return first_timestamp, last_timestamp

def _check_data_types(id, master, depend_0_name, logger):

  if depend_0_name not in master['CDFVariables']:
    emsg = f"    Referenced DEPEND_0 = '{depend_0_name}' in CDF is not in master"
    cdawmeta.error("cadence", id, depend_0_name, "CDF.FileDEPEND_0NotInMaster", emsg, logger)
    return emsg

  path = ['CDFVariables', depend_0_name]
  VarDescription = cdawmeta.util.get_path(master, [*path, 'VarDescription'])
  if VarDescription is None:
    emsg = "    No VarDescripion in master."
    cdawmeta.error("cadence", id, depend_0_name, "CDF.NoVarDescriptionInMaster", emsg, logger)
    return emsg

  if 'DataType' not in VarDescription:
    emsg = "    No DataType in master."
    cdawmeta.error("cadence", id, depend_0_name, "CDF.NoDataTypeInMaster", emsg, logger)
    return emsg

  if 'RecVariance' not in VarDescription:
    emsg = "    No RecVariance in master."
    cdawmeta.error("cadence", id, depend_0_name, "CDF.RecVarianceNotVARYInMaster", emsg, logger)
    return emsg

  if VarDescription['RecVariance'] != 'VARY':
    emsg = f"    RecVariance = '{VarDescription['RecVariance']}' is not 'VARY' in master."
    cdawmeta.error("cadence", id, depend_0_name, "CDF.RecVarianceNotVARYInMaster", emsg, logger)
    return emsg

  VarAttributes = cdawmeta.util.get_path(master, [*path, 'VarAttributes'])
  if VarAttributes is None:
    emsg = "    No VarAttributes in master"
    cdawmeta.error("cadence", id, depend_0_name, "CDF.NoVarAttributesInMaster", emsg, logger)
    return emsg

  # e.g, THA_L2_ESA
  if 'VIRTUAL' in VarAttributes and VarAttributes['VIRTUAL'].lower().strip() == 'true':
    emsg = "    Not Implemented: VIRTUAL DEPEND_0"
    cdawmeta.error("cadence", id, None, "HAPI.NotImplementedVirtualDEPEND_0", emsg, logger)
    return emsg

  return None

def _check_data(id, depend_0_name, data, master, url, logger):

  if data is None:
    emsg = f"    cdawmeta.io.read_cdf('{url}', variables='{depend_0_name}', iso8601=False) returned None."
    cdawmeta.error("cadence", id, None, "CDF.FailedCDFRead", emsg, logger)
    return None, emsg

  emsg_coda = f"in {url}; CDF metadata = {data}"

  VarAttributes = data[depend_0_name].get('VarAttributes', None)
  if VarAttributes is None:
    emsg = f"    {depend_0_name}['VarAttributes'] = None {emsg_coda}"
    cdawmeta.error("cadence", id, None, "CDF.NoVarAttributes", emsg, logger)
    return None, emsg

  if 'VarData' not in data[depend_0_name]:
    emsg = f"    {depend_0_name}: No 'VarData' in {emsg_coda}"
    cdawmeta.error("cadence", id, None, "CDF.NoVarDataAttribute", emsg, logger)
    return None, emsg

  # e.g., PSP_FLD_L3_RFS_HFR
  if data[depend_0_name]['VarData'] is None:
    emsg = f"    {depend_0_name}['VarData'] = None in {emsg_coda}"
    cdawmeta.error("cadence", id, None, "CDF.NoVarData", emsg, logger)
    return None, emsg

  DataType = cdawmeta.util.get_path(data[depend_0_name],['VarDescription', 'DataType'])
  if DataType is None:
    emsg = f"    {depend_0_name}['VarDescription']['DataType'] = None in {emsg_coda}"
    cdawmeta.error("cadence", id, None, "CDF.NoDataType", emsg, logger)
    return None, emsg

  data_type = data[depend_0_name]['VarDescription']['DataType']
  time_types = ['CDF_EPOCH', 'CDF_EPOCH16', 'CDF_TIME_TT2000']

  if data_type not in time_types:
    emsg = f"    DataType = {data_type} is not one of {time_types}"
    cdawmeta.error("cadence", id, depend_0_name, "CDF.FileDEPEND_0NotTimeDataType", emsg, logger)
    try:
      data_type_master = master['CDFVariables'][depend_0_name]['VarDescription']['DataType']
      if data_type_master != data_type:
        emsg += f" and does not match DataType in master = {data_type_master}"
      return None, emsg
    except:
      emsg += " and cannot determine DataType in master"
      cdawmeta.error("cadence", id, depend_0_name, "CDF.FileDEPEND_0NotTimeDataType", emsg, logger)
      return None, emsg

  return DataType, None