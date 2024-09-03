import cdawmeta

def _samples(id, _orig_data):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  wsbase = cdawmeta.CONFIG['cdaweb']['wsbase']

  last_file = _orig_data['data']['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  _samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return _samples

def _sample_start_stop(metadatum):

  cdawmeta.util.print_dict(metadatum)

  orig_data = metadatum["orig_data"]['data']

  if "FileDescription" not in orig_data:
    logger.info("No orig_data for " + metadatum["id"])
    return None

  if isinstance(orig_data["FileDescription"], dict):
    orig_data["FileDescription"] = [orig_data["FileDescription"]]

  num_files = len(orig_data["FileDescription"])
  if num_files == 0:
    sampleFile = None
  if num_files == 1:
    sampleFile = orig_data["FileDescription"][0]
  elif num_files == 2:
    sampleFile = orig_data["FileDescription"][1]
  else:
    sampleFile = orig_data["FileDescription"][-2]

  if sampleFile is not None:
    sampleStartDate = sampleFile["StartTime"]
    sampleStopDate = sampleFile["EndTime"]

  range = {
            "sampleStartDate": sampleStartDate,
            "sampleStopDate": sampleStopDate
          }

  return range
