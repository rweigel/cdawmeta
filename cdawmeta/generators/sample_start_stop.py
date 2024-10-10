dependencies = ['orig_data']

# TODO: Like cadence(), there should be an option to not update this
# information. The requests take a long time and once a sample start and stop
# date is created it is unlikely to need updating.

def sample_start_stop(metadatum, logger):

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
    file_pos = "first"
    sampleFile = orig_data["FileDescription"][0]
  elif num_files == 2:
    file_pos = "second"
    sampleFile = orig_data["FileDescription"][1]
  else:
    file_pos = "penultimate"
    sampleFile = orig_data["FileDescription"][-2]

  if sampleFile is not None:
    sampleStartDate = sampleFile["StartTime"]
    sampleStopDate = sampleFile["EndTime"]

  url = metadatum["orig_data"]["url"]
  note = "sample{Start,Stop}Date corresponds to the time range spanned "
  note += f"by the {file_pos} file in the reponse from {url}, where the start/end "
  note += "in this URL correponds to the start/end of the dataset."
  range = {
            "sampleStartDate": sampleStartDate,
            "sampleStopDate": sampleStopDate,
            "note": note
          }

  return [range]
