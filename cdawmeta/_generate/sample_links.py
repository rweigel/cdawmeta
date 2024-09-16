import cdawmeta

dependencies = ['orig_data']

# TODO: Like cadence(), there should be an option to not update this
# information. The requests take a long time and once a sample start and stop
# date is created it is unlikely to need updating.

def sample_links(metadatum, logger):

  def reformat_dt(dt):
    dt = dt.replace(" ", "T").replace("-","").replace(":","")
    return dt.split(".")[0] + "Z"

  wsbase = cdawmeta.CONFIG['metadata']['wsbase']

  last_file = metadatum['orig_data']['data']['FileDescription'][-1]
  start = reformat_dt(last_file['StartTime'])
  stop = reformat_dt(last_file['EndTime'])
  id = metadatum['id']
  samples = {
    'file': last_file['Name'],
    'url': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=cdf",
    'plot': f"{wsbase}{id}/data/{start},{stop}/ALL-VARIABLES?format=png"
  }
  return [samples]
