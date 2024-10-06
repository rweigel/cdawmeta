import datetime

from timedelta_isoformat import timedelta

import cdawmeta
import hapiclient

dependencies = ['hapi']

def sample_links(metadatum, logger):


  def file_based_start_stop(orig_data):
    def reformat_dt(dt):
      dt = dt.replace(" ", "T").replace("-","").replace(":","")
      return dt.split(".")[0] + "Z"
    # The last file in the list is the most recent.
    last_file = orig_data['FileDescription'][-1]
    start = reformat_dt(last_file['StartTime'])
    stop = reformat_dt(last_file['EndTime'])
    logger.info(f"start/stop based on last file from orig_data: {start}/{stop}")
    return start, stop, last_file

  def cadence_based_start_stop(dataset):
    dt = timedelta.fromisoformat(cadence)
    stop = hapiclient.hapitime2datetime(dataset['info']['stopDate'])[0]
    start = stop - 10*dt
    start = datetime.datetime.strftime(start, "%Y%m%dT%H%M%SZ")
    stop = datetime.datetime.strftime(stop,   "%Y%m%dT%H%M%SZ")
    logger.info(f"start/stop based on cadence: {start}/{stop}")
    return start, stop

  wsbase = cdawmeta.CONFIG['urls']['cdasr']

  id = metadatum['id']

  start, stop, last_file = file_based_start_stop(metadatum['orig_data']['data'])

  # Possibly should use master for the variable names for the CDAWeb links, 
  # but the HAPI metadata has dropped datasets with problems that would
  # probably lead to errors in the links. Also, we were using a the time span
  # of the last file from the CDASR /orig_data endpoint for the time span of the
  # request, but this sometimes leads to an "TimeInterval is too large" error.
  # So now we use the cadence and request a 10 records. This may not always work
  # if the number of components in a variable is so large that the error is
  # triggered, however.
  hapi = metadatum['hapi']['data']
  if hapi is None:
    return [None]

  if isinstance(hapi, dict):
    hapi = [hapi]

  txt_links = []
  plot_links = []
  for dataset in hapi:

    cadence = dataset['info'].get('cadence', None)
    if cadence is not None:
      start, stop = cadence_based_start_stop(dataset)
      logger.info("Using cadence for time range in links.")

    wsbase = f"{wsbase}{id}/data/{start},{stop}"

    if start is None or stop is None:
      logger.error("Information needed for time range in links is not avaialable.")
      continue

    variables = cdawmeta.util.array_to_dict(dataset['info']['parameters'], 'name')
    variables = list(variables.keys())[1:] # CDAWeb API does not accept the name of the DEPEND_0 variable.
    if len(variables) == 0:
      continue # Dataset only contains a DEPEND_0.

    # In some cases, the list of variables makes the URL too long for their back-end code to handle. For example,
    # https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/AC_H2_CRIS/data/20080601T000000Z,20080601T010000Z/flux_B,flux_C,flux_N,flux_O,flux_F,flux_Ne,flux_Na,flux_Mg,flux_Al,flux_Si,flux_P,flux_S,flux_Cl,flux_Ar,flux_K,flux_Ca,flux_Sc,flux_Ti,flux_V,flux_Cr,flux_Mn,flux_Fe,flux_Co,flux_Ni,cnt_B,cnt_C,cnt_N,cnt_O,cnt_F,cnt_Ne,cnt_Na,cnt_Mg,cnt_Al,cnt_Si,cnt_P,cnt_S,cnt_Cl,cnt_Ar,cnt_K,cnt_Ca,cnt_Sc,cnt_Ti,cnt_V,cnt_Cr,cnt_Mn,cnt_Fe,cnt_Co,cnt_Ni?format=cdf
    # <Status> Please select fewer variables.</Status><Error>Error number:         -360 in listing (wrt_hybd_strct).</Error><Error>Error Message: </Error>
    # We could put a switch into handle this case, but it may be more
    # useful to be able to inspect the data for each variable individually
    # as the following will allow, especially for the case when there are
    # more than one DEPEND_0s.
    for variable in variables:
      txt_links.append(f"{wsbase}/{variable}?format=text")
      plot_links.append(f"{wsbase}/{variable}?format=png")

  samples = {
    "file": last_file['Name'],
    "cdaweb":
      {
        "cdf": f"{wsbase}/ALL-VARIABLES?format=cdf",
        "txt": txt_links,
        "plot": plot_links
      },
    "hapi": {
    }
  }
  return [samples]
