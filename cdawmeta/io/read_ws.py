from cdasws import CdasWs
from cdasws.datarepresentation import DataRepresentation
cdas = CdasWs()

def read_ws(dataset, parameters, start, stop):

  if isinstance(parameters, str):
    parameters = parameters.split(",")

  start = start.replace(".000000000", "")
  stop = stop.replace(".000000000", "")

  meta = []
  data = []
  # CdasWs() sends warnings to stdout instead of using Python logging module.
  # We need to capture to prevent it from being sent out.
  import io
  stdout_ = io.StringIO()
  from contextlib import redirect_stdout
  with redirect_stdout(stdout_):
    status, xrdata = cdas.get_data(\
                      dataset, parameters, start, stop,
                      dataRepresentation=DataRepresentation.XARRAY)
  if status['http']['status_code'] != 200:
    raise Exception(f"Error: cdas.get_data() returned {status['http']['status_code']}")

  #print("Writing to test.nc")
  #xrdata.to_netcdf('test.nc')
  #print("Wrote to test.nc")
  # ValueError: failed to prevent overwriting existing key units in attrs on
  # variable 'Epoch'. This is probably an encoding field used by xarray to
  # describe how a variable is serialized. To proceed, remove this key
  # from the variable's attributes manually.

  # Note: Assumes DEPEND_0 = 'Epoch', which is not always true.
  # TODO: Determine how to extract DEPEND_0. Not easy b/c can't save
  # xrdata due to a bug, so need to run request each time or modify
  # source of CdasWs() to use cache.
  time = xrdata['Epoch'].values

  for parameter in parameters:
    data.append(xrdata[parameter].values)
    meta.append({"FILLVAL": xrdata[parameter].FILLVAL, "FORMAT": xrdata[parameter].FORMAT})

  return time, data, meta

if __name__ == '__main__':
  from timeit import default_timer as timer
  import cdawmeta

  cdawmeta.DATA_DIR = '../../data'

  logger = cdawmeta.logger(name='io', log_level='info')

  dataset = 'AC_H1_MFI'
  variables = ['Magnitude', 'BGSEc']
  start = '2024-06-11T00:00:00.000Z'
  stop = '2024-06-11T23:56:00.000Z'

  print(20*"-")

  t1 = timer()
  print("Generating file on server")
  resp = cdas.get_data_file(dataset, variables, start, stop)
  file = resp[1]['FileDescription'][0]['Name']
  print(f"Generated file on server: {timer() - t1}")

  t2 = timer()
  print("Downloading and reading generated file")
  data = cdawmeta.io.read_cdf(file, variables, use_cache=False, logger=logger)
  print(f"Downloaded and read generated file: {timer() - t2}")
  dtws = timer() - t1
  print(f"Total web service time: {dtws}")

  print(20*"-")

  t0 = timer()
  print("Computing files needed to download")
  files = cdawmeta.io.files(id=dataset, start=start, stop=stop, logger=logger)
  print(f"Computed files needed to download: {timer() - t2}")
  print(f"Downloading and reading: {timer() - t2}")
  data, meta = cdawmeta.io.read_cdf(files[0], variables, use_cache=False, logger=logger)
  print(f"Downloading and reading: {timer() - t2}")
  dt = timer() - t0

  print(f"Total non-webservice time: {dt}")

  print(20*"-")

  print(f"Web service/Non-web service time: {dtws / dt}")

  print(20*"-")
