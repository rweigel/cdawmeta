from cdasws import CdasWs
from cdasws.datarepresentation import DataRepresentation
cdas = CdasWs()

def read_ws(dataset, parameters, start, stop):

  start = start.replace(".000000000", "")
  stop = stop.replace(".000000000", "")

  meta = []
  data = []
  parameters = parameters.split(",")
  # CdasWs() sends warnings to stdout instead of using Python logging
  # module. We need to capture to prevent it from being sent out.
  import io
  stdout_ = io.StringIO()
  from contextlib import redirect_stdout
  with redirect_stdout(stdout_):
    status, xrdata = cdas.get_data(\
                      dataset, parameters, start, stop,
                      dataRepresentation=DataRepresentation.XARRAY)
  if status['http']['status_code'] != 200:
    raise Exception(f"Error: cdas.get_data() returned {status['http']['status_code']}")

  # Note: Assumes DEPEND_0 = 'Epoch', which is not always true.
  # TODO: Determine how to extract DEPEND_0. Not easy b/c can't save
  # xrdata due to a bug, so need to run request each time or modify
  # source of CdasWs() to use cache.
  time = xrdata['Epoch'].values

  for parameter in parameters:
    data.append(xrdata[parameter].values)
    meta.append({"FILLVAL": xrdata[parameter].FILLVAL, "FORMAT": xrdata[parameter].FORMAT})

  return time, data, meta
