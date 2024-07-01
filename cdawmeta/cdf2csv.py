def url2file(url):
  return url.replace("https://cdaweb.gsfc.nasa.gov/pub/data/", "")

def get(url, url2file=url2file):

  import os
  from urllib.request import urlopen
  from shutil import copyfileobj

  import cdawmeta

  file_name = url2file(url)
  cdawmeta.util.mkdir(os.path.dirname(file_name))

  length=16*1024
  req = urlopen(url)
  with open(file_name, 'wb') as fp:
    copyfileobj(req, fp, length)

  headers = dict(req.getheaders())
  cdawmeta.util.write(file_name + ".headers.json", headers)

  return file_name

def write(time, meta, data):

  import sys
  import numpy as np

  # https://stackoverflow.com/a/30091579
  from signal import signal, SIGPIPE, SIG_DFL
  signal(SIGPIPE, SIG_DFL)

  nrecords = 0
  size = 0
  for t in range(len(time)):

    tstr = str(time[t])

    if tstr >= stop[0:len(tstr)]:
      break

    size += sys.stdout.write('%sZ' % tstr)

    for p, parameter in enumerate(data):

      fmt = f2c_specifier(meta[p]['FORMAT'])
      FILLVAL = np.array(meta[p]["FILLVAL"], dtype=parameter.dtype)

      # TODO: Check that this row-major (order-'C') is correct
      #  and not column major ('F').
      for value in parameter[t].flatten(order='C'):
        if value == FILLVAL:
          size += sys.stdout.write("," + str(FILLVAL))
        else:
          size += sys.stdout.write(fmt.format(value))

    # Omit trailing newline by executing if t < len(time) - 1?
    # Does spec say last character of CSV is newline?
    size += sys.stdout.write("\n")

    nrecords += 1

  return nrecords, size

def read_ws(dataset, parameters, start, stop):

  from cdasws import CdasWs
  from cdasws.datarepresentation import DataRepresentation
  cdas = CdasWs()

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
  size = -1

  for parameter in parameters:
    data.append(xrdata[parameter].values)
    meta.append({"FILLVAL": xrdata[parameter].FILLVAL, "FORMAT": xrdata[parameter].FORMAT})

  return time, data, meta, size

def read_cdf(file, parameters, start, stop):

  import cdflib
  import cdawmeta

  if file.startswith('http'):
    print(f"Get: {file}")
    begin = cdawmeta.util.tick()
    file = get(file)
    print(f"  Got: {cdawmeta.util.tock(begin):.2f}s {file}")

  meta = []
  data = []

  cdffile  = cdflib.CDF(file)

  size = 0
  depend_0s = []
  parameters = parameters.split(",")
  for parameter in parameters:
    data.append(cdffile.varget(variable=parameter))
    meta.append(cdffile.varattsget(variable=parameter))
    size = size + data[-1].size*data[-1].itemsize
    depend_0s.append(meta[-1]['DEPEND_0'])

  udepend_0s = list(set(depend_0s)); # Unique depend_0s
  assert len(udepend_0s) == 1, 'Multiple DEPEND0s not implemented. Found: ' + ", ".join(udepend_0s)

  epoch = cdffile.varget(variable=depend_0s[0])
  time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

  if isinstance(time, str):
    time = [time]

  return time, data, meta, size

def f2c_specifier(f_template):

  import re

  # TODO: If invalid, return {}

  f_template = f_template.lower().strip(' ')

  # e.g., 10s => s and 10a => s
  fmt = re.sub(r"([0-9].*)([a|s])", r",{:s}", f_template)

  # e.g., i4 => d
  fmt = re.sub(r"([i])([0-9].*)", r",{:d}", f_template)

  # e.g., E11.4 => %.4e, F8.1 => %.1f
  fmt = re.sub(r"([f|e])([0-9].*)\.([0-9].*)", r",{:.\3\1}", f_template)

  return fmt

if __name__ == '__main__':

  def argcheck(args, cl=False):

    if args['lib'] == 'cdflib' and args['file'] is not None:
      #logger.error("file argument is not used with lib=cdasws. Exiting.")
      if cl:
        exit(1)
      return False

    if args['lib'] == 'cdflib':
      if file is None and url is None:
        #logger.error("Error: file or url argument is required with lib=cdflib")
        if cl:
          exit(1)
        return False
      if file is not None and url is not None:
        if cl:
          exit(1)
        return False
        #logger.error("file argument is used instead of url.")

    return True

  def cli():

    import argparse

    clkws = {
      "dataset": {
        "_default": "AC_H2_MFI"
      },
      "parameters": {
        "_default": "Magnitude,BGSEc"
      },
      "start": {
        "_default": "1997-09-03T00:00:00.000000000Z"
      },
      "stop": {
        "_default": "1997-09-04T00:00:00.000000000Z"
      },
      "file": {
        "_default": "ac_h2_mfi_19970902_v04.cdf"
      },
      "url": {
        "_default": "https://cdaweb.gsfc.nasa.gov/pub/data/ace/mag/level_2_cdaweb/mfi_h2/1997/ac_h2_mfi_19970902_v04.cdf"
      },
      "lib": {
        "default": "cdflib",
        "choices": ["cdflib", "cdasws"]
      },
      "test": {
        "action": "store_true",
        "default": False
      },
      "debug": {
        "action": "store_true",
        "default": False
      }
    }

    test_defaults = {}
    for k, v in clkws.items():
      if "_default" in v:
        test_defaults[k] = v["_default"]
      else:
        test_defaults[k] = v["default"]

    import argparse
    parser = argparse.ArgumentParser()
    for k, v in clkws.items():
      if "_default" in v:
        del v["_default"]
      parser.add_argument(f'--{k}', **v)

    args = vars(parser.parse_args())
    if args['test']:
      test_defaults['test'] = True
      return test_defaults

    argcheck(args, cl=True) # Exits if error
    return args

  def report(dt, nrecords, size, what):

    if argv['debug'] == False:
      return

    fmtstr = what + "   {0:8.4f}s | {1:d} records | {2:6d} records/s"
    if size != -1:
      fmtstr += " | {3:d} B"
    print(fmtstr.format(dt, nrecords, int(nrecords/dt), size))

  import cdawmeta
  argv = cli()

  file       = argv['file']
  url        = argv['url']
  lib        = argv['lib']
  dataset    = argv['dataset']
  parameters = argv['parameters']
  start      = argv['start']
  stop       = argv['stop']

  if argv['test']:

    begin_read = cdawmeta.util.tick()
    time, data, meta, size_read = read_cdf(url, parameters, start, stop)
    dt_read = cdawmeta.util.tock(begin_read)

    print(f"\nlib=cdflib")
    print(f"dataset={dataset}&parameters={parameters}&start={start}&stop={stop}")
    report(dt_read, len(time), size_read, 'Read ')

    start = start.replace(".000000000", "")
    stop = stop.replace(".000000000", "")
    msg = f"dataset={dataset}&parameters={parameters}&start={start}&stop={stop}"

    begin_read = cdawmeta.util.tick()
    time, data, meta, size_read = read_ws(dataset, parameters, start, stop)
    dt_read = cdawmeta.util.tock(begin_read)

    print(f"\nlib=cdasws")
    print(msg)
    report(dt_read, len(time), size_read, 'Read ')


if False:
  begin_write = cdawmeta.util.tick()
  n_write, size_write = write(time, meta, data)
  dt_write = cdawmeta.util.tock(begin_write)

  print(f"\nlib={argv['lib']}")
  print(f"dataset={dataset}&parameters={parameters}&start={start}&stop={stop}")
  report(dt_read, len(time), size_read, 'Read ')
  report(dt_write, n_write, size_write, 'Write')
