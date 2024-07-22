import sys
import numpy as np

def write_csv(time, meta, data):

  from cdawmeta import f2c_specifier

  # https://stackoverflow.com/a/30091579
  from signal import signal, SIGPIPE, SIG_DFL
  signal(SIGPIPE, SIG_DFL)

  nrecords = 0
  size = 0
  for t in range(len(time)):

    tstr = str(time[t])

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

if __name__ == '__main__':

  def argcheck(args, cl=False):

    if args['lib'] == 'cdflib' and args['file'] is not None:
      #logger.error("file argument is not used with lib=cdasws. Exiting.")
      if cl:
        exit(1)
      return False

    if args['lib'] == 'cdflib':
      if args['file'] is None and args['url'] is None:
        #logger.error("Error: file or url argument is required with lib=cdflib")
        if cl:
          exit(1)
        return False
      if args['file'] is not None and args['url'] is not None:
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

  def report(dt, nrecords, what):

    fmtstr = what + "   {0:8.4f}s | {1:d} records | {2:6d} records/s"
    print(fmtstr.format(dt, nrecords, int(nrecords/dt)))

  import cdawmeta
  argv = cli()

  file       = argv['file']
  url        = argv['url']
  lib        = argv['lib']
  dataset    = argv['dataset']
  parameters = argv['parameters']
  start      = argv['start']
  stop       = argv['stop']

  def print_csv(time, meta, data, lib):
    print(f"\n{lib}")
    nrecords, size = write_csv(time, meta, data)
    print(f"{lib}: {nrecords} records, {size} bytes")

  if argv['test']:

    print(f"\n{dataset}&parameters={parameters}&start={start}&stop={stop}")

    begin_read = cdawmeta.util.tick()
    time1, data1, meta1 = cdawmeta.read_cdf(url, parameters, start, stop)
    dt_read = cdawmeta.util.tock(begin_read)

    report(dt_read, len(time1), 'Read[cdflib] ')

    start = start.replace(".000000000", "")
    stop = stop.replace(".000000000", "")

    begin_read = cdawmeta.util.tick()
    time2, data2, meta2 = cdawmeta.read_ws(dataset, parameters, start, stop)
    dt_read = cdawmeta.util.tock(begin_read)

    report(dt_read, len(time2), 'Read[cdasws] ')

    print_csv(time1, meta1, data1, 'cdflib')
    print_csv(time2, meta2, data2, 'cdasws')