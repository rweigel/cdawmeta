import cdflib
import cdawmeta

def read_cdf(file, parameters, start=None, stop=None):

  def url2file(url):
    return url.replace("https://cdaweb.gsfc.nasa.gov/pub/data/", "")

  if file.startswith('http'):
    print(f"Get: {file}")
    begin = cdawmeta.util.tick()
    file = cdawmeta.get_file(file, url2file=url2file)
    print(f"  Got: {cdawmeta.util.tock(begin):.2f}s {file}")

  meta = []
  data = []

  cdffile  = cdflib.CDF(file)

  depend_0s = []
  parameters = parameters.split(",")
  for parameter in parameters:
    data.append(cdffile.varget(variable=parameter))
    meta.append(cdffile.varattsget(variable=parameter))
    depend_0s.append(meta[-1]['DEPEND_0'])

  udepend_0s = list(set(depend_0s)); # Unique depend_0s
  assert len(udepend_0s) == 1, 'Multiple DEPEND0s not implemented. Found: ' + ", ".join(udepend_0s)

  epoch = cdffile.varget(variable=depend_0s[0])
  time  = cdflib.cdfepoch.encode(epoch, iso_8601=True) 

  if isinstance(time, str):
    time = [time]

  start_idx = 0
  stop_idx = len(time)

  if start is not None or stop is not None:
    n = len(time)
    startr = start[0:n-1]
    stopr = stop[0:n-1]
    start_found = False
    for idx, t in enumerate(time):
      if start_found == False and t >= startr:
        start_found = True
        start_idx = idx
      if t >= stopr:
        stop_idx = idx + 1
        break

    time = time[start_idx:stop_idx]

  return time, data, meta
