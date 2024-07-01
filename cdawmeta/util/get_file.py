def get_file(url, url2file=None):

  import os
  from urllib.request import urlopen
  from shutil import copyfileobj

  import cdawmeta

  if url2file is not None:
    file_name = url2file(url)
  else:
    file_name = url.split('/')[-1]

  cdawmeta.util.mkdir(os.path.dirname(file_name))

  length=16*1024
  req = urlopen(url)
  with open(file_name, 'wb') as fp:
    copyfileobj(req, fp, length)

  headers = dict(req.getheaders())
  cdawmeta.util.write(file_name + ".headers.json", headers)

  return file_name
