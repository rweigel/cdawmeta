def omit(id):
  return False
  if id.startswith("A"):
    return False
  return True

# AIM_CIPS_SCI_3A has large master cdf.
omitids = ['AIM_CIPS_SCI_3A']
allxml  = 'https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml'
max_workers = 5

import os
import json

try:
  # TODO: Create and use setup.py
  import requests_cache
  import xmltodict
except:
  print(os.popen('pip install xmltodict requests_cache').read())

import requests_cache
import xmltodict

base_dir = os.path.dirname(__file__)
all_file = os.path.join(base_dir, 'data/all-resolved.json')
os.makedirs(os.path.join(base_dir, 'data'), exist_ok=True)

def CachedSession(cdir):
  from datetime import timedelta
  # https://requests-cache.readthedocs.io/en/stable/#settings
  # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html

  # Cache dir
  copts = {
    "use_cache_dir": True,                # Save files in the default user cache dir
    "cache_control": True,                # Use Cache-Control response headers for expiration, if available
    "expire_after": timedelta(days=1),    # Otherwise expire responses after one day
    "allowable_codes": [200],             # Cache responses with these status codes
    "stale_if_error": True,               # In case of request errors, use stale cache data if possible
    "backend": "filesystem"
  }
  return requests_cache.CachedSession(cdir, **copts)

def create_datasets(allxml):

  """
  Create a list of datasets; each element has content of dataset node in
  all.xml. An info and id node is also added.
  """

  cdir = os.path.join(os.path.dirname(__file__), 'data/cache/all')
  session = CachedSession(cdir)

  resp = session.get(allxml)
  allxml_text = resp.text

  all_dict = xmltodict.parse(allxml_text);

  datasets = []
  for dataset_allxml in all_dict['sites']['datasite'][0]['dataset']:

    id = dataset_allxml['@serviceprovider_ID']

    if id in omitids or omit(id):
      continue

    startDate = dataset_allxml['@timerange_start'].replace(' ', 'T') + 'Z';
    stopDate = dataset_allxml['@timerange_stop'].replace(' ', 'T') + 'Z';

    contact = ''
    if 'data_producer' in dataset_allxml:
      if '@name' in dataset_allxml['data_producer']:
        contact = dataset_allxml['data_producer']['@name']
      if '@affiliation' in dataset_allxml['data_producer']:
        contact = contact + " @ " + dataset_allxml['data_producer']['@affiliation']

    dataset = {
                'id': id,
                'info': {
                    'startDate': startDate,
                    'stopDate': stopDate,
                    'resourceURL': f'https://cdaweb.gsfc.nasa.gov/misc/Notes{id[0]}.html#' + id,
                    'contact': contact
                },
                '_allxml': dataset_allxml
    }

    if not 'mastercdf' in dataset_allxml:
      print('No mastercdf for ' + id)
      continue

    if not '@ID' in dataset_allxml['mastercdf']:
      print('No ID attribute for mastercdf for ' + id)
      continue

    datasets.append(dataset)

  return datasets

def add_master(datasets):

  """
  Add a _master key to each dataset containing JSON from master CDF
  """

  def get_master(dataset):

    mastercdf = dataset['_allxml']['mastercdf']['@ID']
    masterjson = mastercdf.replace('.cdf', '.json').replace('0MASTERS', '0JSONS')

    r = session.get(masterjson)
    print(f'Read: (from cache={r.from_cache}) {masterjson}')
    dataset['_master'] = r.json()

    files = list(dataset['_master'].keys())
    if len(files) > 1:
      print("Aborting. Expected only one file key in _master object for " + dataset['id'])
      exit(1)

  cdir = os.path.join(os.path.dirname(__file__), 'data/cache/masters')
  session = CachedSession(cdir)

  if max_workers == 1:
    for dataset in datasets:
      dataset['_master'] = get_master([dataset, session])
  else:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(get_master, datasets)

def add_spase(datasets):

  """
  Add a _spase key to each dataset containing JSON from master CDF
  """

  def spase_url(dataset):

    if '_master' not in dataset:
      return None

    files = list(dataset['_master'].keys())
    if len(files) > 1:
        print("Expected only one file key in _master object.")
        exit(0)

    global_attributes = dataset['_master'][files[0]]['CDFglobalAttributes']
    for attribute in global_attributes:
      if 'spase_DatasetResourceID' in attribute:
        id = attribute['spase_DatasetResourceID'][0]['0']
        return id.replace('spase://', 'https://hpde.io/') + '.json';

  def get_spase(dataset):

    url = spase_url(dataset)
    if url is None:
      dataset['_spase'] = None
      return

    r = session.get(url)
    print(f'Read: (from cache={r.from_cache}) {url}')
    dataset['_spase'] = r.json()['Spase']

  cdir = os.path.join(os.path.dirname(__file__), 'data/cache/spase')
  session = CachedSession(cdir)

  if max_workers == 1:
    for dataset in datasets:
      dataset['_spase'] = get_spase([dataset, session])
  else:
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
      pool.map(get_spase, datasets)

datasets = create_datasets(allxml)

add_master(datasets)
add_spase(datasets)

print(f'# of datasets: {len(datasets)}')

print(f'Writing {all_file}')
with open(all_file, 'w', encoding='utf-8') as f:
  json.dump(datasets, f, indent=2)
print(f'Wrote {all_file}')