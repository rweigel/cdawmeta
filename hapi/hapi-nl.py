test_run = False

base_url = "https://cdaweb.gsfc.nasa.gov/hapi"
initial = 'nl'

def omit(id):
  if test_run:
    if id.startswith("AC_"):
      return False
    return True
  else:
    return False

import os
import json
import requests_cache

base_dir = os.path.dirname(__file__)
out_file = os.path.join(base_dir, '..', 'data', 'hapi', f'catalog-all.{initial}.json')
os.makedirs(os.path.dirname(out_file), exist_ok=True)

def CachedSession():
  import os
  from datetime import timedelta
  # https://requests-cache.readthedocs.io/en/stable/#settings
  # https://requests-cache.readthedocs.io/en/stable/user_guide/headers.html

  # Cache dir
  cdir = os.path.join(os.path.dirname(__file__), '..', 'data', 'cache', f'hapi-{initial}')
  copts = {
    "use_cache_dir": True,                # Save files in the default user cache dir
    "cache_control": True,                # Use Cache-Control response headers for expiration, if available
    "expire_after": timedelta(days=1),    # Otherwise expire responses after one day
    "allowable_codes": [200],             # Cache responses with these status codes
    "stale_if_error": True,               # In case of request errors, use stale cache data if possible
    "backend": "filesystem"
  }
  return requests_cache.CachedSession(cdir, **copts)

session = CachedSession()

resp = session.get(base_url + '/catalog')
if resp.status_code != 200:
  print('Error. Server returned status code of ' + str(resp.status_code))
  exit(1)

try:
  datasets = resp.json()['catalog']
except:
  # TODO: Catch other errors.
  print('Error. Could not parse catalog. Response given below.')
  print(resp.text)
  exit(1)

for idx, dataset in enumerate(datasets):

  id = dataset['id']
  if omit(id):
    print(f'Omitting {id}')
    datasets[idx] = None
    continue

  url = base_url + '/info?id=' + id
  resp = session.get(url)
  print(f'Read: (from cache={resp.from_cache}) {url}')
  if resp.status_code != 200:
    continue

  try:
    dataset['info'] = resp.json()
  except:
    print('Error. Could not info response. Response given below.')
    print(resp.text)
    exit(1)

  del dataset['info']['status']
  del dataset['info']['HAPI']

datasets = [i for i in datasets if i is not None]

with open(out_file, 'w', encoding='utf-8') as f:
  json.dump(datasets, f, indent=2)
print(f'Wrote: {out_file}')