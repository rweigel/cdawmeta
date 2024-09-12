import cdawmeta

urls = cdawmeta.util.read('data/query/query-hpde.io-ids-urls.txt')
urls = urls.split('\n')

import requests
from concurrent.futures import ThreadPoolExecutor

results = []
def call(url):
  if not url.startswith('http'):
    return
  print(url)
  try:
    response = requests.head(url, timeout=10, allow_redirects=True)
    result = f"{response.status_code} {url}"
    print(result)
    results.append(result)
  except Exception as e:
    print(e)

with ThreadPoolExecutor(max_workers=4) as pool:
  pool.map(call, urls)
