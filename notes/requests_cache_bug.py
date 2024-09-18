import requests_cache
copts = {
  # Save files in the default user cache dir
  "use_cache_dir": True,

  # Use Cache-Control response headers for expiration, if available
  "cache_control": True,

  "expire_after": 0,

  # Cache responses with these status codes
  "allowable_codes": [200],

  # In case of request errors, use stale cache data if possible
  "stale_if_error": True,

  "serializer": "json",

  # This causes caching to not work unless decode_content = False
  "backend": "filesystem",

  # https://github.com/requests-cache/requests-cache/issues/963
  "decode_content": False
}
session = requests_cache.CachedSession('/tmp', **copts)

#headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
headers = {'Accept': 'application/json'}
url = "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/AC_H2_MFI/orig_data/19970902T000000Z,20240323T230000Z"
resp = session.get(url, headers=headers)
print(resp.from_cache)