from time import sleep
import cdawmeta
from cdawmeta.metadata import allxml
cdawmeta.DATA_DIR = "data"

log_level = 'info'
interval = 1 # Time between checks in seconds
logger = cdawmeta.logger(name='updater', log_level=log_level)

res = allxml(update=True, log_level=log_level)
last_modified = None
if 'request' not in res:
  logger.info(f"Request failed with error: {res['error']}")
else:
  last_modified = res['request']['file-header']['Last-Modified']

last_modified_now = None
while True:
  sleep(interval)
  logger.info("Checking for update")
  res = allxml(update=True, diffs=True, log_level=log_level)
  if 'request' not in res:
    logger.info(f"Request failed with error: {res['error']}")
  else:
    last_modified_now = res['request']['file-header']['Last-Modified']
    if last_modified is not None:
      if last_modified_now != last_modified:
        logger.info("allxml changed. Diff:")
        logger.info(res['request']['diff'])
      else:
        logger.info("allxml did not change")
        pass
      cdawmeta.metadata(update=True, update_skip=['cadence'])
      # TODO: Rename data/metadata.log and archive
  last_modified = last_modified_now