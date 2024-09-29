import os
import glob

import cdawmeta

def additions(logger):

  if hasattr(additions, 'additions'):
    return additions.additions

  additions_path = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-additions')
  pattern = f"{additions_path}/*.json"
  files = glob.glob(pattern, recursive=True)
  additions_ = {}
  for file in files:
    logger.info(f"Reading {file}")
    key = os.path.basename(file).replace(".json", "")
    additions_[key] = cdawmeta.util.read(file)

  additions.additions = additions_
  return additions_
