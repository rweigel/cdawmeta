import os
import glob

import git

import cdawmeta

def additions(logger):

  if hasattr(additions, 'additions'):
    return additions.additions

  repo_path = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-spase')

  if not os.path.exists(repo_path):
    repo_url = cdawmeta.CONFIG['urls']['cdawmeta-spase']
    logger.info(f"Cloning {repo_url} into {repo_path}")
    git.Repo.clone_from(repo_url, repo_path, depth=1)

  pattern = f"{repo_path}/*.json"
  files = glob.glob(pattern, recursive=True)
  additions_ = {}
  for file in files:
    logger.info(f"Reading {file}")
    key = os.path.basename(file).replace(".json", "")
    additions_[key] = cdawmeta.util.read(file)

  additions.additions = additions_
  return additions_
