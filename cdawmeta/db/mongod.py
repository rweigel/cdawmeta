import os
import subprocess

import cdawmeta

def mongod(collection_name, logger=None, mongod_binary=None, db_path=None, port=27017):

  if mongod_binary is None:
    raise ValueError("Must specify mongod_binary, e.g., /bin/mongod")

  db_path = os.path.join(cdawmeta.DATA_DIR, 'mongodb', collection_name)
  cdawmeta.util.mkdir(db_path)

  cmd = f"pkill -f mongod --port {port}"
  logger.info(f"Killing any existing mongod with: {cmd}")
  os.system(cmd)

  import time
  # Would be better to get pid from ps and then wait for it to die
  # https://stackoverflow.com/questions/7653178/wait-until-a-certain-process-knowing-the-pid-end
  # Really we should not be starting mongod from Python; this is used for testing
  # and should be replaced with a proper service manager.
  logger.info("Sleeping for 0.5 seconds to allow mongod to die")
  time.sleep(0.5)

  cmd = f"{mongod_binary} --port {port} --dbpath {db_path} --logpath {db_path}/mongod.log --fork"
  logger.info(f"Starting mongod: {cmd}")
  with subprocess.Popen(cmd.split(" "), stdout=subprocess.PIPE, stderr=subprocess.PIPE) as mongod_:
    for line in mongod_.stdout:
      line = line.decode().strip()
      # Won't get pid until after this function returns
      #if 'fork process' in line:
      #pid = line.split(":")[1]
      logger.info(f"mongod stdout: {line}")

  if True:
    if mongod_.returncode != 0:
      print("mongod_.returncode: ", mongod_.returncode)
      raise subprocess.CalledProcessError(mongod_.returncode, mongod_.args)

  return mongod_.returncode