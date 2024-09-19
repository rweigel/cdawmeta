def mkdir(dirname, logger=None):
  if dirname == '':
    if logger is not None:
      logger.info(f"mkdir({dirname}): dirname = ''. Not creating directory.")
    return

  import os
  if not os.path.exists(dirname):
    if logger is not None:
      logger.info(f"Creating dir {dirname}")
    os.makedirs(dirname, exist_ok=True)
