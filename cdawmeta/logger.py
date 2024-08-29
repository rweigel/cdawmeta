def logger(name):
  import os
  import cdawmeta

  data_dir = cdawmeta.DATA_DIR
  config = cdawmeta.CONFIG['logger'][name]

  for file_type in ['file_log', 'file_error']:
    if config[file_type]:
      file_ = os.path.normpath(config[file_type].format(name=name))
      if os.path.isabs(file_) == False:
        config[file_type] = os.path.join(data_dir, file_)

  return cdawmeta.util.logger(**config)
