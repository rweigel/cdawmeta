def logger(name):
  import os
  import cdawmeta

  data_dir = cdawmeta.DATA_DIR
  if name not in cdawmeta.CONFIG['logger']:
    raise ValueError(f'No logger configuration for name {name} in config.json')
  config = cdawmeta.CONFIG['logger'][name]

  rm_string = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
  config['rm_string'] = rm_string + "/"

  for file_type in ['file_log', 'file_error']:
    if config[file_type]:
      file_ = os.path.normpath(config[file_type].format(name=name))
      if not os.path.isabs(file_):
        config[file_type] = os.path.join(data_dir, file_)

  return cdawmeta.util.logger(**config)
