def logger(name=None):
  import os
  import sys
  import cdawmeta

  if name is None:
    name = sys._getframe(1).f_code.co_name

  data_dir = cdawmeta.DATA_DIR
  if name in cdawmeta.CONFIG['logger']:
    config = cdawmeta.CONFIG['logger'][name]
  else:
    config = cdawmeta.CONFIG['logger']['default']
    config['name'] = name
    msg = f'No logger configuration for name {name} in config.json'
    print(msg)
    #raise ValueError(f'No logger configuration for name {name} in config.json')

  #rm_string = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
  #config['rm_string'] = rm_string + "/"

  for file_type in ['file_log', 'file_error']:
    if config[file_type]:
      file_ = os.path.normpath(config[file_type].format(name=name))
      if not os.path.isabs(file_):
        config[file_type] = os.path.join(data_dir, file_)

  return cdawmeta.util.logger(**config)
