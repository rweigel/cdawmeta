
loggers = {}

def logger(name=None):
  import os
  import sys

  import cdawmeta

  if name is None:
    name = sys._getframe(1).f_code.co_name

  if name in loggers:
    # Note that with threading, it is possible for the logger to be created
    # multiple times because the following code may not have been executed
    # to the point where logger[name] is set. This can be fixed by putting
    # the logger creation code in __init__.py.
    #print(f'Logger {name} already exists')
    return loggers[name]
  else:
    pass
    #print(f'Creating logger {name}')

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
    config = config.copy()
    if config[file_type]:
      file_ = os.path.normpath(config[file_type].format(name=name))
      if not os.path.isabs(file_):
        config[file_type] = os.path.join(data_dir, file_)

  loggers[name] = cdawmeta.util.logger(**config)

  return loggers[name]
