
loggers = {}

def logger(name=None, dir_name=None, log_level='info'):
  import os
  import sys

  import cdawmeta

  if name is None:
    name = sys._getframe(1).f_code.co_name

  if dir_name is None:
    dir_name = name

  if name in loggers:
    # Note that with threading, it is possible for the logger to be created
    # multiple times because the following code may not have been executed
    # to the point where logger[name] is set. This can be fixed by putting
    # the logger creation code in __init__.py.
    #print(f'Logger {name} already exists')
    return loggers[name]
  else:
    pass

  data_dir = cdawmeta.DATA_DIR

  msgs = [f"Creating logger with name = '{name}'"]
  config_default = cdawmeta.CONFIG['logger']['default']
  if name in cdawmeta.CONFIG['logger']:
    config = {**config_default, **cdawmeta.CONFIG['logger'][name]}
  else:
    config = config_default.copy()
    config['name'] = name
    msgs.append(f"No logger configuration for name '{name}' in config.json. Using default configuration.")

  #rm_string = os.path.dirname(os.path.abspath(os.path.join(__file__, '..')))
  #config['rm_string'] = rm_string + "/"

  for file_type in ['file_log', 'file_error']:
    config = config.copy()
    if config[file_type]:
      file_ = os.path.normpath(config[file_type].format(dir_name=dir_name, name=name))
      if not os.path.isabs(file_):
        config[file_type] = os.path.join(data_dir, file_)

  if log_level.lower() == 'debug':
    #config['debug_logger'] = True
    config['console_format'] = config['console_format_debug']
  del config['console_format_debug']

  loggers[name] = cdawmeta.util.logger(**config)
  loggers[name].setLevel(log_level.upper())

  if log_level.lower() == 'debug':
    for msg in msgs:
      loggers[name].debug(msg)

    loggers[name].debug(f"Logger config: {config}")

  return loggers[name]
