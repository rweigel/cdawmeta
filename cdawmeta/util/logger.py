def logger(name=None,
           format=u'%(asctime)sZ %(message)s',
           console_format=u"%(asctime)s %(levelname)s %(name)s %(message)s",
           datefmt="%Y-%m-%dT%H:%M:%S.%f",
           utc_timestamps=True,
           file_log=None,
           file_error=None,
           rm_existing=True,
           rm_string=None,
           disable_existing_loggers=False):

  #'format': '(%(process)d) %(asctime)s %(name)s (line %(lineno)s) | %(levelname)s %(message)s'

  import os
  import sys
  import time
  import inspect
  import logging
  import logging.config

  if utc_timestamps:
    logging.Formatter.converter = time.gmtime

  class CustomFormatter(logging.Formatter):
    def __init__(self, rm_string=rm_string, datefmt=datefmt, *args, **kwargs):
      super(CustomFormatter, self).__init__(*args, **kwargs)
      self.rm_string = rm_string
      self.datefmt = datefmt

    import datetime as dt
    converter=dt.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
          s = ct.strftime("%Y-%m-%dT%H:%M:%S.%f")
        if utc_timestamps:
          s = s + "Z"
        return s

    def format(self, record):
      record.pathname = record.pathname.replace(os.getcwd() + "/","")
      res = super(CustomFormatter, self).format(record)
      if self.rm_string is None:
        return res
      return res.replace(self.rm_string, '')

  if name is None:
    name = __name__ # Use top-level module name

  if file_log is None:
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    file_log = os.path.splitext(module.__file__)[0] + ".log"

  if file_error is None:
    file_error = os.path.splitext(file_log)[0] + ".errors.log"

  if rm_existing:
    if os.path.exists(file_log):
      os.remove(file_log)
    if file_error and os.path.exists(file_error):
      os.remove(file_error)

  from . import mkdir as mkdir
  if file_log:
    mkdir(os.path.dirname(file_log))
  if file_error:
    mkdir(os.path.dirname(file_error))

  class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
      """Only show log messages with log level below ERROR."""
      return record.levelno < logging.ERROR

  handlers = [
            'console_stderr',
            'console_stdout',
            'file_stdout'
          ]

  if file_error:
    handlers.append('file_stderr')

  # Based on https://stackoverflow.com/a/66728490
  config = {
      'version': 1,
      'disable_existing_loggers': disable_existing_loggers,
      'filters': {
          'exclude_errors': {
              '()': _ExcludeErrorsFilter
          }
      },
      'formatters': {
          'console_formatter': {
            "class": "logging.Formatter",
            "datefmt": datefmt,
            "format": console_format
           },
          'file_formatter': {
            'format': f'{format}'
          }
      },
      'handlers': {
          'console_stderr': {
              # Sends log messages with log level ERROR or higher to stderr
              'class': 'logging.StreamHandler',
              'level': 'ERROR',
              'formatter': 'console_formatter',
              'stream': sys.stderr
          },
          'file_stderr': {
              # Sends all log messages to a file
              'class': 'logging.FileHandler',
              'level': 'ERROR',
              'formatter': 'file_formatter',
              'filename': file_error,
              'encoding': 'utf8'
          },
          'console_stdout': {
              # Sends log messages with log level lower than ERROR to stdout
              'class': 'logging.StreamHandler',
              'level': 'DEBUG',
              'formatter': 'console_formatter',
              'filters': ['exclude_errors'],
              'stream': sys.stdout
          },
          'file_stdout': {
              # Sends all log messages to a file
              'class': 'logging.FileHandler',
              'level': 'DEBUG',
              'formatter': 'file_formatter',
              'filename': file_log,
              'encoding': 'utf8'
          }
      },
      'loggers': {
          name: {
              'level': 'DEBUG',
              'handlers': handlers
          }
      },
      'xroot': {
          # Docs say:
          #   In general, this should be kept at 'NOTSET'.
          #   Otherwise it would interfere with the log levels set for each handler.
          # However, this leads to duplicate log messages.
          'level': 'NOTSET',
          'handlers': handlers
      }
  }

  if name is not None:
    msgx = f"for {name} "
  print(f'Logging output {msgx}to: {file_log}')
  if file_error:
    print(f'Logging errors {msgx}to: {file_error}')
  else:
    del config['handlers']['file_stderr']

  logging.config.dictConfig(config)

  _logger = logging.getLogger(name)
  for handler in _logger.handlers:
    handler.setFormatter(CustomFormatter(fmt=handler.formatter._fmt, rm_string=rm_string))

  return logging.getLogger(name)
