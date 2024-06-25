def logger(format=u'%(asctime)sZ %(message)s',
           file_log=None,
           file_error=None,
           rm_existing=True,
           utc_timestamps=True,
           rm_string=None):

  #'format': '(%(process)d) %(asctime)s %(name)s (line %(lineno)s) | %(levelname)s %(message)s'

  import os
  import sys
  import time
  import inspect
  import logging
  import logging.config

  class CustomFormatter(logging.Formatter):
    def __init__(self, rm_string=rm_string, *args, **kwargs):
      super(CustomFormatter, self).__init__(*args, **kwargs)
      self.rm_string = rm_string

    def format(self, record):
      res = super(CustomFormatter, self).format(record)
      if self.rm_string is None:
        return res
      return res.replace(self.rm_string, '')

  if file_log is None:
    frame = inspect.stack()[1]
    module = inspect.getmodule(frame[0])
    file_log = os.path.splitext(module.__file__)[0] + ".log"

  if file_error is None:
    file_error = os.path.splitext(file_log)[0] + ".errors.log"

  if rm_existing and os.path.exists(file_error):
    os.remove(file_log)
    os.remove(file_error)

  class _ExcludeErrorsFilter(logging.Filter):
    def filter(self, record):
      """Only lets through log messages with log level below ERROR ."""
      return record.levelno < logging.ERROR

  if utc_timestamps:
    logging.Formatter.converter = time.gmtime

  # Based on https://stackoverflow.com/a/66728490
  config = {
      'version': 1,
      'disable_existing_loggers': True,
      'filters': {
          'exclude_errors': {
              '()': _ExcludeErrorsFilter
          }
      },
      'formatters': {
          # Modify log message format here or replace with your custom formatter class
          'my_formatter': {
            'format': format
          }
      },
      'handlers': {
          'console_stderr': {
              # Sends log messages with log level ERROR or higher to stderr
              'class': 'logging.StreamHandler',
              'level': 'ERROR',
              'formatter': 'my_formatter',
              'stream': sys.stderr
          },
          'file_ERROR': {
              # Sends all log messages to a file
              'class': 'logging.FileHandler',
              'level': 'ERROR',
              'formatter': 'my_formatter',
              'filename': file_error,
              'encoding': 'utf8'
          },
          'console_stdout': {
              # Sends log messages with log level lower than ERROR to stdout
              'class': 'logging.StreamHandler',
              'level': 'DEBUG',
              'formatter': 'my_formatter',
              'filters': ['exclude_errors'],
              'stream': sys.stdout
          },
          'file_DEBUG': {
              # Sends all log messages to a file
              'class': 'logging.FileHandler',
              'level': 'DEBUG',
              'formatter': 'my_formatter',
              'filename': file_log,
              'encoding': 'utf8'
          }
      },
      'root': {
          # In general, this should be kept at 'NOTSET'.
          # Otherwise it would interfere with the log levels set for each handler.
          'level': 'NOTSET',
          'handlers': [
            'console_stderr',
            'file_ERROR',
            'console_stdout',
            'file_DEBUG'
          ]
      },
  }


  logging.config.dictConfig(config)

  for handler in logging.getLogger().handlers:
    handler.setFormatter(CustomFormatter(rm_string=rm_string))

  return logging.getLogger(__name__)
