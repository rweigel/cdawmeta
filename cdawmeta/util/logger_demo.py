import os
import logging

import cdawmeta
log_dir = os.path.dirname(__file__)

config0 = {
  'name': 'logger0',
  'color': True,
  'debug_logger': True
}

logger0 = cdawmeta.util.logger(**config0)
logger0.setLevel(logging.DEBUG)
#logger0.info('Logger0 info message')
logger0.debug('Logger0 debug message')
exit()

logger0 = cdawmeta.util.logger(**config0)
logger0.info('Logger0 info message')

print('')

config1 = {
  'name': 'logger1',
  'file_log': os.path.join(log_dir, 'logger_demo2.log'),
  'file_error': os.path.join(log_dir, 'logger_demo2.errors.log'),
  'console_format': '%(asctime)s p%(process)s %(pathname)s:%(lineno)d %(levelname)s - %(message)s',
  'file_format': u'%(asctime)s %(levelname)s %(name)s %(message)s',
  'datefmt': '%Y-%m-%dT%H:%M:%S',
  'rm_string': log_dir + '/',
  'color': True,
  'debug_logger': False
}

logger2 = cdawmeta.util.logger(**config1)
logger2.info('Logger2 info message')
logger2.error('Logger2 error message')




