import os
import cdawmeta
log_dir = os.path.dirname(__file__)

config0 = {
  'name': 'logger0',
}

_logger0 = cdawmeta.util.logger(**config0)
_logger0.info('Logger0 info message')

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

_logger2 = cdawmeta.util.logger(**config1)
_logger2.info('Logger2 info message')
_logger2.error('Logger2 error message')




