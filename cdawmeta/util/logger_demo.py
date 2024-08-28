import os
import cdawmeta
log_dir = os.path.dirname(__file__)

config1 = {
  'name': 'logger1',
}

_logger1 = cdawmeta.util.logger(**config1)
_logger1.info('Logger1 info message')

print('')

config2 = {
  'name': 'logger1',
  'file_log': os.path.join(log_dir, 'logger_demo2.log'),
  'file_error': os.path.join(log_dir, 'logger_demo2.errors.log'),
  'console_format': '%(asctime)s p%(process)s %(pathname)s:%(lineno)d %(levelname)s - %(message)s',
  'file_format': u'%(asctime)s %(levelname)s %(name)s %(message)s',
  'datefmt': '%Y-%m-%dT%H:%M:%S',
  'rm_string': log_dir + '/',
  'color': True
}

_logger2 = cdawmeta.util.logger(**config2)
_logger2.info('Logger2 info message')




