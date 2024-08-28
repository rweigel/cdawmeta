import os

def logger(name):
  import cdawmeta
  data_dir = cdawmeta.DATA_DIR
  config = {
    'cdaweb': {
        'name': 'cdaweb',
        'file_log': os.path.join(data_dir, f'{name}.log'),
        'file_error': os.path.join(data_dir, f'{name}.errors.log'),
        'console_format': '%(name)s %(pathname)s:%(lineno)d %(levelname)s %(message)s',
        'rm_string': data_dir + '/'
    },
    'hapi': {
          'name': 'hapi',
          'file_log': os.path.join(data_dir, 'hapi', f'{name}.log'),
          'file_error': None, # This is handled by hapi() b/c it needs formatting.
          'console_format': '%(name)s %(pathname)s:%(lineno)d %(levelname)s %(message)s',
          'rm_string': data_dir + '/'
    },
    'table': {
        'name': 'table',
        'file_log': os.path.join(data_dir, 'table', f'{name}.log'),
        'file_error': os.path.join(data_dir, 'table', f'{name}.errors.log'),
        'console_format': '%(name)s %(pathname)s:%(lineno)d %(levelname)s %(message)s',
        'rm_string': data_dir + '/'
    },
    'query': {
        'name': 'query',
        'file_log': os.path.join(data_dir, 'query', f'{name}.log'),
        'file_error': os.path.join(data_dir, 'query', f'{name}.errors.log'),
        'console_format': '%(message)s',
        'rm_string': data_dir + '/'
    }
  }
  return cdawmeta.util.logger(**config[name])
