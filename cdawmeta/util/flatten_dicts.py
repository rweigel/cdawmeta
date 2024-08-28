def flatten_dicts(d, parent_key='', sep='/'):
  items = []
  for k, v in d.items():
    new_key = parent_key + sep + k if parent_key else k
    if isinstance(v, dict):
      items.extend(flatten_dicts(v, new_key, sep=sep).items())
    else:
      items.append((new_key, v))
  return dict(items)

if __name__ == '__main__':
  d = {
    'a': 1,
    'b': {
      'c': 2,
      'd': 3
    }
  }
  d['b']['c'] = d['b'].copy()
  import cdawmeta
  cdawmeta.util.print_dict(d)
  print(flatten_dicts(d))