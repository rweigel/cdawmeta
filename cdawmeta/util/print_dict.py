from .sort_dict import sort_dict
def print_dict(d, sort=False, indent=0):

  if not isinstance(d, dict):
    print(d)
    return
  if sort:
    d = sort_dict(d)

  for key, value in d.items():
    end = ''
    if isinstance(value, dict):
      end = '\n'
    print(' ' * indent + str(key), end=end)
    if isinstance(value, dict):
        print_dict(value, sort=sort, indent=indent+1)
    else:
      if isinstance(value, str):
        print(f": '{value}'")
      else:
        print(f": {value}")
