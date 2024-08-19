import pprint
from .sort_dict import sort_dict

def print_dict(d, sort_dicts=False, indent=0, use_pprint=False):

  if not isinstance(d, dict):
    print(d)
    return

  if sort_dicts:
    d = sort_dict(d)

  if use_pprint:
    pp = pprint.PrettyPrinter(depth=4, indent=0, compact=True)
    pp.pprint(d)
    return

  # TODO: There must be a library that does this. Find it.
  for key, value in d.items():
    end = ''
    if isinstance(value, dict):
      end = '\n'
    print(' ' * indent + str(key), end=end)
    if isinstance(value, dict):
        print_dict(value, sort_dicts=sort_dicts, indent=indent+1)
    else:
      if isinstance(value, str):
        print(f": '{value}'")
      else:
        if isinstance(value, list):
          if len(value) < 5:
            print(f": {value}")
          else:
            # TODO: If element is string, they are not quoted in the following. Fix this.
            print(f": [{value[0]}, {value[1]}, ..., {value[len(value)-2]}, {value[len(value)-1]} ({len(value)} elements)")
        else:
          print(f": {value}")

