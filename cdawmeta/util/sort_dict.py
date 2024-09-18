def sort_dict(d):
  import collections
  if not isinstance(d, dict):
    return d
  d = collections.OrderedDict(sorted(d.items()))
  for key in d:
    d[key] = sort_dict(d[key])
  return d
