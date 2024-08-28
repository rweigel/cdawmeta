def get_path(obj, path):
  for key in path:
    if obj is None:
      return None
    if key not in obj:
      return None
    obj = obj[key]
  return obj
