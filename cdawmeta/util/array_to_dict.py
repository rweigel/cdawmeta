def array_to_dict(array, key=None):
  """Convert array of dicts to dict of dicts

  If `array` is a list of dicts, each with one key-value pair
  array_to_dict(array) returns a dict with the key-value pairs, e.g,
  array_to_dict([{key1: value1}, {key2: value1}, ...])
                ->
                 {key1: value1, key2: value2, ...}
  If a dict in `array` has more than one key-value pair, None is returned.

  If `array` is a list of dicts, each with one or more key-value pairs
  array_to_dict(array, key=KEY) returns a dict with keys of the VALUE of KEY
  from each dict, e.g,
  array_to_dict([{KEY: value1, ...}, {KEY: value2}, ...])
                ->
                {value1: {KEY: value1, ...}, value2: {KEY: value2, ...}}
  If a dict in `array` does not have a key of KEY, None is returned.
  """

  if not isinstance(array, list):
    return array

  obj = {}
  for array_elem in array:
    if key is None:
      elem_keys = list(array_elem.keys())
      if len(elem_keys) != 1:
        return None
      obj[elem_keys[0]] = array_elem[elem_keys[0]]
    else:
      if key in array_elem:
        obj[array_elem[key]] = array_elem
      else:
        return None

  return obj

if __name__ == '__main__':
  array = [{'key1': 'value1'}, {'key2': {'a': 'a', 'b': 'b'}}]
  print(array)
  print(array_to_dict(array))
  print("---")
  array = [{'key1': 'value1', 'other': 'other1'}, {'key1': 'value2', 'other': 'other2'}]
  print(array)
  print(array_to_dict(array, key='key1'))
