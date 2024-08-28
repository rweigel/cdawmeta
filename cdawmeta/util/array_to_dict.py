def array_to_dict(array, key=None):
  """Convert array of dicts to dict of dicts
  
  array_to_dict([{key1: value1}, {key2: value1}, ...]) -> 
                 {key1: value1, key2: value2, ...}

  array_to_dict([{key: value1, ...}, {key: value2}, ...]) -> 
                 {value1: {key: value1, ...}, value2: {key: value2, ...}}
  """

  obj = {}
  for array_idx, array_elem in enumerate(array):
    if key is None:
      elem_keys = list(array_elem.keys())
      if len(elem_keys) != 1:
        msg = "Each array element must be a dict with one key. "
        msg += f"Array element {array_idx} has {len(elem_keys)} keys: {elem_keys}"
        raise ValueError(msg)
      obj[elem_keys[0]] = array_elem[elem_keys[0]]
    else:
      if key in array_elem:
        obj[array_elem[key]] = array_elem
      else:
        raise ValueError(f"Array element {array_idx} does not have key '{key}'")

  return obj

if __name__ == '__main__':
  array = [{'key1': 'value1'}, {'key2': {'a': 'a', 'b': 'b'}}]
  print(array)
  print(array_to_dict(array))
  print("---")
  array = [{'key1': 'value1', 'other': 'other1'}, {'key1': 'value2', 'other': 'other2'}]
  print(array)
  print(array_to_dict(array, key='key1'))
