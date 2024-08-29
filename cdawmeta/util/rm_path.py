def rm_path(d, keys, ignore_error=True):
  # https://stackoverflow.com/a/74583007
  for k in keys[:-1]:
    d = d[k]

  if ignore_error:
    d.pop(keys[-1], None)
  else:
    d.pop(keys[-1])

if __name__ == '__main__':
  d = {'a': {'b': {'c': 1}}}
  print(f"Initial:  {d}")
  rm_path(d, ['a', 'b'])
  print(f"Modified: {d}")
  # Initial:  {'a': {'b': {'c': 1}}}
  # Modified: {'a': {}}

  d = {'a': {'b': {'c': 1}}}
  print(f"Initial:  {d}")
  rm_path(d, ['a', 'b', 'c'])
  print(f"Modified:  {d}")
  # Initial:  {'a': {'b': {'c': 1}}}
  # Modified:  {'a': {'b': {}}}

  d = {'a': {'b': {'c': 1}}}
  # Does not raise an error
  print(f"Initial:  {d}")
  rm_path(d, ['a', 'x'])
  print(f"Modified: {d}")
  # Initial:  {'a': {'b': {'c': 1}}}
  # Modified: {'a': {'b': {'c': 1}}}

  # Raise an error
  try:
    print(f"Initial: {d}")
    rm_path(d, ['a', 'x'], ignore_error=False)
  except KeyError as e:
    print(f"After:   {d} (no change due to {e.__class__.__name__}: {e})")
  # Initial: {'a': {'b': {'c': 1}}}
  # After:   {'a': {'b': {'c': 1}}} (no change due to KeyError: 'x')
