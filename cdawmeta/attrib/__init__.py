def _set_public():
  # Each generator file has a function with the same name as the file.
  # Here we import the function and set it as a global variable.
  # This allows cdawmeta.generators.<file_name> to be used as a function.
  # This is doing the equivalent of, e.g.,
  #   from .<file_name> import <file_name>

  import os
  import glob
  import importlib

  root = os.path.dirname(os.path.abspath(__file__))
  files = glob.glob(os.path.join(root, "*.py"))
  for file in files:
    file_name = os.path.basename(os.path.splitext(file)[0])
    if file_name == '__init__':
      continue

    module = importlib.import_module('.' + file_name, package=__name__)
    globals()[file_name] = getattr(module, file_name)

_set_public()
del _set_public