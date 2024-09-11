import os
import glob

generators = []

root = os.path.dirname(os.path.abspath(__file__))
files = glob.glob(os.path.join(root, "*.py"))
for file in files:
  if file.endswith('__init__.py'):
    continue
  file_name = os.path.basename(os.path.splitext(file)[0])
  generators.append(file_name)
  # TODO: Do this with importlib instead of exec
  #       https://stackoverflow.com/questions/67631/how-can-i-import-a-module-dynamically-given-the-full-path
  #       https://stackoverflow.com/questions/301134/how-can-i-import-a-module-dynamically-given-its-name-as-string
  try:
    exec(f'from .{file_name} import {file_name}')
  except Exception as e:
    print(f"Skipping {file_name} due to error: ", e)

# The above does no impose an order on the generators. 'hapi' and 
# 'AccessInformation' are needed by other generators. The following code
# ensures that 'hapi' and 'AccessInformation' are generated first.
# TODO: Should indicate dependency in generator functions, e.g.
#       hapi.dependencies = ['cadence', 'sample_start_stop']
#       spase_alt.dependencies = ['hapi', 'AccessInformation']
#       Then have metadata.py determine the order.
set_order = ['cadence', 'sample_start_stop', 'hapi', 'AccessInformation']
diff = list(set(generators) - set(set_order))
generators = [*set_order, *diff]

del os, glob, root, files, file, file_name, diff