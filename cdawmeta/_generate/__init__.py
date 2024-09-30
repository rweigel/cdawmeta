import os
import glob

# TODO: Rewrite.

def _find_deps(deps, all_deps):
  if deps is None:
    return None

  full = []
  for dep in deps:
    if all_deps[dep] is not None:
      x = _find_deps(all_deps[dep], all_deps)
      full = [*full, *x, dep]
    else:
      full.append(dep)

  # https://stackoverflow.com/a/7961390
  full = list(dict.fromkeys(full))

  return full

root = os.path.dirname(os.path.abspath(__file__))
files = glob.glob(os.path.join(root, "*.py"))

generators = []
all_dependencies = {}
for file in files:
  if file.endswith('__init__.py'):
    continue
  file_name = os.path.basename(os.path.splitext(file)[0])

  # TODO: Do this with importlib instead of exec
  #       https://stackoverflow.com/questions/67631/how-can-i-import-a-module-dynamically-given-the-full-path
  #       https://stackoverflow.com/questions/301134/how-can-i-import-a-module-dynamically-given-its-name-as-string
  try:
    exec(f'from .{file_name} import {file_name}')
    generators.append(file_name)
  except Exception as e:
    print(f"Skipping {file_name} due to error: ", e)

  dependencies = None
  try:
    #print(f"Reading {file_name} dependencies")
    exec(f'from .{file_name} import dependencies')
    #import pdb; pdb.set_trace()
    #print("Dependencies: ", dependencies)
  except:
    pass
    #print("No dependencies for ", file_name)

  all_dependencies[file_name] = dependencies

set_order = ['cadence', 'sample_start_stop', 'hapi', 'AccessInformation']
diff = list(set(generators) - set(set_order))

generators = [*set_order, *diff]

all_dependencies['allxml'] = None
all_dependencies['master'] = None
all_dependencies['orig_data'] = None
all_dependencies['spase'] = None
all_dependencies['spase_hpde_io'] = None

dependencies = {}
for d in all_dependencies.keys():
  dependencies[d] = _find_deps(all_dependencies[d], all_dependencies)

all = []
for meta_type in dependencies.keys():
  if dependencies[meta_type] is not None:
    all = [*all, *dependencies[meta_type], meta_type]
  else:
    all = [*all, meta_type]

# https://stackoverflow.com/a/7961390
dependencies['all'] = list(dict.fromkeys(all))

del os, glob, root, files, file, file_name, diff, _find_deps