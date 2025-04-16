def _set_public(file_names):
  # Each generator file has a function with the same name as the file.
  # Here we import the function and set it as a global variable.
  # This allows cdawmeta.generators.<file_name> to be used as a function.
  # This is doing the equivalent of, e.g.,
  #   from .<file_name> import <file_name>
  import importlib
  for file_name in file_names:
    module = importlib.import_module('.' + file_name, package=__name__)
    globals()[file_name] = getattr(module, file_name)

def _get_dependencies():
  import os
  import glob
  import runpy

  root = os.path.dirname(os.path.abspath(__file__))
  files = glob.glob(os.path.join(root, "*.py"))

  all_dependencies = {}
  # List of metadata types in metadata.py that are not generated.
  # If a metadata type is added to metadata.py, it should be added here.
  # TODO: This list should be generated in metadata.py.
  all_dependencies['allxml'] = None
  all_dependencies['master'] = None
  all_dependencies['cdfmetafile'] = None
  all_dependencies['orig_data'] = None
  all_dependencies['spase'] = None
  all_dependencies['spase_hpde_io'] = None

  file_names = []
  for file in files:
    if file.endswith('__init__.py'):
      continue
    file_name = os.path.basename(os.path.splitext(file)[0])

    settings = runpy.run_path(file)
    generator_deps = settings.get('dependencies', None)
    if generator_deps is not None:
      all_dependencies[file_name] = generator_deps

    file_names.append(file_name)

  return file_names, all_dependencies

def _expand_dependencies(all_dependencies):
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

  dependencies = {}
  for d in all_dependencies.keys():
    dependencies[d] = _find_deps(all_dependencies[d], all_dependencies)

  all = []
  for meta_type in dependencies.keys():
    if dependencies[meta_type] is not None:
      all = [*all, *dependencies[meta_type], meta_type]
    else:
      all = [*all, meta_type]

  # Remove duplicates; https://stackoverflow.com/a/7961390
  dependencies['all'] = list(dict.fromkeys(all))

  return dependencies

files, top_level_dependencies = _get_dependencies()

# Set the generator functions as global variables to allow importing.
_set_public(files)

# Each generator function may have a dependencies attribute that is a list of
# top-level dependencies. _expand_dependencies() creates the full list of
# dependencies for each function by adding the dependencies of the top-level
# dependencies.
dependencies = _expand_dependencies(top_level_dependencies)

del files, top_level_dependencies, _set_public, _get_dependencies, _expand_dependencies