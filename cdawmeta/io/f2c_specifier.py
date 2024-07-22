import re

def f2c_specifier(f_template):

  # TODO: If invalid, return {}

  f_template = f_template.lower().strip(' ')

  # e.g., 10s => s and 10a => s
  fmt = re.sub(r"([0-9].*)([a|s])", r",{:s}", f_template)

  # e.g., i4 => d
  fmt = re.sub(r"([i])([0-9].*)", r",{:d}", f_template)

  # e.g., E11.4 => %.4e, F8.1 => %.1f
  fmt = re.sub(r"([f|e])([0-9].*)\.([0-9].*)", r",{:.\3\1}", f_template)

  return fmt
