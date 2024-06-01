import os
import json

def write_json(_dict, file_name):
  print(f'Writing {file_name}')

  file_dir = os.path.dirname(file_name)
  if not os.path.exists(file_dir):
    print(f'Creating {file_dir}')
    os.makedirs(file_dir, exist_ok=True)

  with open(file_name, 'w', encoding='utf-8') as f:
    json.dump(_dict, f, indent=2)

  print(f'Wrote {file_name}')
