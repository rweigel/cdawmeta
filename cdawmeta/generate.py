import os
import cdawmeta

def generate(metadatum, gen_name, logger,
             update=True, regen=False, diffs=False,
             exit_on_exception=False):

  sub_dir = 'info'

  id = metadatum['id']
  base_path = os.path.join(cdawmeta.DATA_DIR, gen_name, sub_dir)
  file_name_pkl = os.path.join(base_path, f'{id}.pkl')
  file_name_json = os.path.join(base_path, f'{id}.json')
  file_name_error = os.path.join(base_path, f'{id}.error.txt')

  if not update and not regen:
    if os.path.exists(file_name_pkl):
      msg = "Using cache because update = regen = False and found cached file."
      logger.info(msg)
      data = cdawmeta.util.read(file_name_pkl, logger=logger)
      if isinstance(data, list) and len(data) == 1:
        data = data[0]
      return {'id': id, 'log': msg, 'data-file': file_name_json, 'data': data}

    if os.path.exists(file_name_error):
      msg = "Using cached error response because update = regen = False."
      logger.info(msg)
      emsg = cdawmeta.util.read(file_name_error, logger=logger)
      return {'id': id,'log': msg, 'error': emsg, 'data-file': None, 'data': None}

  try:
    gen_func = getattr(cdawmeta.generators, gen_name)
    datasets = gen_func(metadatum, logger)
    if isinstance(datasets, dict):
      logger.info(f"Writing {file_name_error}")
      cdawmeta.util.write(file_name_error, datasets['error'])
      return {'id': id, 'log': None, 'error': datasets['error'], 'data-file': None, 'data': None}
  except Exception as e:
    logger.info(f"Writing {file_name_error}")
    cdawmeta.util.write(file_name_error, datasets['error'])
    cdawmeta.exception(id, logger, exit_on_exception=exit_on_exception)
    return {'id': id, 'log': None, 'error': emsg, 'data-file': None, 'data': None}

  if os.path.exists(file_name_error):
    logger.info(f"Removing {file_name_error}")
    os.remove(file_name_error)

  single = False
  if len(datasets) == 1:
    single = True
    datasets = datasets[0]

  # Write pkl file with all datasets associated with a CDAWeb dataset.
  # "data" will be an array of dicts if datasets is an array of dicts.
  logger.info(f"Writing {file_name_pkl}")
  cdawmeta.util.write(file_name_pkl, datasets)
  # JSON file not used internally, but useful for visual debugging
  logger.info(f"Writing {file_name_json}")
  cdawmeta.util.write(file_name_json, datasets)

  if gen_name == 'spase_auto':
    # spase_auto always returns one dataset
    data_xml = _to_spase_xml(datasets)
    file_name_xml = file_name_pkl.replace('.pkl', '.xml')
    logger.info(f"Writing {file_name_xml}")
    cdawmeta.util.write(file_name_xml, data_xml)

  if single:
    return {"id": id, "data-file": file_name_json, "data": datasets}

  data = []
  data_files = []

  for dataset in datasets:
    data.append(dataset)
    sid = dataset['id'] # Sub dataset id

    file_name_pkl = os.path.join(base_path, f"{sid}.pkl")
    file_name_json = os.path.join(base_path, f"{sid}.json")
    cdawmeta.util.write(file_name_pkl, dataset, logger=logger)
    cdawmeta.util.write(file_name_json, dataset, logger=logger)

    data_files.append(file_name_json)

  return {"id": id, "data-file": data_files, "data": data}

def _to_spase_xml(data):

  import dicttoxml
  from xml.dom.minidom import parseString

  debug = False

  def _node_depth(node):
    depth = 0
    current_node = node
    while current_node is not None:
      depth += 1
      current_node = current_node.parentNode
    return depth

  def flatten_item_nodes(dom):
    items = dom.getElementsByTagName('item')
    unique_parents = []
    depths = []
    for item in items:
      if item.parentNode not in unique_parents:
        if debug:
          print(f"Parent node: {item.parentNode.nodeName}")
          print(f"depth: {_node_depth(item)}")
        depths.append(_node_depth(item))
        unique_parents.append(item.parentNode)

    # Create a list of tuples (parent, depth)
    parent_depths = list(zip(unique_parents, depths))
    if debug:
      print(parent_depths)
    parent_depths.sort(key=lambda x: x[1], reverse=True)
    if debug:
      print("")
      print(parent_depths)

    # Extract the sorted unique parents
    unique_parents = [parent for parent, _ in parent_depths]

    if debug:
      print("Unique parent nodes for items:", unique_parents)

    for unique_parent in unique_parents:
      if debug:
        print(f"Parent name: {unique_parent.nodeName}")
      items = unique_parent.getElementsByTagName('item')
      items = [item for item in items if item.parentNode == unique_parent]
      for item in items:
        if debug:
          print(item.toxml())
        new_node = dom.createElement(unique_parent.nodeName)
        for child in item.childNodes:
          new_node.appendChild(child.cloneNode(True))
        unique_parent.parentNode.appendChild(new_node)
      unique_parent.parentNode.removeChild(unique_parent)

    return dom

  Spase = f'<Spase xmlns="{data["Spase"]["xmlns"]}" xmlns:xsi="{data["Spase"]["xmlns:xsi"]}" xsi:schemaLocation="{data["Spase"]["xsi:schemaLocation"]}">'
  del data['Spase']["xmlns"]
  del data['Spase']["xmlns:xsi"]
  del data['Spase']["xsi:schemaLocation"]

  data_xml = dicttoxml.dicttoxml(data, attr_type=False, root=False, encoding='UTF-8')
  dom = parseString(data_xml)

  dom = parseString(data_xml)
  dom = flatten_item_nodes(dom)
  data_xml = dom.toprettyxml(indent='  ')

  # TODO: Find robust way to do this
  data_xml = data_xml.replace('<?xml version="1.0" ?>', '<?xml version="1.0" encoding="UTF-8" standalone="yes" ?>')
  data_xml = data_xml.replace('<Spase>', Spase)

  return data_xml
