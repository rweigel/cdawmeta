def table(id=None,
          id_skip=None,
          update=False,
          update_skip='',
          regen=False,
          regen_skip='',
          max_workers=3,
          table_name=None,
          embed_data=False,
          log_level='info'):

  # Capture kwargs for passing to metadata()
  kwargs = locals()
  # Remove args not passed to metadata()
  del kwargs['table_name']
  del kwargs['embed_data']

  import os
  import utilrsw
  import tableui
  import cdawmeta

  configs = cdawmeta.CONFIG['table']['tables']
  table_names = list(configs.keys())

  if table_name is None:
    # Create all tables
    info = {}
    for table_name in table_names:
      kwargs['table_name'] = table_name
      info[table_name] = table(**kwargs)
    return info

  logger = cdawmeta.logger('table')
  logger.setLevel(log_level.upper())

  if table_name is not None:
    if table_name not in table_names:
      emsg = f"name='{table_name}' not in {table_names} in config.json"
      logger.error(emsg)
      raise ValueError(emsg)

  # Set metadata needed for the requested table.
  if table_name.startswith('spase'):
    meta_type = ['spase']
  elif table_name.startswith('cdaweb'):
    meta_type = ['allxml', 'master']
  elif table_name.startswith('hapi'):
    meta_type = ['hapi']

  datasets = cdawmeta.metadata(**kwargs, meta_type=meta_type)

  if table_name.startswith('cdaweb'):
    for dsid in datasets.keys():
      if table_name == 'cdaweb.dataset':
        dataset = datasets[dsid].get("allxml", None)
        if dataset is not None:
          # {"a": {"b": "c"}, ...} -> {"a/b": "c"}
          datasets[dsid]["allxml"] = utilrsw.flatten_dicts(dataset)
          datasets[dsid]["allxml"]['datasetID'] = dsid

      if table_name == 'cdaweb.variable':
        variables_flat = []
        path = ['master','data', 'CDFVariables']
        variables = utilrsw.get_path(datasets[dsid], path)
        if variables is not None:
          for vid in variables:
            if variables[vid] is None:
              continue
            # Flatten CDFVariables dict
            variables[vid] = utilrsw.flatten_dicts(variables[vid], simplify=True)
            # Put VariableName and datasetID at the top of the dict
            variables[vid] = {
              'datasetID': dsid,
              'VariableName': vid,
              **variables[vid]
            }
            variables_flat.append(variables[vid])
        utilrsw.set_path(datasets[dsid], variables_flat, path)

  if table_name.startswith('spase'):
    for dsid in datasets.keys():
      path = ['spase', 'data', 'Spase', 'NumericalData', 'Parameter']
      parameters = utilrsw.get_path(datasets[dsid], path)
      if parameters is None:
        continue
      if isinstance(parameters, dict):
        # If only one parameter, make it a list of one parameter.
        parameters = [parameters]
      for i in range(0, len(parameters)):
        parameters[i]['datasetID'] = dsid
        parameters[i] = utilrsw.flatten_dicts(parameters[i])
      utilrsw.set_path(datasets[dsid], parameters, path)

  if table_name.startswith('hapi'):
    datasets_expanded = {}
    for dsid in datasets.keys():
      sub_datasets = datasets[dsid]['hapi']['data']

      if sub_datasets is None:
        logger.warning(f"No hapi datasets for {dsid}. Skipping.")
        continue

      if isinstance(sub_datasets, dict):
        # If only one sub-dataset, make it a list of one sub-dataset.
        sub_datasets = [sub_datasets]

      for sub_dataset in sub_datasets:
        sdsid = sub_dataset['id']
        sub_dataset = cdawmeta.restructure.hapi(sub_dataset, simplify_bins=True)
        sub_dataset['id'] = sdsid # Add id back in
        datasets_expanded[sdsid] = {
          'id': sdsid,
          'hapi': {
            'data': sub_dataset
            }
        }
        # Convert parameters object to an array.
        path = ['hapi', 'data', 'parameters']
        parameters = utilrsw.get_path(datasets_expanded[sdsid], path)
        parameters_array = []
        if parameters:
          for key in parameters:
            parameters[key]['id'] = dsid
            parameters_array.append(parameters[key])
          utilrsw.set_path(datasets_expanded[sdsid], parameters_array, path)

    datasets = datasets_expanded

  config = configs[table_name]

  out_dir = os.path.join(cdawmeta.DATA_DIR, 'table')
  if id is not None:
    wmsg = "Using id to create subdirectory for table files. "
    wmsg += "If id is a regex, expect trouble."
    logger.warning(wmsg)
    out_dir = os.path.join(out_dir, 'partial', id)

  logger.info(40*"-")
  logger.info(f"Creating table '{table_name}'")
  info = tableui.dict2sql(datasets,
                          config,
                          table_name,
                          embed=embed_data,
                          out_dir=out_dir,
                          logger=logger)

  return info
