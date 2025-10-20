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
    datasets_list = []

    for dsid in datasets.keys():
      if table_name == 'cdaweb.dataset':
        allxml = datasets[dsid].get("allxml", None)
        if allxml is not None:
          # {"a": {"b": "c"}, ...} -> {"a/b": "c"}
          allxml = utilrsw.flatten_dicts(allxml)
          allxml['datasetID'] = dsid
        datasets_list.append({"allxml": allxml, 'master': datasets[dsid]['master']})

      if table_name == 'cdaweb.variable':
        path = ['master','data', 'CDFVariables']
        variables = utilrsw.get_path(datasets[dsid], path)
        if variables is not None:
          for vid in variables:
            if variables[vid] is None:
              continue
            variables[vid] = {
              "Added": {
                'datasetID': dsid,
                'VariableName': vid
              },
              **variables[vid]
            }
            datasets_list.append(variables[vid])

    datasets = datasets_list

  if table_name.startswith('spase'):
    datasets_list = []
    for dsid in datasets.keys():

      if table_name == 'spase.dataset':
        path = ['spase', 'data']
        dataset = utilrsw.get_path(datasets[dsid], ['spase', 'data'])
        if not dataset:
          continue
        dataset['Added'] = {'datasetID': dsid}
        datasets_list.append(dataset)

      if table_name == 'spase.parameter':
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
          datasets_list.append(parameters[i])

    datasets = datasets_list

  if table_name.startswith('hapi'):
    datasets_list = []
    for dsid in datasets.keys():
      sub_datasets = datasets[dsid]['hapi']['data']

      if sub_datasets is None:
        logger.warning(f"No hapi datasets for {dsid}. Skipping.")
        continue

      if isinstance(sub_datasets, dict):
        # If only one sub-dataset, make it a list of one sub-dataset.
        sub_datasets = [sub_datasets]

      for sub_dataset in sub_datasets:
        sub_dataset = utilrsw.flatten_dicts(sub_dataset, simplify=True)

        if table_name == 'hapi.dataset':
          datasets_list.append(sub_dataset)

        if table_name == 'hapi.parameter':
          if 'parameters' in sub_dataset:
            for parameter in sub_dataset['parameters']:
              parameter['id'] = dsid
              datasets_list.append(parameter)

    datasets = datasets_list

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
                          out_dir=out_dir,
                          embed=embed_data,
                          logger=logger)

  return info
