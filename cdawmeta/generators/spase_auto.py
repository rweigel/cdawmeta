import cdawmeta

dependencies = ['master_resolved', 'hapi', 'AccessInformation']

def spase_auto(metadatum, logger):

  additions = cdawmeta.additions(logger)

  allxml = metadatum['allxml']
  hapi = metadatum['hapi']['data']
  master = metadatum['master_resolved']['data']

  config = cdawmeta.CONFIG['spase_auto']
  logger.debug(f"Using config: {config}")

  cdawmeta_spase = config['cdawmeta-spase']

  xmlns = additions["config"]["xmlns"]
  Version = additions["config"]["version"]
  Version_ = Version.replace('.', '_')
  Note = "Nodes prefixed with a _ are not valid SPASE, but are included for "
  Note += "debugging. Values prefixed with a x_ are not valid SPASE but may "
  Note += "considered for addition for completeness."
  spase_auto_ = {
    "Spase": {
      "xmlns": xmlns,
      "xmlns:xsi": additions["config"]["xmlns:xsi"],
      "xsi:schemaLocation": f"{xmlns} {xmlns}/spase-{Version_}.xsd",
      "_Note": Note,
      "Version": Version
      }
    }

  NumericalData = {
    "ResourceID": None,
    "_ResourceID": None,
    "ResourceHeader": {}
  }

  url = cdawmeta.util.get_path(metadatum, ['master', 'url']) + " (from all.xml)"
  spase_auto_['Spase']['_MasterURL'] = url

  ResourceIDs = additions.get('ResourceID', None)
  NumericalData['ResourceID'] = ResourceIDs.get(metadatum['id'], None)
  NumericalData['_ResourceID'] = f"Source: {cdawmeta_spase}/ResourceID.json"

  p = ['CDFglobalAttributes', 'Logical_source_description']
  ResourceName = cdawmeta.util.get_path(master, p)
  if ResourceName is not None:
    NumericalData['ResourceHeader']['ResourceName'] = ResourceName
    source = f'Source: Master/{"/".join(p)}'
    NumericalData['ResourceHeader']['_ResourceName'] = source

  p = ['CDFglobalAttributes', 'Logical_source']
  AlternateName = cdawmeta.util.get_path(master, p)
  if AlternateName is not None:
    NumericalData['ResourceHeader']['AlternateName'] = AlternateName
    source = f'Source: Master/{"/".join(p)}'
    NumericalData['ResourceHeader']['_AlternateName'] = source

  p = ['CDFglobalAttributes', 'TEXT']
  Description = cdawmeta.util.get_path(master, p)
  if Description is not None:
    NumericalData['ResourceHeader']['Description'] = ''.join(Description)
    source = f"Source: Master/{'/'.join(p)}"
    NumericalData['ResourceHeader']['_Description'] = source

  p = ['CDFglobalAttributes', 'Acknowledgement']
  Acknowledgement = cdawmeta.util.get_path(master, p)
  if Acknowledgement is not None:
    NumericalData['ResourceHeader']['Acknowledgement'] = Acknowledgement
    source = f"Source: Master/{'/'.join(p)}"
    NumericalData['ResourceHeader']['_Acknowledgement'] = source

  p = ['CDFglobalAttributes', 'TITLE']
  ProviderResourceName = cdawmeta.util.get_path(master, p)
  if ProviderResourceName is not None:
    source = f"Source: {'/'.join(p)}"
    NumericalData['ProviderResourceName'] = ProviderResourceName

  p = ['CDFglobalAttributes', 'Rules_of_use']
  Caveats = cdawmeta.util.get_path(master, p)
  if Caveats is not None:
    source = f"Source: Master/{'/'.join(p)}"
    NumericalData['Caveats'] = Caveats
    NumericalData['_Caveats'] = source

  DOIs = additions.get('DOI')
  NumericalData['DOI'] = DOIs.get(metadatum['id'], None)
  NumericalData['_DOI'] = f"Source: {cdawmeta_spase}/DOI.json"

  NumericalData['ResourceHeader']['_Rights'] = additions.get('Rights')

  fromAllXML = _InformationURL_allxml(allxml)
  fromMaster = _InformationURL_master(master)
  fromHDPEIO = _InformationURL_hpdeio(metadatum['id'], additions.get('InformationURL'))
  InformationURL = _InformationURL(fromAllXML, fromMaster, fromHDPEIO, logger)
  if InformationURL is not None:
    NumericalData['ResourceHeader']['InformationURL'] = InformationURL

  if config['include_access_information']:
    NumericalData['AccessInformation'] = metadatum['AccessInformation']['data']
    NumericalData['_AccessInformation'] = f"Source: {cdawmeta_spase}/AccessInformation.json"

  Contacts = _Contact(metadatum['id'], additions.get('Contact'))
  if len(Contacts) > 0:
    NumericalData['ResourceHeader']['Contact'] = Contacts
    NumericalData['ResourceHeader']['_Contact'] = f"Source: {cdawmeta_spase}/Contact.json"

  NumericalData['TemporalDescription'] = _TemporalDescription(allxml)
  if isinstance(hapi, dict):
    # Only one DEPEND_0
    Cadence = _Cadence(hapi['info'])
    if Cadence is not None:
      NumericalData['TemporalDescription'].update(Cadence)

  NumericalData['Keyword'] = _Keyword(allxml, master)

  ObservedRegions = additions.get('ObservedRegion')
  sc = metadatum['id'].split('_')[0]
  NumericalData['ObservedRegion'] = ObservedRegions.get(sc, None)
  NumericalData['_ObservedRegion'] = f"Source: {cdawmeta_spase}/ObservedRegion.json"

  NumericalData['ProcessingLevel'] = None
  msg = "Processing level is not available in the master file; "
  msg += f"it should be there instead of, say, {cdawmeta_spase}/ProcessingLevel.json"
  NumericalData['_ProcessingLevel'] = msg

  InstrumentIDs = additions.get('InstrumentID')
  NumericalData['InstrumentID'] = InstrumentIDs.get(metadatum['id'], None)

  MeasurementType = additions.get('MeasurementType')
  NumericalData['MeasurementType'] = MeasurementType.get(metadatum['id'], None)


  if config['include_parameters']:
    if hapi is None:
      NumericalData['_Parameter'] = "No HAPI parameter info available to generate Parameter list."
    else:
      #NumericalData['Parameter'] = _Parameter(hapi, additions)
      NumericalData['Parameter2'] = _Parameter2(metadatum['id'], master, additions, logger)
      if False:
        comparison = _compare_parameters(
          NumericalData['Parameter'], NumericalData['Parameter2'], logger
        )
        if comparison['diffs'] != {}:
          logger.warning(f"Parameter and Parameter2 differ for {metadatum['id']}.")
          logger.warning(comparison['diffs'])
          #raise Exception(f"Parameter and Parameter2 differ: {comparison['diffs']}")

  spase_auto_['Spase']['NumericalData'] = NumericalData

  if config.get('strip_underscore', False):
    # Remove keys starting with _ from output. These keys are comments used for
    # helping understand the source of the data and for debugging.
    # They are not valid SPASE.
    spase_auto_ = _strip_underscore(spase_auto_)

  return [spase_auto_]


def _strip_underscore(d):
  if isinstance(d, dict):
    return {k: _strip_underscore(v) for k, v in d.items() if not k.startswith('_')}
  elif isinstance(d, list):
    return [_strip_underscore(i) for i in d]
  else:
    return d


def _Contact(dsid, fromRepo):

  import re
  contacts = []

  for idx, element in enumerate(fromRepo):
    _cdaweb_ids = element.get('_cdaweb_ids', None)
    keep = False
    if _cdaweb_ids is not None:
      for _cdaweb_id in _cdaweb_ids:
        if _cdaweb_id.startswith('^'):
          regex = re.compile(_cdaweb_id)
          if regex.match(dsid):
            keep = True
        if _cdaweb_id == dsid:
          keep = True
      if keep:
        contacts.extend(element['Contacts'])

  return contacts


def _InformationURL(fromAllXML, fromMaster, fromHPDEIO, logger):

  # Collect all URLs and which sources contain them
  source_map = {}  # url -> {'all.xml': entry, 'master': entry, 'InformationURL.json': entry}
  for entry in fromAllXML:
    url = entry.get('URL', '').strip()
    if url:
      source_map.setdefault(url, {})['all.xml'] = entry
  for entry in fromMaster:
    url = entry.get('URL', '').strip()
    if url:
      source_map.setdefault(url, {})['master'] = entry
  for entry in fromHPDEIO:
    url = entry.get('URL', '').strip()
    if url:
      source_map.setdefault(url, {})['InformationURL.json'] = entry

  if not source_map:
    return None

  urls_allxml = {u for u, s in source_map.items() if 'all.xml' in s}
  urls_master = {u for u, s in source_map.items() if 'master' in s}
  urls_hpdeio = {u for u, s in source_map.items() if 'InformationURL.json' in s}
  common_all           = urls_allxml & urls_master & urls_hpdeio
  common_allxml_master = (urls_allxml & urls_master) - urls_hpdeio
  common_allxml_hpdeio = (urls_allxml & urls_hpdeio) - urls_master
  common_master_hpdeio = (urls_master & urls_hpdeio) - urls_allxml
  unique_allxml = urls_allxml - urls_master - urls_hpdeio
  unique_master = urls_master - urls_allxml - urls_hpdeio
  unique_hpdeio = urls_hpdeio - urls_allxml - urls_master

  logger.info(f"  all.xml URLs         : {sorted(urls_allxml) or 'none'}")
  logger.info(f"  master URLs          : {sorted(urls_master) or 'none'}")
  logger.info(f"  InformationURL.json  : {sorted(urls_hpdeio) or 'none'}")

  def _field_compare(sources, field):
    """Return a dict of {source: value} for sources that have the field, or None if all agree."""
    vals = {src: sources[src].get(field, '').strip() for src in sources if sources[src].get(field, '').strip()}
    if len(set(vals.values())) <= 1:
      return None  # all present values are identical (or none present)
    return vals

  if common_all:
    logger.info(f"  Common to all three          : {sorted(common_all)}")
    for url in sorted(common_all):
      sources = source_map[url]
      for field in ['Name', 'Description']:
        diff = _field_compare(sources, field)
        if diff:
          logger.info(f"    {url} {field} differs:")
          for src, val in diff.items():
            logger.info(f"      {src}: {val!r}")

  if common_allxml_master:
    logger.info(f"  Common to all.xml + master   : {sorted(common_allxml_master)}")
    for url in sorted(common_allxml_master):
      sources = source_map[url]
      for field in ['Name', 'Description']:
        diff = _field_compare(sources, field)
        if diff:
          logger.info(f"    {url} {field} differs:")
          for src, val in diff.items():
            logger.info(f"      {src}: {val!r}")

  if common_allxml_hpdeio:
    logger.info(f"  Common to all.xml + hpdeio   : {sorted(common_allxml_hpdeio)}")
    for url in sorted(common_allxml_hpdeio):
      sources = source_map[url]
      for field in ['Name', 'Description']:
        diff = _field_compare(sources, field)
        if diff:
          logger.info(f"    {url} {field} differs:")
          for src, val in diff.items():
            logger.info(f"      {src}: {val!r}")

  if common_master_hpdeio:
    logger.info(f"  Common to master + hpdeio    : {sorted(common_master_hpdeio)}")
    for url in sorted(common_master_hpdeio):
      sources = source_map[url]
      for field in ['Name', 'Description']:
        diff = _field_compare(sources, field)
        if diff:
          logger.info(f"    {url} {field} differs:")
          for src, val in diff.items():
            logger.info(f"      {src}: {val!r}")

  if unique_allxml:
    logger.info(f"  Unique to all.xml            : {sorted(unique_allxml)}")
  if unique_master:
    logger.info(f"  Unique to master             : {sorted(unique_master)}")
  if unique_hpdeio:
    logger.info(f"  Unique to InformationURL.json: {sorted(unique_hpdeio)}")

  # Merge with priority: InformationURL.json > master > all.xml
  merged = []
  for url, sources in source_map.items():
    for winning in ['InformationURL.json', 'master', 'all.xml']:
      if winning in sources:
        entry = {k: v for k, v in sources[winning].items() if k != '_source'}
        break
    source_labels = [lbl for lbl in ['all.xml', 'master', 'InformationURL.json'] if lbl in sources]
    if len(source_labels) > 1:
      others = [lbl for lbl in source_labels if lbl != winning]
      entry['_Note'] = f"URL also in {', '.join(others)}. Using {winning}."
    else:
      entry['_Note'] = f"Source: {winning}"
    merged.append(entry)

  return merged if merged else None


def _InformationURL_hpdeio(dsid, fromRepo):
  import re

  if not fromRepo:
    return []
  result = []
  for key, value in fromRepo.items():
    matched = False
    for _cdaweb_id in value.get('_cdaweb_ids', []):
      if _cdaweb_id.startswith('^'):
        if re.compile(_cdaweb_id).match(dsid):
          matched = True
      elif _cdaweb_id == dsid:
        matched = True
    if matched:
      entry = dict(value.get('InformationURL', {}))
      entry['_source'] = 'InformationURL.json'
      result.append(entry)
  return result


def _InformationURL_master(master):
  HTTP_LINK = cdawmeta.util.get_path(master, ['CDFglobalAttributes', 'HTTP_LINK'])
  if HTTP_LINK is None:
    return []
  LINK_TEXT  = cdawmeta.util.get_path(master, ['CDFglobalAttributes', 'LINK_TEXT'])
  LINK_TITLE = cdawmeta.util.get_path(master, ['CDFglobalAttributes', 'LINK_TITLE'])

  if isinstance(HTTP_LINK, str):
    HTTP_LINK = [HTTP_LINK]
  if isinstance(LINK_TEXT, str):
    LINK_TEXT = [LINK_TEXT]
  if isinstance(LINK_TITLE, str):
    LINK_TITLE = [LINK_TITLE]

  result = []
  for i, url in enumerate(HTTP_LINK):
    if not url or not url.strip():
      continue
    entry = {"URL": url.strip(), "_source": "master"}
    if LINK_TITLE is not None and i < len(LINK_TITLE):
      entry["Name"] = LINK_TITLE[i]
    if LINK_TEXT is not None and i < len(LINK_TEXT):
      entry["Description"] = LINK_TEXT[i]
    result.append(entry)

  return result


def _InformationURL_allxml(allxml):
  links = cdawmeta.util.get_path(allxml, ['other_info', 'link'])
  if links is None:
    return []
  if isinstance(links, dict):
    links = [links]

  result = []
  for link in links:
    if '@URL' not in link:
      continue
    entry = {"URL": link['@URL'], "_source": "all.xml"}
    if '@title' in link:
      entry['Name'] = link['@title']
    if '#text' in link:
      entry['Description'] = link['#text']
    result.append(entry)

  return result


def _TemporalDescription(allxml):

  _TemporalDescriptionNote = "Generated from all.xml/@timerange_start and all.xml/@timerange_stop"
  _TemporalDescription = {
    'TimeSpan': {},
    '_TemporalDescription': _TemporalDescriptionNote
  }

  StartDate = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  _TemporalDescription['TimeSpan']['StartDate'] = StartDate
  StopDate = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  _TemporalDescription['TimeSpan']['StopDate'] = StopDate

  return _TemporalDescription


def _Cadence(hapi_info):

  if hapi_info.get('cadence', None) is None:
    return None

  Cadence = {
    'Cadence': hapi_info['cadence'],
    '_Cadence': hapi_info['x_cadence_note']
  }

  return Cadence


def _Keyword(allxml, master):

  _Keyword = []
  for key in ['observatory', 'instrument']:
    p = [key, 'description', '@short']
    keyword = cdawmeta.util.get_path(allxml, p)
    if keyword.strip() != '':
      _Keyword.append(f'{keyword.strip()} (from all.xml/{"/".join(p)})')

  for key in ['Discipline', 'Source_name', 'Data_type']:
    values = cdawmeta.util.get_path(master, ['CDFglobalAttributes', key])
    if values is not None:
      if isinstance(values, str):
        values = [values]
      for value in values:
        keyword_split = value.split('>')
        for keyword in keyword_split:
          keyword_split2 = keyword.split('\n')
          for keyword2 in keyword_split2:
            if keyword2.strip() == '':
              continue
            val = keyword2.strip() + f" (from Master/CDFglobalAttributes/{key})"
            _Keyword = [*_Keyword, val]
        _Keyword = list(dict.fromkeys(_Keyword))

  InstrumentID = cdawmeta.util.get_path(allxml, ['instrument', '@ID'])
  if InstrumentID is not None:
    _Keyword.append(InstrumentID + " (from all.xml/instrument/@ID)")
  return _Keyword


def _compare_parameters(params1, params2, logger):
  """Compare Parameter (hapi) and Parameter2 (master) lists by ParameterKey.

  Returns a dict summarising differences, common keys, and keys unique to each.
  Also logs a summary.
  """
  if not params1 or not params2:
    return None

  map1 = {p['ParameterKey']: p for p in params1 if 'ParameterKey' in p}
  map2 = {p['ParameterKey']: p for p in params2 if 'ParameterKey' in p}

  keys1 = set(map1)
  keys2 = set(map2)
  common   = keys1 & keys2
  only_hapi   = keys1 - keys2
  only_master = keys2 - keys1

  compare_fields = ['Name', 'Description', 'Units', 'FillValue', 'Structure']
  diffs = {}
  for key in sorted(common):
    p1, p2 = map1[key], map2[key]
    field_diffs = {}
    for field in compare_fields:
      v1 = p1.get(field)
      v2 = p2.get(field)
      if v1 != v2:
        field_diffs[field] = {'hapi': v1, 'master': v2}
    if field_diffs:
      diffs[key] = field_diffs

  logger.info(f"Parameter comparison: {len(common)} common, "
              f"{len(only_hapi)} only in hapi, {len(only_master)} only in master")
  if only_hapi:
    logger.info(f"  Only in hapi  : {sorted(only_hapi)}")
  if only_master:
    logger.info(f"  Only in master: {sorted(only_master)}")
  if diffs:
    logger.info(f"  Differences in {len(diffs)} common parameter(s):")
    for key, field_diffs in diffs.items():
      for field, vals in field_diffs.items():
        logger.info(f"    {key}/{field}: hapi={vals['hapi']!r}  master={vals['master']!r}")

  return {
    'common': sorted(common),
    'only_hapi': sorted(only_hapi),
    'only_master': sorted(only_master),
    'diffs': diffs
  }


def _Parameter2(id, master, additions, logger):

  master_split = cdawmeta.split_variables(id, master['CDFVariables'], logger=None, meta_type='master', omit_variable=None)
  all_parameters = []
  n_DEPEND_0 = len(master_split)
  logger.info(f"    Computing parameters for {n_DEPEND_0} {n_DEPEND_0} DEPEND_0 variable(s)")
  for depend_0_name, variables in master_split.items():
    depend_0_var = master['CDFVariables'].get(depend_0_name, {})
    depend_0_attrs = depend_0_var.get('VarAttributes', {})
    depend_0_desc = depend_0_var.get('VarDescription', {})

    logger.info(f"    {depend_0_name}")

    DataType = depend_0_desc.get('DataType', '')
    Name = depend_0_attrs.get('FIELDNAM', depend_0_name)
    ParameterKey = depend_0_name

    logger.info(f"      DataType: {DataType}")
    logger.info(f"      Name: {Name}")
    logger.info(f"      ParameterKey: {ParameterKey}")


    catdesc = depend_0_attrs.get('CATDESC', None)
    Description = f"The time index for this variable is {depend_0_name}."
    if catdesc:
      Description = f"CATDESC: '{catdesc}'. Notes not in Master CDF: '{Description}'"

    Units = depend_0_attrs.get('UNITS')
    logger.info(f"      Units: {Units}")

    Description += " The units are the units in CDF files. For other web services, this variable is may be represented as a time string."
    logger.info(f"      Description: {Description}")

    # Not sure about Support.
    Support = {'Qualifier': 'Scalar', 'SupportQuantity': 'Temporal'}
    logger.info(f"      Support: {Support}")

    # Epoch/time parameter
    epoch_param = {
      'Name': Name,
      'ParameterKey': depend_0_name,
      'Description': Description,
      'Units': Units,
      'Support': Support
    }

    all_parameters.append(epoch_param)

    for var_name, var_meta in variables.items():
      attrs = var_meta.get('VarAttributes', {})
      desc = var_meta.get('VarDescription', {})

      if attrs.get('VAR_TYPE', '') not in ('data', 'support_data', ''):
        continue

      param = {
        'Name': attrs.get('FIELDNAM', var_name),
        'ParameterKey': var_name,
      }

      catdesc = attrs.get('CATDESC', None)
      if catdesc:
        param['Description'] = catdesc

      units = attrs.get('x_UNITS') or attrs.get('UNITS')
      if units:
        param['Units'] = units

      fillval = attrs.get('FILLVAL', None)
      if fillval is not None:
        param['FillValue'] = fillval

      dim_sizes = desc.get('DimSizes', None)
      if dim_sizes:
        param['Structure'] = {'Size': dim_sizes}

      all_parameters.append(param)

  return all_parameters if all_parameters else None


def _Parameter(hapi, additions, include_cadence=False):

  if isinstance(hapi, list):
    # More than one DEPEND_0
    Parameter = []
    for dataset in hapi:
      Parameter.append(_Parameter(dataset, additions, include_cadence=True))

    # https://stackoverflow.com/a/45323085
    # sum() is used to flatten a list of lists
    return sum(Parameter, [])

  Parameters = []
  parameters = hapi['info']['parameters']
  Cadence = None
  if include_cadence:
    Cadence = _Cadence(hapi['info'])

  for parameter in parameters:

    if parameter['type'] == 'isotime':
      Unit = "ms"
      if 'x_description' in parameter:
        Description = parameter['x_description']
      if 'description' in parameter:
        Description = parameter['description']
      DataType = parameter['x_cdf_DataType']
      Unit = additions["Epoch"][DataType]["Unit"]
      Parameter = {
        "Name": parameter['name'],
        "ParameterKey": parameter['x_cdf_NAME'],
        "Description": Description,
        "_Description": additions["Epoch"][DataType]["Description"],
        "Units": Unit,
        "_Note": additions["Epoch"]["Note"]
      }
      Parameters.append(Parameter)
      continue

    Parameter = {
      "Name": parameter['name'],
      "ParameterKey": parameter['name']
    }

    if 'x_cdf_FIELDNAM' in parameter:
      Parameter['Name'] = parameter['x_cdf_FIELDNAM']
    if 'description' in parameter:
      Parameter['Description'] = parameter['description']
    if 'units' in parameter:
      Parameter['Units'] = parameter['units']
    if 'x_units_original' in parameter:
      Parameter['_UnitsSchema'] = parameter['x_unitsSchema']
      Parameter['_UnitsOriginal'] = parameter['x_units_original']
    if Cadence is not None:
      Parameter.update(Cadence)
    if 'fill' in parameter and parameter['fill'] is not None:
      Parameter['FillValue'] = parameter['fill']
    if 'size' in parameter:
      Parameter['Structure'] = {'Size': parameter['size']}

    Parameters.append(Parameter)

  return Parameters


if __name__ == '__main__':
  if False:
    import pdb; pdb.set_trace()
    from cdawmeta.io import read_cdf_meta
    file = 'https://cdaweb.gsfc.nasa.gov/sp_phys/data/ace/orbit/'
    file += 'level_2_cdaweb/or_ssc/ac_or_ssc_19970101_v01.cdf'
    meta_file = read_cdf_meta(file)
    print(meta_file)

