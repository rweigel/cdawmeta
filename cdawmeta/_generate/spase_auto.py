import cdawmeta

dependencies = ['master', 'hapi', 'AccessInformation']

def spase_auto(metadatum, logger):

  additions = cdawmeta.additions(logger)

  allxml = metadatum['allxml']
  master = cdawmeta.restructure.master(metadatum['master']['data'], logger=logger)
  hapi = metadatum['hapi']['data']

  config = cdawmeta.CONFIG['spase_auto']
  logger.debug(f"Using config: {config}")

  xmlns = additions["config"]["xmlns"]
  Version = additions["config"]["version"]
  Version_ = Version.replace('.', '_')
  spase_auto_ = {
    "Spase": {
      "xmlns": xmlns,
      "xmlns:xsi": additions["config"]["xmlns:xsi"],
      "xsi:schemaLocation": f"{xmlns} {xmlns}/spase-{Version_}.xsd",
      "_Note": "Nodes prefixed with a _ are not valid SPASE, but are inluded for debugging.",
      "Version": Version
      }
    }

  NumericalData = {
    "ResourceID": None,
    "ResourceHeader": {}
  }

  spase_auto_['Spase']['_MasterURL'] = cdawmeta.util.get_path(metadatum, ['master', 'url'])

  # TODO: Compute ResourceID based on CDAWeb ID and cadence.
  ResourceIDs = additions.get('ResourceID', None)
  NumericalData['ResourceID'] = ResourceIDs.get(metadatum['id'], None)
  DOIs = additions.get('DOI')
  NumericalData['DOI'] = DOIs.get(metadatum['id'], None)

  p = ['CDFglobalAttributes', 'Logical_source_description']
  ResourceName = cdawmeta.util.get_path(master, p)
  if ResourceName is not None:
    NumericalData['ResourceHeader']['ResourceName'] = ResourceName
    source = f'Source: {"/".join(p)}'
    NumericalData['ResourceHeader']['_ResourceName'] = source

  p = ['CDFglobalAttributes', 'TEXT']
  Description = cdawmeta.util.get_path(master, p)
  if Description is not None:
    NumericalData['ResourceHeader']['Description'] = Description
    source = f"Source: {'/'.join(p)}"
    NumericalData['ResourceHeader']['_Description'] = source

  p = ['CDFglobalAttributes', 'Acknowledgement']
  Acknowledgement = cdawmeta.util.get_path(master, p)
  if Acknowledgement is not None:
    NumericalData['ResourceHeader']['Acknowledgement'] = Acknowledgement
    source = f"Source: {'/'.join(p)}"
    NumericalData['ResourceHeader']['_Acknowledgement'] = source

  InformationURL = _InformationURL(allxml)
  # TODO: Add content in cdawmeta-additions/InformationURL.json if unique
  if InformationURL is not None:
    NumericalData['ResourceHeader']['InformationURL'] = InformationURL

  if config['include_access_information']:
    NumericalData['AccessInformation'] = metadatum['AccessInformation']['data']
    NumericalData['_AccessInformation'] = "Source: AccessInformation.json template"

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

  NumericalData['ProcessingLevel'] = None

  p = ['CDFglobalAttributes', 'TITLE']
  ProviderResourceName = cdawmeta.util.get_path(master, p)
  if ProviderResourceName is not None:
    source = f"Source: {'/'.join(p)}"
    NumericalData['ProviderResourceName'] = ProviderResourceName

  InstrumentIDs = additions.get('InstrumentID')
  NumericalData['InstrumentID'] = InstrumentIDs.get(metadatum['id'], None)

  InstrumentIDs = additions.get('MeasurementType')
  NumericalData['MeasurementType'] = InstrumentIDs.get(metadatum['id'], None)

  p = ['CDFglobalAttributes', 'Rules_of_use']
  Caveats = cdawmeta.util.get_path(master, p)
  if Caveats is not None:
    source = "Source: {'/'.join(p)}"
    NumericalData['Caveats'] = Caveats

  if config['include_parameters']:
    NumericalData['Parameter'] = _Parameter(hapi, additions)

  spase_auto_['Spase']['NumericalData'] = NumericalData

  return [spase_auto_]

def _InformationURL(allxml):

  links = cdawmeta.util.get_path(allxml, ['other_info', 'link'])
  _InformationURL = None
  if links is not None:

    InformationURLs = []
    if isinstance(links, dict):
      links = [links]

    for link in links:
      if '@URL' not in link:
        continue

      InformationURL = {"URL": link['@URL']}

      if '@title' in link:
        InformationURL['Name'] = link['@title']

      if '#text' in link:
        InformationURL['Description'] = link['#text']

      InformationURL['_Note'] = "Source: from all.xml/other_info/link"

      InformationURLs.append(InformationURL)

    if len(InformationURLs) > 0:
      _InformationURL = InformationURLs

  return _InformationURL

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
    _Keyword.append(f'{keyword} (from all.xml/{"/".join(p)})')

  for key in ['Discipline', 'Source_name', 'Data_type']:
    val = cdawmeta.util.get_path(master, ['CDFglobalAttributes', key])
    if val is not None:
      keyword_split = val.replace(">", "?").split('?')
      for keyword in keyword_split:
        val = keyword.strip() + " (from Master/CDFglobalAttributes/" + key + ")"
        _Keyword = [*_Keyword, val]
      _Keyword = list(dict.fromkeys(_Keyword))

  InstrumentID = cdawmeta.util.get_path(allxml, ['instrument', '@ID'])
  if InstrumentID is not None:
    _Keyword.append(InstrumentID)

  return _Keyword

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

def _VersionRelease():

  return {
    "2.6.1": "2024-06-20",
    "2.6.0": "2023-08-03",
    "2.5.0": "2022-09-29",
    "2.4.2": "2022-07-21",
    "2.4.1": "2022-05-19",
    "2.4.0": "2021-06-10",
    "2.3.2": "2020-10-15",
    "2.3.1": "2019-11-14",
    "2.3.0": "2018-05-31",
    "2.2.9": "2017-11-14",
    "2.2.8": "2016-07-21",
    "2.2.7": "? - Not listed at https://spase-group.org/data/model/index.html",
    "2.2.6": "2015-09-09",
    "2.2.5": "2015-09-09",
    "2.2.4": "2015-05-31",
    "2.2.3": "2014-05-22",
    "2.2.2": "2012-02-27",
    "2.2.1": "2011-08-18",
    "2.2.0": "2011-01-06",
    "2.1.0": "2010-03-19",
    "2.0.0": "2009-04-15",
    "1.3.0": "2008-11-22",
    "1.2.2": "2008-08-14",
    "1.2.1": "2008-03-20",
    "1.2.0": "2007-05-22",
    "1.1.0": "2006-08-31",
    "1.0.0": "2005-11-22"
  }
