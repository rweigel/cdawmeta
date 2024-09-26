import os
import glob

import cdawmeta

dependencies = ['master', 'hapi', 'AccessInformation']

def _additions(logger):

  if hasattr(_additions, 'additions'):
    return _additions.additions

  additions_path = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-additions')
  pattern = f"{additions_path}/*.json"
  files = glob.glob(pattern, recursive=True)
  additions = {}
  for file in files:
    logger.info(f"Reading {file}")
    key = os.path.basename(file).replace(".json", "")
    additions[key] = cdawmeta.util.read(file)

  _additions.additions = additions
  return additions

def spase_auto(metadatum, logger):

  include_parameters = True
  include_access_information = False

  additions = _additions(logger)

  allxml = metadatum['allxml']
  master = metadatum['master']['data']
  hapi = metadatum['hapi']['data']
  # TODO: Switch to using master instead of HAPI for Parameter now that HAPI
  # generation code was refactored.

  Version = "2.6.1"
  spase_auto_ = {
    "Spase": {
      "xmlns": "http://www.spase-group.org/data/schema",
      "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
      "xsi:schemaLocation": "http://www.spase-group.org/data/schema http://www.spase-group.org/data/schema/spase-2_6_1.xsd",
      "_Note": "Nodes prefixed with a _ are not valid SPASE, but are inluded for debugging.",
      "Version": Version,
      "_VersionRelease": _VersionRelease()[Version]
      }
    }

  NumericalData = {
    "ResourceID": None,
    "ResourceHeader": {}
  }

  spase_auto_['Spase']['_MasterURL'] = cdawmeta.util.get_path(metadatum, ['master', 'url'])

  # TODO: Compute ResourceID based on CDAWeb ID and cadence.
  NumericalData['ResourceID'] = additions.get('ResourceID', None)
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

  if include_access_information:
    NumericalData['AccessInformation'] = metadatum['AccessInformation']['data']
    NumericalData['_AccessInformation'] = "Generated from AccessInformation.json template"

  NumericalData['TemporalDescription'] = _TemporalDescription(allxml)
  if isinstance(hapi, dict):
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

  if include_parameters:
    NumericalData['Parameter'] = _Parameter(hapi)

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

      InformationURL['_Note'] = "Generated from all.xml/other_info/link"

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

  if hapi_info['cadence'] is None:
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

def _Parameter(hapi):

  if isinstance(hapi, list):
    Parameter = []
    for dataset in hapi:
      Parameter.append(_Parameter(dataset))

    # https://stackoverflow.com/a/45323085
    return sum(Parameter, [])

  Parameters = []
  parameters = hapi['info']['parameters']
  Cadence = _Cadence(hapi['info'])
  for parameter in parameters:

    if parameter['type'] == 'isotime':
      Unit = "ms"
      DataType = parameter['x_cdf_DataType']
      if DataType == 'CDF_TIME_TT2000':
        Unit = "ns"
      if DataType == 'CDF_EPOCH16':
        Unit = "ps"
      Parameter = {
        "Name": parameter['name'],
        "ParameterKey": parameter['x_cdf_NAME'],
        "Units": Unit,
        "_Note": "This is in the source CDF file. Not all web services will provide access to this variable in this form (e.g., an ISO 8601 string may be used)."
      }
      if 'x_description' in parameter:
        Parameter['Description'] = parameter['x_description']
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
