import cdawmeta

dependencies = ['spase', 'master', 'cadence', 'hapi', 'AccessInformation']

def spase_auto(metadatum, logger):

  include_parameters = False
  include_access_information = False

  allxml = metadatum['allxml']
  master = metadatum['master']['data']
  spase = metadatum['spase']['data']
  cadence = metadatum['cadence']['data']
  hapi = metadatum['hapi']['data']
  # TODO: Switch to using master instead of HAPI for Parameter now that HAPI
  #  generation code was refactored.

  _Version = "2.6.1"
  spase_auto_ = {
    "Spase": {
      "xmlns": "http://www.spase-group.org/data/schema",
      "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
      "xsi:schemaLocation": "http://www.spase-group.org/data/schema http://www.spase-group.org/data/schema/spase-2_6_1.xsd",
      "_Note": "Nodes prefixed with a _ were auto-generated. Values prefixed with an _ are not valid SPASE, but are needed for completenes.",
      "_Version": _Version,
      "_VersionRelease": _VersionRelease()[_Version]
      }
    }

  if spase is not None:
    p = ['Spase', 'Version']
    Version = cdawmeta.util.get_path(spase, p)
    spase_auto_['Spase']['Version'] = Version
    spase_auto_['Spase']['VersionRelease'] = _VersionRelease()[Version]

  NumericalData = {
    "_ResourceID": None,
    "ResourceID": None,
    "ResourceHeader": {}
  }

  url_master = metadatum['master']['url']
  spase_auto_['Spase']['_MasterURL'] = url_master
  if spase is not None:
    url_spase = metadatum['spase']['url']
    spase_auto_['Spase']['_SPASEURL'] = url_spase

  # TODO: Much of the following could be put in a loop that uses a dict config
  # with SPASE path, master path, etc.

  # TODO: Compute ResourceID based on CDAWeb ID and cadence.
  NumericalData['_ResourceID'] = metadatum['id']
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceID']
    NumericalData['ResourceID'] = cdawmeta.util.get_path(spase, p)

  NumericalData['ResourceHeader']['_DOI'] = None
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'DOI']
    NumericalData['ResourceHeader']['DOI'] = cdawmeta.util.get_path(spase, p)

  p = ['CDFglobalAttributes', 'Logical_source_description']
  via = f' (from {"/".join(p)})'
  _ResourceName = cdawmeta.util.get_path(master, p)
  if _ResourceName is not None:
    NumericalData['ResourceHeader']['_ResourceName'] = _ResourceName + via
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'ResourceName']
    ResourceName = cdawmeta.util.get_path(spase, p)
    NumericalData['ResourceHeader']['ResourceName'] = ResourceName

  p = ['CDFglobalAttributes', 'TEXT']
  _Description = cdawmeta.util.get_path(master, p)
  if _Description is not None:
    via = "(from CDFglobalAttributes/TEXT) "
    NumericalData['ResourceHeader']['_Description'] = via + _Description
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'Description']
    Description = cdawmeta.util.get_path(spase, p)
    NumericalData['ResourceHeader']['Description'] = Description

  p = ['CDFglobalAttributes', 'Acknowledgement']
  ack = cdawmeta.util.get_path(master, p)
  NumericalData['ResourceHeader']['_Acknowledgement'] = ack
  if ack is not None:
    via = "(from CDFglobalAttributes/Acknowledgement) "
    NumericalData['ResourceHeader']['_Acknowledgement'] = via + ack
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'Acknowledgement']
    Acknowledgement = cdawmeta.util.get_path(spase, p)
    NumericalData['ResourceHeader']['Acknowledgement'] = Acknowledgement

  NumericalData['ResourceHeader']['_PublicationInfo'] = None
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'PublicationInfo']
    PublicationInfo = cdawmeta.util.get_path(spase, p)
    NumericalData['ResourceHeader']['PublicationInfo'] = PublicationInfo

  NumericalData['ResourceHeader']['_InformationURL'] = _InformationURL(allxml)
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceHeader', 'InformationURL']
    InformationURL = cdawmeta.util.get_path(spase, p)
    NumericalData['ResourceHeader']['InformationURL'] = InformationURL

  if include_access_information:
    NumericalData['_AccessInformationNote'] = "Generated from AccessInformation.json template"
    NumericalData['_AccessInformation'] = metadatum['AccessInformation']['data']
    if spase is not None:
      p = ['Spase', 'NumericalData', 'AccessInformation']
      AccessInformation = cdawmeta.util.get_path(spase, p)
      NumericalData['AccessInformation'] = AccessInformation

  NumericalData['_TemporalDescription'] = _TemporalDescription(cadence, allxml)
  if spase is not None:
    p = ['Spase', 'NumericalData', 'TemporalDescription']
    TemporalDescription = cdawmeta.util.get_path(spase, p)
    NumericalData['TemporalDescription'] = TemporalDescription

  NumericalData['_Keyword'] = _Keyword(allxml, master)
  if spase is not None:
    p = ['Spase', 'NumericalData', 'Keyword']
    NumericalData['Keyword'] = cdawmeta.util.get_path(spase, p)

  NumericalData['_ObservedRegion'] = None
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ObservedRegion']
    NumericalData['ObservedRegion'] = cdawmeta.util.get_path(spase, p)

  NumericalData['_ProcessingLevel'] = None
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ProcessingLevel']
    NumericalData['ProcessingLevel'] = cdawmeta.util.get_path(spase, p)

  p = ['CDFglobalAttributes', 'TITLE']
  _ProviderResourceName = cdawmeta.util.get_path(master, p)
  if _ProviderResourceName is not None:
    via = " (from master/CDFglobalAttributes/TITLE)"
    NumericalData['_ProviderResourceName'] = _ProviderResourceName + via
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ProviderResourceName']
    NumericalData['ProviderResourceName'] = cdawmeta.util.get_path(spase, p)

  _InstrumentID = cdawmeta.util.get_path(allxml, ['instrument', '@ID'])
  if _InstrumentID is not None:
    via = ' (from allxml/instrument/@ID)'
    NumericalData['_InstrumentID'] = _InstrumentID + via
  if spase:
    p = ['Spase', 'NumericalData', 'InstrumentID']
    NumericalData['InstrumentID'] = cdawmeta.util.get_path(spase, p)

  msg = "TODO: Map from table _InstrumentID => SPASE/MeasurementType"
  NumericalData['_MeasurementType'] = msg
  if spase:
    p = ['Spase', 'NumericalData', 'MeasurementType']
    MeasurementType = cdawmeta.util.get_path(spase, p)
    NumericalData['MeasurementType'] = MeasurementType

  p = ['CDFglobalAttributes', 'Rules_of_use']
  _Caveats = cdawmeta.util.get_path(master, p)
  if _Caveats is not None:
    via = " (from Master/CDFglobalAttributes/Rules_of_use)"
    NumericalData['_Caveats'] = _Caveats
  if spase is not None:
    p = ['Spase', 'NumericalData', 'Caveats']
    Caveats = cdawmeta.util.get_path(spase, p)
    NumericalData['Caveats'] = Caveats

  if include_parameters:
    NumericalData['_Parameter'] = _Parameter(hapi)
    if spase is not None:
      p = ['Spase', 'NumericalData', 'Parameter']
      NumericalData['Parameter'] = cdawmeta.util.get_path(spase, p)

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

def _TemporalDescription(cadence, allxml):

  _TemporalDescriptionNote = "Generated from all.xml/@timerange_start and all.xml/@timerange_stop"
  _TemporalDescription = {
        'TimeSpan': {},
        '_TemporalDescriptionNote': _TemporalDescriptionNote
  }

  StartDate = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  _TemporalDescription['TimeSpan']['StartDate'] = StartDate
  StopDate = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  _TemporalDescription['TimeSpan']['StopDate'] = StopDate

  if cadence is not None:

    depend_0_name = list(cadence.keys())[0]
    counts = cdawmeta.util.get_path(cadence, [depend_0_name, 'counts'])
    CadenceCaveat = cdawmeta.util.get_path(cadence, [depend_0_name, 'note'])
    if len(list(cadence.keys())) > 1:
      CadenceCaveat = "This dataset contains parameters that depend on "
      CadenceCaveat += "different time variables with different cadences."

    Cadence = counts[0]['duration_iso8601']
    _TemporalDescription['_CadenceNote'] = "Generated by inspection of first CDF file."
    _TemporalDescription['_CadenceCaveat'] = CadenceCaveat
    _TemporalDescription['_Cadence'] = Cadence

    return _TemporalDescription

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

  from cdawmeta._generate.hapi import flatten_parameters
  parameters = flatten_parameters(hapi)

  _Parameter = []
  for parameter in parameters:
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
    if 'fill' in parameter:
      Parameter['FillValue'] = parameter['fill']
    if 'size' in parameter:
      Parameter['Structure'] = {'Size': parameter['size']}

    _Parameter.append(Parameter)

  return _Parameter

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
