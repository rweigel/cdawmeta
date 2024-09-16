import cdawmeta

dependencies = ['spase', 'master', 'cadence', 'hapi', 'AccessInformation']

def spase_auto(metadatum, logger):

  spase_auto_ = {
    "Spase": {
      "xmlns": "http://www.spase-group.org/data/schema",
      "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
      "xsi:schemaLocation": "http://www.spase-group.org/data/schema http://www.spase-group.org/data/schema/spase-2_6_1.xsd",
      "Version": "2.6.1"
      }
    }

  NumericalData = {"ResourceID": None, "ResourceHeader": {}}

  url_master = metadatum['master']['url']
  spase_auto_['Spase']['_MasterURL'] = url_master
  spase = cdawmeta.util.get_path(metadatum, ['spase', 'data'])
  if spase is not None:
    url_spase = metadatum['spase']['url']
    spase_auto_['Spase']['_SPASEURL'] = url_spase

    p = ['Spase', 'NumericalData', 'ResourceID']
    NumericalData['ResourceID'] = cdawmeta.util.get_path(spase, p)

    p = ['Spase', 'NumericalData', 'ResourceHeader', 'DOI']
    NumericalData['ResourceHeader']['DOI'] = cdawmeta.util.get_path(spase, p)

    p = ['Spase', 'NumericalData', 'ObservedRegion']
    NumericalData['ObservedRegion'] = cdawmeta.util.get_path(spase, p)
  else:
    # Compute ResourceID based on CDAWeb ID and cadence.
    pass

  allxml = metadatum['allxml']
  master = metadatum['master']['data']

  p = ['CDFglobalAttributes', 'Logical_source_description']
  NumericalData['ResourceHeader']['ResourceName'] = cdawmeta.util.get_path(master, p)

  p = ['CDFglobalAttributes', 'TEXT']
  NumericalData['ResourceHeader']['_Description'] = cdawmeta.util.get_path(master, p)
  if spase is not None:
    Description = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'ResourceHeader', 'Description'])
    NumericalData['ResourceHeader']['Description'] = Description

  p = ['CDFglobalAttributes', 'Acknowledgement']
  NumericalData['ResourceHeader']['_Acknowledgement'] = cdawmeta.util.get_path(master, p)
  if spase is not None:
    Acknowledgement = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'ResourceHeader', 'Acknowledgement'])
    NumericalData['ResourceHeader']['Acknowledgement'] = Acknowledgement

  NumericalData['ResourceHeader']['_PublicationInfo'] = None
  if spase is not None:
    PublicationInfo = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'ResourceHeader', 'PublicationInfo'])
    NumericalData['ResourceHeader']['PublicationInfo'] = PublicationInfo

  links = cdawmeta.util.get_path(allxml, ['other_info', 'link'])
  if links is not None:
    InformationURLs = []
    if isinstance(links, dict):
      links = [links]

    for link in links:
      if '@URL' not in link:
        continue
      InformationURL = {
        "URL": link['@URL'],
      }
      if '@title' in link:
        InformationURL['Name'] = link['@title']
      if '#text' in link:
        InformationURL['Description'] = link['#text']
      InformationURLs.append(InformationURL)

    if len(InformationURLs) > 0:
      NumericalData['ResourceHeader']['_InformationURL'] = InformationURLs

  if spase is not None:
    InformationURL = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'ResourceHeader', 'InformationURL'])
    NumericalData['ResourceHeader']['InformationURL'] = InformationURL

  if True:
    NumericalData['_AccessInformation'] = metadatum['AccessInformation']['data']
    if spase is not None:
      AccessInformation = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'AccessInformation'])
      NumericalData['AccessInformation'] = AccessInformation

  NumericalData['_TemporalDescription'] = {'TimeSpan': {}}
  StartDate = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  NumericalData['_TemporalDescription']['TimeSpan']['StartDate'] = StartDate
  StopDate = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  NumericalData['_TemporalDescription']['TimeSpan']['StopDate'] = StopDate
  cadence = metadatum['cadence']['data']
  depend_0_name = list(cadence.keys())[0]
  counts = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'counts'])
  CadenceNote = cdawmeta.util.get_path(metadatum, ['cadence', 'data', depend_0_name, 'note'])
  if len(list(cadence.keys())) > 1:
    CadenceNote = " This dataset contains parameters that depend on different time variables with different cadences."

  Cadence = counts[0]['duration_iso8601']
  NumericalData['_TemporalDescription']['_Cadence'] = Cadence
  NumericalData['_TemporalDescription']['_CadenceNote'] = CadenceNote
  if spase is not None:
    TemporalDescription = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'TemporalDescription'])
    NumericalData['TemporalDescription'] = TemporalDescription

  NumericalData['_ProcessingLevel'] = None
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ProcessingLevel']
    ProcessingLevel = cdawmeta.util.get_path(spase, p)
    NumericalData['ProcessingLevel'] = ProcessingLevel

  name = cdawmeta.util.get_path(master, ['CDFglobalAttributes', 'TITLE'])
  NumericalData['_ProviderResourceName_via_master/CDFglobalAttributes/TITLE'] = name
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ProviderResourceName']
    ProviderResourceName = cdawmeta.util.get_path(spase, p)
    NumericalData['ProviderResourceName'] = ProviderResourceName

  InstrumentID = cdawmeta.util.get_path(allxml, ['instrument', '@ID'])
  NumericalData['_InstrumentID_via_allxml/instrument/@ID'] = InstrumentID
  if spase:
    InstrumentID = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'InstrumentID'])
    NumericalData['InstrumentID'] = InstrumentID

  NumericalData['_MeasurementType'] = "TODO: Map from _InstrumentID_via_allxml/instrument/@ID to SPASE MeasurementType"
  if spase:
    MeasurementType = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'MeasurementType'])
    NumericalData['MeasurementType'] = MeasurementType

  p = ['CDFglobalAttributes', 'Rules_of_use']
  NumericalData['_Caveats_via_Master/CDFglobalAttributes/Rules_of_use'] = cdawmeta.util.get_path(master, p)
  if spase is not None:
    Caveats = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Caveats'])
    NumericalData['Caveats'] = Caveats

  keywords = []
  keywords.append(cdawmeta.util.get_path(allxml, ['observatory', 'description', '@short']))
  keywords.append(cdawmeta.util.get_path(allxml, ['instrument', 'description', '@short']))
  for key in ['Discipline', 'Source_name', 'Data_type']:
    val = cdawmeta.util.get_path(master, ['CDFglobalAttributes', key])
    if val is not None:
      val = val.replace(">", "?").split('?')
      keywords = [*keywords, *val]
      keywords = list(dict.fromkeys(keywords))
  InstrumentID = cdawmeta.util.get_path(allxml, ['instrument', '@ID'])
  if InstrumentID is not None:
    keywords.append(InstrumentID)
  NumericalData['_Keyword'] = keywords
  if spase is not None:
    Keyword = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Keyword'])
    NumericalData['Keyword'] = Keyword

  if True:
    hapi = metadatum['hapi']['data']
    from cdawmeta._generate.hapi import flatten_parameters
    parameters = flatten_parameters(hapi)

    NumericalData['_Parameter'] = []
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

      NumericalData['_Parameter'].append(Parameter)

  if spase is not None:
    Parameter = cdawmeta.util.get_path(spase, ['Spase', 'NumericalData', 'Parameter'])
    NumericalData['_Parameter'].append(Parameter)

  spase_auto_['Spase']['NumericalData'] = NumericalData

  return [spase_auto_]