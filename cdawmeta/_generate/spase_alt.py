import cdawmeta

def spase_alt(metadatum, logger):

  spase_alt = {
    "Spase": {
      "xmlns": "http://www.spase-group.org/data/schema",
      "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
      "xsi:schemaLocation": "http://www.spase-group.org/data/schema http://www.spase-group.org/data/schema/spase-2_6_1.xsd",
      "Version": "2.6.1"
      }
    }

  NumericalData = {"ResourceHeader": {}, "TemporalDescription": {"TimeSpan": {}}}

  spase = cdawmeta.util.get_path(metadatum, ['spase', 'data'])
  if spase is not None:
    p = ['Spase', 'NumericalData', 'ResourceID']
    NumericalData['ResourceID'] = cdawmeta.util.get_path(spase, p)

    p = ['Spase', 'NumericalData', 'ResourceHeader', 'DOI']
    NumericalData['ResourceHeader']['DOI'] = cdawmeta.util.get_path(spase, p)

    p = ['Spase', 'NumericalData', 'ObservedRegion']
    NumericalData['ObservedRegion'] = cdawmeta.util.get_path(spase, p)
  else:
    # Compute ResourceID based on CDAWeb ID and cadence.
    pass

  master = metadatum['master']['data']
  p = ['CDFglobalAttributes', 'Logical_source_description']
  NumericalData['ResourceHeader']['ResourceName'] = cdawmeta.util.get_path(master, p)

  p = ['CDFglobalAttributes', 'TEXT']
  NumericalData['ResourceHeader']['Description'] = cdawmeta.util.get_path(master, p)

  p = ['CDFglobalAttributes', 'Acknowledgement']
  NumericalData['ResourceHeader']['Acknowledgement'] = cdawmeta.util.get_path(master, p)

  p = ['CDFglobalAttributes', 'Rules_of_use']
  NumericalData['Caveats'] = cdawmeta.util.get_path(master, p)


  allxml = metadatum['allxml']
  StartDate = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  NumericalData['TemporalDescription']['TimeSpan']['StartDate'] = StartDate

  StopDate = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  NumericalData['TemporalDescription']['TimeSpan']['StopDate'] = StopDate

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
      NumericalData['ResourceHeader']['InformationURL'] = InformationURLs

  name = cdawmeta.util.get_path(allxml, ['description', '@short'])
  NumericalData['ResourceHeader']['ProviderResourceName'] = name

  NumericalData['AccessInformation'] = metadatum['AccessInformation']['data']

  hapi = metadatum['hapi']['data']
  from cdawmeta._generate.hapi import flatten_parameters
  parameters = flatten_parameters(hapi)

  NumericalData['Parameter'] = []
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

    NumericalData['Parameter'].append(Parameter)

  spase_alt['Spase']['NumericalData'] = NumericalData

  return [spase_alt]