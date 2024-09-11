import cdawmeta

def soso(metadatum, logger):

  jsonld = {
    "@context": [
        "https://schema.org/",
        {
            "qudt": "http://qudt.org/schema/qudt/",
            "unit": "http://qudt.org/vocab/unit/"
        }
    ],
    "@type": "Dataset"
  }

  include_variableMeasured = True

  keywords = []

  jsonld['@id'] = metadatum['id']

  allxml = metadatum['allxml']
  jsonld['identifier'] = allxml['@ID']
  jsonld['startDate'] = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  jsonld['endDate'] = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  keywords.append(cdawmeta.util.get_path(allxml, ['observatory', 'description', '@short']))
  keywords.append(cdawmeta.util.get_path(allxml, ['instrument', 'description', '@short']))


  master = metadatum['master']
  jsonld['name'] = cdawmeta.util.get_path(master, ['data', 'CDFglobalAttributes', 'TITLE'])
  jsonld['description'] = cdawmeta.util.get_path(allxml, ['description', '@short'])
  discipline = cdawmeta.util.get_path(master, ['data', 'CDFglobalAttributes', 'Discipline'])
  if discipline is None:
    discipline = []
  else:
    discipline = discipline.split(',')
  jsonld['keywords'] = [*keywords, *discipline]

  if include_variableMeasured:
    # TODO: We are using HAPI metadata because all of the issues with master CDF
    # metadata has been handled.
    hapi = metadatum['hapi']['data']
    jsonld['variableMeasured'] = _variableMeasured(hapi)

  return [jsonld]

def _cdf2xmltype(cdf_type):

  if cdf_type in ['CDF_CHAR', 'CDF_UCHAR']:
    return 'token'

  if cdf_type.startswith('CDF_EPOCH') or cdf_type.startswith('CDF_TIME'):
    return 'isotime'

  if cdf_type.startswith('CDF_INT'):
    return 'int'

  if cdf_type.startswith('CDF_UINT'):
    return 'nonNegativeInteger'

  if cdf_type.startswith('CDF_BYTE'):
    return 'boolean'

  if cdf_type in ['CDF_FLOAT', 'CDF_DOUBLE', 'CDF_REAL4', 'CDF_REAL8']:
    return 'decimal'

def _variableMeasured(hapi):

  #vounits_url = 'https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html#tth_sEc2.4'
  xmlschema_url = 'https://www.w3.org/TR/xmlschema-2/'

  from cdawmeta._generate.hapi import flatten_parameters
  parameters = flatten_parameters(hapi)

  variableMeasured = []
  for parameter in parameters:
    element = {
      "@type": "PropertyValue",
      "name": parameter['name'],
    }

    if 'description' in parameter:
      element['description'] = parameter['description']

    if 'units' in parameter:
      element['unitText'] = parameter['units']
      element['qudt:hasUnit'] = {"@id": parameter['units']}
      if 'x_cdf_DataType' in parameter:
        xml_unit = _cdf2xmltype(parameter['x_cdf_DataType'])
        element['qudt:dataType'] = f"{xmlschema_url}#{xml_unit}"

    if 'x_fractionDigits' in parameter:
      element['x_fractionDigits'] = parameter['x_fractionDigits']


    variableMeasured.append(element)

  return variableMeasured