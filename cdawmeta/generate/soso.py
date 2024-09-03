import cdawmeta

logger = None

def soso(metadatum=None, update=True, regen=False, diffs=False, log_level='info'):
  from .generate import generate
  global logger
  if logger is None:
    logger = cdawmeta.logger('soso')
    logger.setLevel(log_level.upper())

  return generate(metadatum, _soso, logger, update=update, regen=regen, diffs=diffs)

def _soso(metadatum):

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

  allxml = metadatum['allxml']
  master = metadatum['master']

  jsonld['@id'] = metadatum['id']
  jsonld['identifier'] = allxml['@ID']
  jsonld['startDate'] = allxml['@timerange_start'].replace(' ', 'T') + "Z"
  jsonld['endDate'] = allxml['@timerange_stop'].replace(' ', 'T') + "Z"
  jsonld['name'] = cdawmeta.util.get_path(master, ['data', 'CDFglobalAttributes', 'TITLE'])
  jsonld['description'] = cdawmeta.util.get_path(allxml, ['description', '@short'])

  keywords = []
  keywords.append(cdawmeta.util.get_path(allxml, ['observatory', 'description', '@short']))
  keywords.append(cdawmeta.util.get_path(allxml, ['instrument', 'description', '@short']))
  discipline = cdawmeta.util.get_path(master, ['data', 'CDFglobalAttributes', 'Discipline']).split('>')
  keywords = [*keywords, *discipline]
  jsonld['keywords'] = keywords

  if include_variableMeasured:
    # TODO: We are using HAPI metadata because all of the issues with master CDF
    # metadata has been handled. One should really modify the code so we create
    # "emaster" which handles all of the issues and also creates UNITS_VO* and
    # then use metadatum['emaster']['data'].
    #
    # *In _variableMeasured, we are using the raw CDF units, which do not
    # conform to a schema but marking them as conforming to vounits.
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

  if isinstance(hapi, list):
    # TODO: Loop over all
    hapi = hapi[0]

  #vounits_url = 'https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html#tth_sEc2.4'
  xmlschema_url = 'https://www.w3.org/TR/xmlschema-2/'

  variableMeasured = []
  for parameter in hapi['info']['parameters']:
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