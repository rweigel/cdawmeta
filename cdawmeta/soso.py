import cdawmeta

logger = None

def soso(id=None, update=True, diffs=False, max_workers=None, orig_data=False, log_level='info', skip=None):
  global logger
  if logger is None:
    logger = cdawmeta.logger('soso')
    logger.setLevel(log_level.upper())

  return cdawmeta.generate(id, _soso, logger, update=update, diffs=diffs, max_workers=max_workers, orig_data=orig_data, skip=skip)

def _soso(metadatum):

  jsonld = {
    "@context": "https://schema.org/",
    "@type": "Dataset",
    "@id": "https://example.org/datasets/1234567890",
    "identifier": "doi:10.1234/1234567890",
    "name": "Removal of organic carbon by natural bacterioplankton communities as a function of pCO2 from laboratory experiments between 2012 and 2016",
    "description": "A description between 50 and 5000 characters.",
    "url": "https://example.org/datasets/1234567890",
    "version": None,
    "keywords": None,
    "license": None,
    "sameAs": None,
    "isAccessibleForFree": "true",
    "startDate": None,
    "endDate": None,
  }

  id = metadatum['id']

  allxml = metadatum['allxml']
  master = metadatum['master']
  cdawmeta.util.print_dict(allxml)
  cdawmeta.util.print_dict(master)

  jsonld['@id'] = allxml['@ID']
  jsonld['@identifier'] = allxml['@ID']
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

  cdawmeta.util.print_dict(jsonld)
