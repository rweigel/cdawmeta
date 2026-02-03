import cdawmeta

#dependencies = ['orig_data', 'hapi']
dependencies = ['hapi']

def AccessInformation(metadatum, logger):

  import re
  import copy

  additions = cdawmeta.additions(logger)
  AccessInformation = copy.deepcopy(additions['AccessInformation'])

  allxml = metadatum['allxml']
  dsid = metadatum['id']

  keys_to_delete = []
  for key in AccessInformation:
    _cdaweb_ids = AccessInformation[key].get('_cdaweb_ids', None)
    if _cdaweb_ids is not None:
      for _cdaweb_id in _cdaweb_ids:
        if _cdaweb_id.startswith('^'):
          regex = re.compile(_cdaweb_id)
          if not regex.match(dsid):
            keys_to_delete.append(key)
        else:
          if _cdaweb_id != dsid:
            keys_to_delete.append(key)
      del AccessInformation[key]['_cdaweb_ids']

  for key in keys_to_delete:
    del AccessInformation[key]

  for protocol in ["HTTPS", "FTPS"]:
    repo = f'CDAWeb/{protocol}/CDF'
    AccessURL = AccessInformation[repo]['AccessURL']
    AccessURL['AccessFilenameTemplate'] = allxml['access']['@filenaming']
    subdividedby = allxml['access']['@subdividedby']
    if subdividedby.lower() != "none":
      AccessURL['AccessDirectoryTemplate'] = subdividedby
    AccessURL['URL'] = allxml['access']['URL']
    if protocol == 'FTPS':
      AccessURL['URL'] = AccessURL['URL'].replace('https', 'ftps')

    if 'orig_data' in metadatum:
      orig_data = metadatum['orig_data']['data']['FileDescription']
      DataExtent = 0
      for file in orig_data:
        DataExtent += file['Length']
      AccessURL['DataExtent'] = DataExtent

  for key in AccessInformation:
    AccessInformation[key]['AccessURL']['ProductKey'] = dsid
    ack = AccessInformation[key]['Acknowledgement']
    name = allxml['data_producer']['@name']
    affiliation = allxml['data_producer']['@affiliation'].strip()
    ack = f'{ack}. Please also acknowledge the data producer: {name} at {affiliation}'
    AccessInformation[key]['Acknowledgement'] = ack

    url = AccessInformation[key]['AccessURL']['URL']
    if "{dsid}" in url and "HAPI" not in key:
      # Replace "{dsid}" with the actual dataset ID unless HAPI is in the key.
      # (HAPI URLs are hanlded differently.)
      AccessInformation[key]['AccessURL']['URL'] = url.format(dsid=dsid)

  ssc = allxml['instrument']['@ID'] == 'SSC'
  if ssc:
    sscid = allxml['observatory']['@ID'].lower()
    for key in ['SSCWeb/SSCWS', 'SSCWeb/HAPI', 'SSCWeb/HAPI/Program', 'SSCWeb/HAPI/Plot']:
      AccessInformation[key]['AccessURL']['ProductKey'] = sscid
  else:
    for key in ['SSCWeb/SSCWS', 'SSCWeb/HAPI', 'SSCWeb/HAPI/Program', 'SSCWeb/HAPI/Plot']:
      if key in AccessInformation:
        del AccessInformation[key]

  hapi_languages, hapi_language_formats = _hapi_languages()
  Description = AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description']
  Description = Description.format(hapi_languages=hapi_languages)
  AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description'] = Description
  AccessInformation['CDAWeb/HAPI/Program']['Format'] = hapi_language_formats

  if ssc:
    AccessInformation['SSCWeb/HAPI/Program']['AccessURL']['Description'] = Description.format(hapi_languages=hapi_languages)
    AccessInformation['SSCWeb/HAPI/Plot']['AccessURL']['Description'] = Description.format(hapi_languages=hapi_languages)
    AccessInformation['SSCWeb/HAPI/Program']['Format'] = hapi_language_formats

  hapi = metadatum['hapi']['data']
  if not hapi:
    print(f"Warning: No HAPI dataset information found for {dsid}")
    for key in list(AccessInformation.keys()):
      if 'HAPI' in key:
        del AccessInformation[key]
  else:
    hapi_keys = []
    time_variable_names = []
    if isinstance(hapi, list):
      for dsidx, dataset in enumerate(hapi):
        time_variable_names.append(f"@{dsidx} => {dataset['info']['parameters'][0]['x_cdf_NAME']}")
        hapi_keys.append(dataset['id'])
        n_keys = len(hapi_keys)

      x_Description = AccessInformation['CDAWeb/HAPI']['AccessURL']['x_Description']
      x_Description = " " + x_Description.strip().format(n_keys=n_keys, time_variable_names=", ".join(time_variable_names))

      Description = AccessInformation['CDAWeb/HAPI']['AccessURL']['Description']
      Description = Description.format(n_keys=n_keys, time_variable_names=", ".join(time_variable_names))
      Description += x_Description
      AccessInformation['CDAWeb/HAPI']['AccessURL']['Description'] = Description

      Description = AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description']
      Description += x_Description
      AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description'] = Description
    else:
      hapi_keys = hapi['id']

    del AccessInformation['CDAWeb/HAPI']['AccessURL']['x_Description']

    hapi_keys_0 = hapi_keys
    if isinstance(hapi_keys, list):
      hapi_keys_0 = hapi_keys[0]

    for key in ['CDAWeb/HAPI', 'CDAWeb/HAPI/Program', 'CDAWeb/HAPI/Plot']:
      AccessInformation[key]['AccessURL']['ProductKey'] = hapi_keys
      AccessInformation[key]['AccessURL']['ProductKey'] = hapi_keys
      url = AccessInformation[key]['AccessURL']['URL']
      AccessInformation[key]['AccessURL']['URL'] = url.format(dsid=hapi_keys_0)

  # Strip leading key under AccessInformation and keep only their values.
  AccessInformation = [information for information in AccessInformation.values()]

  # TODO: Check URLs. Also add to AccessInformation _URLRegEx and report error
  #       if non-200 or RegEx does not match.
  return [AccessInformation]

def _hapi_languages():

  # TODO: Get languages from https://hapi-server.org/servers/?return=script-options
  languages = ['IDL', 'Javascript', 'MATLAB', 'Python', 'Autoplot', 'curl', 'wget']
  languages = ', '.join(languages)
  language_formats = []
  for language in languages.split(', '):
    language_formats.append(f"x_Script.{language.strip()}")
  return languages, language_formats