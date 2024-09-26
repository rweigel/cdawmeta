import os

import cdawmeta

dependencies = ['orig_data', 'hapi']

def AccessInformation(metadatum, logger):

  repo_path = os.path.join(cdawmeta.DATA_DIR, 'cdawmeta-additions')
  if not os.path.exists(repo_path):
    import git
    repo_url = cdawmeta.CONFIG['urls']['cdawmeta-additions']
    logger.info(f"Cloning {repo_url} into {repo_path}")
    git.Repo.clone_from(repo_url, repo_path, depth=1)

  fname = os.path.join(repo_path, 'AccessInformation.json')
  AccessInformation = cdawmeta.util.read(fname)

  allxml = metadatum['allxml']

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
    orig_data = metadatum['orig_data']['data']['FileDescription']
    DataExtent = 0
    for file in orig_data:
      DataExtent += file['Length']
    AccessURL['DataExtent'] = DataExtent


  dsid = metadatum['id']
  for key in AccessInformation:
    AccessInformation[key]['AccessURL']['ProductKey'] = dsid
    ack = AccessInformation[key]['Acknowledgement']
    name = allxml['data_producer']['@name']
    affiliation = allxml['data_producer']['@affiliation'].strip()
    AccessInformation[key]['Acknowledgement'] = f'{ack}. Please also acknowledge the data producer: {name} at {affiliation}'

    url = AccessInformation[key]['AccessURL']['URL']
    if "{dsid}" in url and "HAPI" not in key:
      AccessInformation[key]['AccessURL']['URL'] = url.format(dsid=dsid)

  # TODO: Add note that additional Ephemeris variables are available and
  #       the values may not match those in the CDF file.
  ssc = allxml['instrument']['@ID'] == 'SSC'
  if not ssc:
    del AccessInformation['SSCWeb/SSCWS']
    del AccessInformation['SSCWeb/HAPI']
    del AccessInformation['SSCWeb/HAPI/Program']

  if ssc:
    sscid = allxml['observatory']['@ID'].lower()
    AccessInformation['SSCWeb/SSCWS']['AccessURL']['ProductKey'] = sscid
    AccessInformation['SSCWeb/HAPI']['AccessURL']['ProductKey'] = sscid
    url = AccessInformation['SSCWeb/HAPI']['AccessURL']['URL']
    AccessInformation['SSCWeb/HAPI']['AccessURL']['URL'] = url.format(dsid=sscid)


  # TODO: Get languages from https://hapi-server.org/servers/?return=script-options
  hapi_languages = ['IDL', 'Javascript', 'MATLAB', 'Python', 'Autoplot', 'curl', 'wget']
  hapi_languages = ', '.join(hapi_languages)

  Description = AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description']
  AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description'] = Description.format(hapi_languages=hapi_languages)
  hapi_language_formats = []
  for language in hapi_languages.split(', '):
    hapi_language_formats.append(f"_Script.{language.strip()}")
  AccessInformation['CDAWeb/HAPI/Program']['Format'] = hapi_language_formats

  if ssc:
    AccessInformation['SSCWeb/HAPI/Program']['AccessURL']['Description'] = Description.format(hapi_languages=hapi_languages)
    AccessInformation['SSCWeb/HAPI/Program']['Format'] = hapi_language_formats

  hapi = metadatum['hapi']['data']
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
    Description += x_Description
    AccessInformation['CDAWeb/HAPI']['AccessURL']['Description'] = Description

    Description = AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description']
    Description += x_Description
    AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['Description'] = Description
  else:
    hapi_keys = hapi['id']
  del AccessInformation['CDAWeb/HAPI']['AccessURL']['x_Description']

  AccessInformation['CDAWeb/HAPI']['AccessURL']['ProductKey'] = hapi_keys
  AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['ProductKey'] = hapi_keys

  hapi_keys_0 = hapi_keys
  if isinstance(hapi_keys, list):
    hapi_keys_0 = hapi_keys[0]

  url = AccessInformation['CDAWeb/HAPI']['AccessURL']['URL']
  AccessInformation['CDAWeb/HAPI']['AccessURL']['URL'] = url.format(dsid=hapi_keys_0)

  url = AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['URL']
  AccessInformation['CDAWeb/HAPI/Program']['AccessURL']['URL'] = url.format(dsid=hapi_keys_0)

  # Strip leading key under AccessInformation and keep only their values.
  AccessInformation = [information for information in AccessInformation.values()]

  # TODO: Check URLs. Also add to AccessInformation _URLRegEx and report error
  #       if non-200 or RegEx does not match.
  return [AccessInformation]
