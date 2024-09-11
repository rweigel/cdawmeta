import copy
import cdawmeta

def spase(spase, logger=None):

  spase_r = copy.deepcopy(spase)

  Parameter = cdawmeta.util.get_path(spase_r, ['Spase', 'NumericalData', 'Parameter'])
  if Parameter is not None:
    if not isinstance(Parameter, list):
      Parameter = [Parameter]

    Parameter = cdawmeta.util.array_to_dict(Parameter, 'ParameterKey')
    if Parameter is not None:
      spase_r['Spase']['NumericalData']['Parameter'] = Parameter

  AccessInformation = cdawmeta.util.get_path(spase_r, ['Spase', 'NumericalData', 'AccessInformation'])
  if AccessInformation is not None:
    AccessInformation = AccessInformation.copy()
    #cdawmeta.util.print_dict(AccessInformation, style='json')
    if isinstance(AccessInformation, dict):
      # Single Repository
      # AccessInformation = {"RepositoryID": ...} -> [{"RepositoryID": ...}]
      AccessInformation = [AccessInformation]

    Repositories = []
      # Loop over Repositories
    for _, Repository in enumerate(AccessInformation):
      AccessURL = Repository.get('AccessURL')

      if AccessURL is None:
        Repositories.append(Repository)
        continue

      if isinstance(AccessURL, dict):
        AccessURL = [AccessURL]

      # If Repository contains multiple AccessURLs, create one Repository for each.
      repo = Repository.copy()
      for _, obj in enumerate(AccessURL):
        repo['AccessURL'] = obj
        Repositories.append(repo)

    spase_r['Spase']['NumericalData']['AccessInformation'] = Repositories

  return spase_r
