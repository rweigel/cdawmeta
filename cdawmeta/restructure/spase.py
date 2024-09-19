import copy
import cdawmeta

def spase(spase, logger=None):

  if spase is None:
    return None

  spase_r = copy.deepcopy(spase)

  # Convert Parameter array to a dict with keys of ParameterKey
  Parameter = cdawmeta.util.get_path(spase_r, ['Spase', 'NumericalData', 'Parameter'])
  if Parameter is not None:
    if not isinstance(Parameter, list):
      Parameter = [Parameter]

    _Parameter = cdawmeta.util.array_to_dict(Parameter, 'ParameterKey')
    if _Parameter is None:
      Parameter = cdawmeta.util.array_to_dict(Parameter, 'Name')
      spase_r['Spase']['NumericalData']['Parameter'] = Parameter
    else:
      spase_r['Spase']['NumericalData']['Parameter'] = _Parameter

  # Flatten AccessInformation so that each array element (Repository) only has
  # one AccessURL. If a Repository has N AccessURLs, create N Repositories, each
  # with one AccessURL with all other Repository fields the same.
  p = ['Spase', 'NumericalData', 'AccessInformation']
  AccessInformation = cdawmeta.util.get_path(spase_r, p)
  if AccessInformation is not None:
    AccessInformation = AccessInformation.copy()
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
      for _, obj in enumerate(AccessURL):
        SubRepository = Repository.copy()
        SubRepository['AccessURL'] = obj
        Repositories.append(SubRepository)

    spase_r['Spase']['NumericalData']['AccessInformation'] = Repositories

  return spase_r
