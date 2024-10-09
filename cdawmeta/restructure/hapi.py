import copy

import cdawmeta

def hapi(hapi, simplify_bins=False, logger=None):

  hapi = copy.deepcopy(hapi)
  info = copy.deepcopy(hapi['info'])
  del hapi['info']
  hapi = {**hapi, **info}
  del hapi['id']

  # bins = [{'name': val, 'attr': val, ...}, {'name': val, 'attr': val, ...}, ...] =>
  # {'bins[0]/name': val, 'bins[0]/attr': val, ..., 'bins[1]/name': val, 'bins[1]/attr': val, ...}
  # Then bins object is removed an these attributes are placed at the parameter level
  if simplify_bins:
    for parameter in hapi['parameters']:
      new_bins = {}
      if 'bins' in parameter:
        for idx, bin in enumerate(parameter['bins']):
          for attribute in ['centers', 'ranges']:
            if attribute in bin:
              values = bin[attribute]
              if len(values) > 5:
                bin[attribute] = [values[0], '...', values[-1]]
          new_bins[f"bin[{idx}]"] = bin
        del parameter['bins']
        parameter.update(cdawmeta.util.flatten_dicts(new_bins))

  hapi['parameters'] = cdawmeta.util.array_to_dict(hapi['parameters'], key='name')

  return hapi