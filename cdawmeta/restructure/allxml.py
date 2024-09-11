import copy
import cdawmeta

def allxml(allxml, logger=None):

  allxml = copy.deepcopy(allxml)

  if 'mission_group' in allxml:
    mission_groups = cdawmeta.util.array_to_dict(allxml['mission_group'],'@ID')
    allxml['mission_group'] = mission_groups
  if 'instrument_type' in allxml:
    instrument_type = cdawmeta.util.array_to_dict(allxml['instrument_type'],'@ID')
    allxml['instrument_type'] = instrument_type
  if 'links' in allxml:
    links = cdawmeta.util.array_to_dict(allxml['link'],'@URL')
    allxml['links'] = links
  for key, val in allxml.items():
    #if val is not None and '@ID' in val:
      # e.g., allxml['observatory] = {'@ID': 'ACE', ...} ->
      #       allxml['observatory']['ACE'] = {'@ID': 'ACE', ...}
      #allxml[key] = {val['@ID']: val}
    # TODO: Read all.xsd file and check if any others are lists that converted to dicts.
    if isinstance(val, list):
      logger.warning(f"Warning[all.xml]: {id}: {key} is a list and was not restructured.")

  return allxml
