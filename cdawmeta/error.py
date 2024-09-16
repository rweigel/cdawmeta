def error(generator, id, name, msg, logger):
  logger.error(msg)
  if generator not in error.errors:
    error.errors[generator] = {}
  if id not in error.errors[generator]:
    error.errors[generator][id] = {}
  if name is None:
    error.errors[generator][id]["_"] = msg.lstrip()
  else:
    if name not in error.errors[generator][id]:
      error.errors[generator][id][name] = []
    error.errors[generator][id][name].append(msg.lstrip())
error.errors = {}
