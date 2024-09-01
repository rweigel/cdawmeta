def error(id, name, msg, logger):
  logger.error(msg)
  if id not in error.errors:
    error.errors[id] = {}
  if name is None:
    error.errors[id] = msg.lstrip()
  else:
    if name not in error.errors[id]:
      error.errors[id][name] = []
    error.errors[id][name].append(msg.lstrip())
error.errors = {}
