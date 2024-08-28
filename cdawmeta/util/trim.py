def trim(label):
  if isinstance(label, str):
    return label.strip()
  for i in range(0, len(label)):
    if isinstance(label[i], str):
      label[i] = str(label[i]).strip()
  return label
