import json
import cdawmeta

dir_name = 'query'
cldefs = cdawmeta.cli('query.py', defs=True)
collection_names = cldefs['collection-name']['choices']

args = cdawmeta.cli('query.py')
collection_name = args['collection_name']

if args['port'] is None:
  args['port'] = 27017

filter = args['filter']
del args['filter']

try:
  filter = json.loads(filter)
except Exception:
  raise ValueError(f"Could not parse filter as JSON: {filter}")

if collection_name is not None: 
  collection_names = [collection_name]

for collection_name in collection_names:
  args['collection_name'] = collection_name
  cdawmeta.db.write_mongodb(**args)

if True:
  documents = cdawmeta.db.query(args['collection_name'], port=args['port'], filter=filter)
  match = "documents match" if len(documents) > 1 else "document matches"
  print(f"{len(documents)} {match} query of {filter}:")

  for document in documents:
    print(f"  '{document['_id']}'")
