# https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x-tarball/
# mkdir -p data/mongodb
# ~/mongodb-macos-x86_64-7.0.14/bin/mongod --dbpath data/mongodb --logpath data/mongo.log --fork

import cdawmeta

from pymongo import MongoClient

clargs = cdawmeta.cli('table.py')
print(clargs)
print(cdawmeta.DATA_DIR)
exit()
db_name = "spase"
collection_name = clargs['id']

meta = cdawmeta.metadata(id=id, )
documents = []
for key, value in meta.items():
  spase = meta[key]['spase']['data']['Spase']
  documents.append({"_id": key, **spase})

client = MongoClient('localhost', 27017)

db_list = client.list_database_names()
print(f"Database list: {db_list}")

db = client[db_name]
if db_name in db_list:
  print(f"Database {db_name} exists. Dropping it.")
  client.drop_database(db_name)

if False:
  collection_list = db.list_collection_names()
  print(f"Collection list in {db_name}: {collection_list}")

collection = db[collection_name]
#if collection_name in collection_list:
#  print(f"Collection {collection_name} in {db_name} exists.")

x = collection.insert_many(documents)

query = {"Version": "2.4.1"}
count = collection.count_documents(query)
print(f"{count} documents with Version 2.4.1:")
documents = collection.find(query)
for x in documents:
  print(x["Version"])
