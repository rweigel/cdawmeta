# https://www.mongodb.com/docs/manual/tutorial/install-mongodb-on-os-x-tarball/
# mkdir -p data/mongodb
# ~/mongodb-macos-x86_64-7.0.14/bin/mongod --dbpath data/mongodb --logpath data/mongo.log --fork

from pymongo import MongoClient

db_name = "db1"
collection_name = "collection1"

client = MongoClient('localhost', 27017)

db_list = client.list_database_names()
print(f"Database list: {db_list}")

db = client[db_name]
if db_name in db_list:
  print(f"Database {db_name} exists.")

collection_list = db.list_collection_names()
print(f"Collection list in {db_name}: {collection_list}")

collection = db[collection_name]
if collection_name in collection_list:
  print(f"Collection {collection_name} in {db_name} exists.")

document = { "name": "John", "address": "Highway 37" }
x = collection.insert_one(document)


for x in collection.find():
  print(x)


query = { "address": "Highway 37" }
documents = collection.find(query)
for x in documents:
  print(x)
