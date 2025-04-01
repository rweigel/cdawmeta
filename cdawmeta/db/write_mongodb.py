def write_mongodb(id=None, id_skip=None, mongod_binary=None, collection_name=None, port=27017,
                  update=False, update_skip=None, regen=False, regen_skip=None, max_workers=1, log_level='info'):

  import os
  import cdawmeta

  import pymongo

  db_path = os.path.join(cdawmeta.DATA_DIR, 'mongodb', collection_name)

  logger = cdawmeta.logger(name=f'{collection_name}', dir_name=db_path)
  logger.setLevel(log_level.upper())

  meta = cdawmeta.metadata(id=id, meta_type=collection_name, id_skip=id_skip,
                              update=update, regen=regen, embed_data=True,
                              diffs=False, max_workers=max_workers)

  status = cdawmeta.db.mongod(collection_name, mongod_binary=mongod_binary,
                              port=port, db_path=db_path, logger=logger)

  if status != 0:
    raise ValueError(f"mongod failed with status {status}")

  documents = []
  for dsid in meta.keys():
    if collection_name == 'spase':
      spase = cdawmeta.util.get_path(meta[dsid], ['spase', 'data', 'Spase'])
      if spase:
        documents.append({"_id": dsid, **spase})
    else:
      documents.append({"_id": dsid, **meta[dsid]})

  logger.info(f"Creating database {collection_name} and collection {collection_name}.")

  indent = "  "
  done = f"{indent}Done."

  logger.info(f"{indent}Creating MongoClient for localhost:{port}.")
  client = pymongo.MongoClient('localhost', port)
  logger.info(done)

  db_list = client.list_database_names()
  logger.info(f"{indent}Database list: {db_list}")

  logger.info(f"{indent}Creating wildcard index on all fields.")
  client[collection_name].command("createIndexes", collection_name, indexes=[{"key": {"$**": 1}, "name": "wildcard_index"}])
  logger.info(done)

  if collection_name in db_list:
    logger.info(f"{indent}Database '{collection_name}' exists. Dropping it.")
    client.drop_database(collection_name)
    logger.info(done)

  logger.info(f"{indent}Creating db '{collection_name}'.")
  db = client[collection_name]
  logger.info(done)

  # Creates a collection with same name as db. Each db will have one collection.
  logger.info(f"{indent}Creating collection '{collection_name}'.")
  collection = db[collection_name]
  logger.info(done)

  logger.info(f"{indent}Inserting {len(documents)} documents.")
  collection.insert_many(documents)
  logger.info(done)

  logger.info(f"{indent}Closing client.")
  client.close()
  logger.info(done)
