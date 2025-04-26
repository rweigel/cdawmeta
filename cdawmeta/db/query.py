import pymongo
import cdawmeta

def query(collection_name, port=27017, filter=None, log_level='info'):

  logger = cdawmeta.logger('query')
  logger.setLevel(log_level.upper())

  logger.info(f"Querying collection '{collection_name}'.")

  indent = "  "
  done = f"{indent}Done."

  logger.info(f"{indent}Creating MongoClient for localhost:{port}.")
  client = pymongo.MongoClient('localhost', port)
  logger.info(done)

  logger.info(f"{indent}Accessing database '{collection_name}'.")
  db = client[collection_name]
  logger.info(done)

  logger.info(f"{indent}Accessing collection '{collection_name}'.")
  collection = db[collection_name]
  logger.info(done)

  def _count(filter):
    if filter is None:
      filter = {}
    logger.info(f"{indent}Executing collection.count_documents('{filter}').")
    count_ = collection.count_documents(filter)
    logger.info(done)
    return count_

  count = _count({})
  logger.info(f"  {count} documents found.")

  #logger.info(f"{indent}Printing all documents without filtering.")
  #for document in collection.find():
  #  print(document)
  #logger.info(done)

  logger.info(done)
  logger.info(f"{indent}Executing collection.find('{filter}').")
  documents_iter = collection.find(filter)
  logger.info(done)

  logger.info(f"{indent}Extracting documents.")
  documents = []
  for document in documents_iter:
    # Iterate over documents iterable to pull documents from db.
    documents.append(document)
  logger.info(done)
  logger.info(f"  {len(documents)} documents match filter = {filter}.")

  logger.info(f"{indent}Closing client.")
  client.close()
  logger.info(done)

  return documents
