import os
import sys
import sqlite3

import cdawmeta

def _unique_attribs(sqldb, table_name, attrib, logger):
  connection = sqlite3.connect(sqldb)

  query_ = f"SELECT `{attrib}`, COUNT(*) FROM '{table_name}' WHERE `{attrib}` IS NOT NULL AND `{attrib}` != '' GROUP BY `{attrib}`;"
  try:
    cursor = connection.cursor()
    cursor.execute(query_)
    func_counts = cursor.fetchall()
    total_count = sum(count for _, count in func_counts)
    logger.info(f"Total count of non-null, non-empty '{attrib}' values in table '{table_name}': {total_count}")
    logger.info(f"Unique '{attrib}' values and their counts in table '{table_name}':")
    for func, count in func_counts:
      logger.info(f"  {attrib}: {func}, Count: {count}")
  except Exception as e:
    logger.info(f"Error executing query for FUNC values using '{query_}' on {sqldb}")
    raise e
  finally:
    connection.close()

def funct(clargs):
  out_dir = 'reports'
  report_name = sys._getframe().f_code.co_name
  logger = cdawmeta.logger(name=f'{report_name}', dir_name=out_dir, log_level=clargs['log_level'])

  sqldb = './data/table/cdaweb.variable.sql'
  table_name = 'cdaweb.variable'

  _unique_attribs(sqldb, table_name, 'FUNCT', logger)
  _unique_attribs(sqldb, table_name, 'FUNCTION', logger)
