#!/bin/bash

source /Users/weigel/opt/anaconda3/etc/profile.d/conda.sh; conda activate
conda activate base

cd ..;
python table-ui/ajax/server.py 8051 ../data/tables/cdaweb.table.head.json ../data/tables/cdaweb.table.body.json.sql
#make serve-cdaweb-sql-noupdate &
#make serve-hapi-sql
