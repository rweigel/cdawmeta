# To force re-recreation of all metadata:
#   make clean; make all
#
# For a fast update, use
#   make all --always-make
# This causes all scripts to be executed. The scripts use caching so only
# metadata parts that appear in the json files in ./data/cache that need to be
# updated will be updated.
#
# If an update is needed due only to a source code change, use
#   make all

PYTHON=~/anaconda3/bin/python

all:
	make hapi
	make hapi-nl
	make compare

clean:
	-rm -rf data/*

rsync-to-mag:
	rsync -avz --delete data weigel@mag.gmu.edu:www/git-data/cdawmeta

rsync-from-mag:
	rsync -avz weigel@mag.gmu.edu:www/git-data/cdawmeta/ .

compare:
	make hapi
	make hapi-nl
	python hapi/compare.py | tee data/hapi/compare.log

cdawmeta.egg-info:
	pip install -e .

################################################################################
table/table-ui:
	@- cd table; git clone https://github.com/rweigel/table-ui
	@- cd table; git pull https://github.com/rweigel/table-ui

# Generate all tables
table-update: table/table-ui
	python table.py --update

# Use code to generate table code or metadata code it uses changes
table-regen:
	python table.py

data/table/cdaweb.variable.sql:
	python table.py --table_name cdaweb.variable

data/table/cdaweb.dataset.sql:
	python table.py --table_name cdaweb.dataset

data/table/spase.parameter.sql:
	python table.py --table_name spase.parameter

table-serve: data/table/cdaweb.variable.sql data/table/cdaweb.dataset.sql data/table/spase.parameter.sql
	$(PYTHON) table/table-ui/ajax/server.py --port 8051 --sqldb data/table/cdaweb.variable.sql &
	$(PYTHON) table/table-ui/ajax/server.py --port 8052 --sqldb data/table/cdaweb.dataset.sql &
	$(PYTHON) table/table-ui/ajax/server.py --port 8053 --sqldb data/table/spase.parameter.sql &
	$(PYTHON) table/table-ui/ajax/server.py --port 8054 --sqldb data/table/spase.dataset.sql
################################################################################

################################################################################
cdaweb: cdaweb.py
	make data/all.json

data/all.json: cdaweb.py
	python cdaweb.py --data_dir data
################################################################################

################################################################################
hapi:
	make data/hapi/catalog-all.json

data/hapi/catalog-all.json: cdawmeta.egg-info hapi.py cdawmeta/hapi-nl-issues.json
	python hapi.py --data_dir data
################################################################################

################################################################################
hapi-nl:
	make data/hapi/catalog-all.nl.json

data/hapi/catalog-all.nl.json: cdawmeta.egg-info hapi/hapi-nl.py
	python hapi/hapi-nl.py | tee data/hapi/catalog-all.nl.log
################################################################################

################################################################################
spase: data/spase/spase.log
	make data/spase-units.txt

data/spase/spase.log: data/cdaweb.json spase/spase.py
	python spase/spase.py | tee data/spase/spase.log
################################################################################
