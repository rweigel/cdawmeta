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

all-regen: cdawmeta.egg-info
	python metadata.py --meta-type hapi --regen --regen-skip cadence

all-update: cdawmeta.egg-info
	python metadata.py --meta-type hapi --update --update-skip cadence

clean:
	-rm -rf data/*

test-README:
	python metadata.py --id AC_OR_SSC --meta-type hapi
	python metadata.py --id AC_OR_SSC --meta-type spase_auto
	python metadata.py --id AC_OR_SSC --meta-type soso
	python metadata.py --id AC_OR_SSC --update
	python metadata.py --id AC_OR_SSC --regen

test-table:
	python table.py --id '^AC_OR'

test-report:
	python report.py --id AC_OR_DEF --update

#test-query:

rsync-to-mag:
	rsync -avz --exclude data/hpde.io --exclude data/cdaweb.gsfc.nasa.gov \
		--delete data weigel@mag.gmu.edu:www/git-data/cdawmeta

rsync-from-mag:
	rsync -avz weigel@mag.gmu.edu:www/git-data/cdawmeta/ .

cdawmeta.egg-info:
	pip install -e .
################################################################################

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
	python table.py --table-name cdaweb.variable

data/table/cdaweb.dataset.sql:
	python table.py --table-name cdaweb.dataset

data/table/spase.dataset.sql:
	python table.py --table-name spase.dataset

data/table/spase.parameter.sql:
	python table.py --table-name spase.parameter

table-serve: data/table/cdaweb.variable.sql data/table/cdaweb.dataset.sql data/table/spase.parameter.sql data/table/spase.dataset.sql
	-pkill -f "python table/table-ui/serve.py"
	$(PYTHON) table/table-ui/serve.py --port 8051 --sqldb data/table/cdaweb.variable.sql &
	$(PYTHON) table/table-ui/serve.py --port 8052 --sqldb data/table/cdaweb.dataset.sql &
	$(PYTHON) table/table-ui/serve.py --port 8053 --sqldb data/table/spase.parameter.sql &
	$(PYTHON) table/table-ui/serve.py --port 8054 --sqldb data/table/spase.dataset.sql
################################################################################
