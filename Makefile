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

UPDATE=''

all:
	make hapi UPDATE=$(UPDATE)
	make hapi-nl
	make compare

clean:
	-rm -rf data/*

rsync-to-mag:
	rsync -avz --delete data weigel@mag.gmu.edu:www/git-data/cdawmeta

rsync-from-mag:
	rsync -avz weigel@mag.gmu.edu:www/git-data/cdawmeta/ .

compare:
	make hapi UPDATE=$(UPDATE)
	make hapi-nl
	python hapi/compare.py | tee data/hapi/compare.log

cdawmeta.egg-info:
	pip install -e .

################################################################################
cdaweb: cdaweb.py
	make data/all.json UPDATE=$(UPDATE)

data/all.json: cdaweb.py
	python cdaweb.py --data_dir ./data UPDATE=$(UPDATE)
################################################################################

################################################################################
hapi:
	make data/hapi/catalog-all.json UPDATE=$(UPDATE)

data/hapi/catalog-all.json: cdawmeta.egg-info hapi.py cdawmeta/hapi-nl-issues.json
	python hapi.py --data_dir ./data UPDATE=$(UPDATE)
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
