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

INCLUDE=.*

all:
	make hapi-new
	make hapi-nl

clean:
	-rm -rf data/*

rsync:
	rsync -avz data weigel@mag.gmu.edu:www/git-data/cdawmeta

compare:
	make cdaweb INCLUDE='$(INCLUDE)'
	make hapi-new
	python hapi/compare.py --include '$(INCLUDE)' | tee data/hapi/compare.log

cdawmeta.egg-info:
	pip install -e .

################################################################################
cdaweb:
	make data/cdaweb.json INCLUDE='$(INCLUDE)'

data/cdaweb.json: cdaweb.py
	python cdaweb.py --include '$(INCLUDE)' | tee data/cdaweb.log
################################################################################

################################################################################
hapi-new: cdawmeta.egg-info
	make data/hapi/catalog-all.json

data/hapi/catalog-all.json: data/cdaweb.json hapi/hapi-new.py hapi/hapi-nl-issues.json
	python hapi/hapi-new.py | tee data/hapi/catalog-all.log
################################################################################

################################################################################
data/hapi/hapi-nl.json: hapi/hapi-nl.py
	python hapi/hapi-nl.py | tee data/hapi/hapi-nl.log

hapi-nl: cdawmeta.egg-info
	make data/hapi/hapi-nl.json
################################################################################

################################################################################
spase: data/spase/spase.log
	make data/spase-units.txt

data/spase/spase.log: data/cdaweb.json spase/spase.py
	python spase/spase.py | tee data/spase/spase.log
################################################################################