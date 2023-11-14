# To force re-recreation of all metadata and tables:
#   make clean; make all
#
# For a fast update, use
#   make all --always-make
# This causes all scripts to be executed. The scripts use caching so only
# metadata parts that appear in the json files in ./cache that need to be
# updated will be updated.
#
# If an update is needed due to a source code change, use
#   make all
# This will use the cached .json files in ./cache only.

all:
	make hapi-bw
	make hapi-nl
	make tables

bundle:
	cp compare.py compare
	mkdir -p compare
	cp compare.py compare/
	cp data/hapi-bw.json compare/
	tar zcvf compare.tgz compare/
	scp compare.tgz weigel@mag.gmu.edu:www/tmp

compare:
	make log/compare.log

log/compare.log: data/hapi-bw.json data/hapi-nl.json compare.py
	python compare.py | tee log/compare.log


data/all-resolve.json: all-resolve.py
	python all-resolve.py

data/all-restructure.json: data/all-resolve.json all-restructure.py
	python all-restructure.py



data/hapi-bw.json: data/all-restructure.json hapi-bw.py hapi-nl-issues.json
	python hapi-bw.py | tee log/hapi-bw.log

hapi-bw:
	make data/hapi-bw.json


data/hapi-nl.json: hapi-nl.py
	python hapi-nl.py

hapi-nl:
	make data/hapi-nl.json


tables:
	make table-hapi
	make table-all

table-hapi:
	make data/tables/hapi.table.body.json

data/tables/hapi.table.body.json: table/table-hapi.py data/hapi-bw.json data/hapi-nl.json
	python table/table-hapi.py

table-all:
	make data/tables/all.table.body.json

data/tables/all.table.body.json: table/table-all.py data/all-restructure.json
	python table/table-all.py


clean:
	rm -f data/*