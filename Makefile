# Use
#   make clean; make
# to force re-recreation of all metadata

all:
	make hapi-bw
	make hapi-nl
	make tables

compare:
	make log/compare.log

log/compare.log: data/hapi-bw.json data/hapi-nl.json compare.py
	python compare.py | tee log/compare.log

data/all-resolved.json: all.py
	python all.py

data/all-resolved.restructured.json: data/all-resolved.json all-restructure.py
	python all-restructure.py


data/hapi-bw.json: data/all-resolved.json hapi-bw.py
	python hapi-bw.py | tee log/hapi-bw.log

hapi-bw: data/all-resolved.restructured.json
	make data/hapi-bw.json

data/hapi-nl.json: hapi-nl.py
	python hapi-nl.py

hapi-nl:
	make data/hapi-nl.json



tables: data/hapi-bw.json data/hapi-nl.json data/all-resolved.restructured.json
	make table-hapi
	make table-all

table-hapi: table/table-hapi.py
	make data/tables/hapi.table.head.json

data/tables/hapi.table.head.json: data/hapi-bw.json data/hapi-nl.json
	python table/table-hapi.py

table-all: table/table-all.py
	make data/tables/all.table.head.json

data/tables/all.table.head.json: data/all-resolved.restructured.json
	python table/table-all.py

clean:
	rm -f data/*