# Use
#   make clean; make
# to force re-recreation of all metadata

all:
	make hapi-bw
	make hapi-nl
	make tables

compare:
	make compare.log

compare.log: data/hapi-bw.json data/hapi-nl.json compare.py
	python compare.py > compare.log

data/all-resolved.json: all.py
	python all.py

data/all-resolved.restructured.json: all-restructure.py
	python all-restructure.py


data/hapi-bw.json: data/all-resolved.json hapi-bw.py
	python hapi-bw.py

hapi-bw:
	make data/hapi-bw.json

data/hapi-nl.json: hapi-nl.py
	python hapi-nl.py

hapi-nl:
	make data/hapi-nl.json

tables:
	cd table; make

clean:
	rm -f data/*