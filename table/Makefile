all:
	make table-hapi
	make table-all

table-all: table-all.py
	make table-ui/ajax/cdaweb/all.table.head.json

table-ui/ajax/cdaweb/all.table.head.json: ../data/all-resolved.restructured.json
	python table-all.py

serve-all: table-ui/ajax/cdaweb/all.table.head.json
	@- git clone https://github.com/rweigel/table-ui
	@- cd table-ui/ajax; ln -s ../../../data/tables cdaweb
	cd table-ui/ajax; python server.py 8051 cdaweb/all.table.head.json cdaweb/all.table.body.json cdaweb/all.table.conf.json


table-hapi: table-hapi.py
	make table-ui/ajax/cdaweb/hapi.table.head.json

table-ui/ajax/cdaweb/hapi.table.head.json: ../data/hapi-bw.json ../data/hapi-nl.json
	python table-hapi.py

serve-hapi: table-ui/ajax/cdaweb/hapi.table.head.json
	@- git clone https://github.com/rweigel/table-ui
	@- cd table-ui/ajax; ln -s ../../../data/tables cdaweb
	cd table-ui/ajax; python server.py 8052 cdaweb/hapi.table.head.json cdaweb/hapi.table.body.json cdaweb/hapi.table.conf.json

serve-hapi-sql: table-ui/ajax/cdaweb/hapi.table.head.json
	@- git clone https://github.com/rweigel/table-ui
	@- cd table-ui/ajax; ln -s ../../../data/tables cdaweb
	cd table-ui/ajax; python server.py 8052 cdaweb/hapi.table.head.json cdaweb/hapi.table.body.json.sql cdaweb/hapi.table.conf.json
