PYTHON=~/anaconda3/bin/python

CDAWEBVARIABLEHEAD=../data/table/cdaweb.table.variable.head.json
CDAWEBVARIABLEBODY=../data/table/cdaweb.table.variable.body.json
CDAWEBDATASETHEAD=../data/table/cdaweb.table.dataset.head.json
CDAWEBDATASETBODY=../data/table/cdaweb.table.dataset.body.json

all:
	make cdaweb-json
	make cdaweb-sql


serve-cdaweb-sql:
	make table-ui
	make cdaweb-sql
	$(PYTHON) table-ui/ajax/server.py 8051 $(CDAWEBVARIABLEHEAD) $(CDAWEBVARIABLEBODY).sql &
	$(PYTHON) table-ui/ajax/server.py 8052 $(CDAWEBDATASETHEAD) $(CDAWEBDATASETBODY).sql

cdaweb-json:
	make $(CDAWEBVARIABLEBODY)
	make $(CDAWEBDATASETBODY)

cdaweb-sql:
	#cd ..; python table.py --data_dir ./data
	make $(CDAWEBVARIABLEBODY).sql
	make $(CDAWEBDATASETBODY).sql

$(CDAWEBVARIABLEBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBVARIABLEBODY)
	python table-ui/ajax/sql/sql.py $(CDAWEBVARIABLEHEAD) $(CDAWEBVARIABLEBODY)

$(CDAWEBDATASETBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBDATASETBODY)
	python table-ui/ajax/sql/sql.py $(CDAWEBDATASETHEAD) $(CDAWEBDATASETBODY)



HAPIHEAD=../data/tables/hapi.table.head.json
HAPIBODY=../data/tables/hapi.table.body.json
HAPI=../data/hapi/catalog-all.json

serve-hapi-json:
	make table-ui
	make hapi-json
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY)

serve-hapi-sql:
	make table-ui
	make hapi-sql
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY).sql

hapi-json: $(HAPI)
	make $(HAPIBODY)

hapi-sql:
	make $(HAPIBODY).sql

$(HAPI):
	cd ../; make hapi-new

$(HAPIBODY): table-hapi.py $(HAPI)
	python table-hapi.py

$(HAPIBODY).sql: table-ui/ajax/sql/sql.py $(HAPIBODY)
	python table-ui/ajax/sql/sql.py $(HAPIHEAD) $(HAPIBODY)
