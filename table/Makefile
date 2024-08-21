CDAWEBVARIABLEHEAD=../data/table/cdaweb.table.variable.head.json
CDAWEBVARIABLEBODY=../data/table/cdaweb.table.variable.body.json
CDAWEBDATASETHEAD=../data/table/cdaweb.table.dataset.head.json
CDAWEBDATASETBODY=../data/table/cdaweb.table.dataset.body.json

all:
	make cdaweb-json
	make cdaweb-sql

table-ui:
	@- git clone https://github.com/rweigel/table-ui
	@- git pull https://github.com/rweigel/table-ui

serve-cdaweb-json:
	make table-ui
	make cdaweb-json
	cd table-ui/ajax; python server.py 8051 $(CDAWEBVARIABLEHEAD) $(CDAWEBVARIABLEBODY) &
	cd table-ui/ajax; python server.py 8052 $(CDAWEBDATASETHEAD) $(CDAWEBDATASETBODY)

serve-cdaweb-sql:
	make table-ui
	make cdaweb-sql
	python table-ui/ajax/server.py 8051 $(CDAWEBVARIABLEHEAD) $(CDAWEBVARIABLEBODY).sql &
	python table-ui/ajax/server.py 8052 $(CDAWEBDATASETHEAD) $(CDAWEBDATASETBODY).sql

cdaweb-json:
	make $(CDAWEBVARIABLEBODY)
	make $(CDAWEBDATASETBODY)

cdaweb-sql:
	make $(CDAWEBVARIABLEBODY).sql
	make $(CDAWEBDATASETBODY).sql

$(CDAWEBVARIABLEBODY): table-cdaweb.py
	python table-cdaweb.py

$(CDAWEBDATASETBODY): table-cdaweb.py
	python table-cdaweb.py

$(CDAWEBVARIABLEBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBVARIABLEBODY)
	python table-ui/ajax/sql/sql.py $(CDAWEBVARIABLEHEAD) $(CDAWEBVARIABLEBODY)

$(CDAWEBDATASETBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBVARIABLEBODY)
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
