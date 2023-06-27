HAPIHEAD=../data/tables/hapi.table.head.json
HAPIBODY=../data/tables/hapi.table.body.json

ALLHEAD=../data/tables/all.table.head.json
ALLBODY=../data/tables/all.table.body.json

tables:
	cd ../; make tables

table-ui:
	@- git clone https://github.com/rweigel/table-ui

serve-all:
	make tables
	make table-ui
	cd table-ui/ajax; python server.py 8051 $(ALLHEAD) $(ALLBODY)

serve-hapi:
	make tables
	make table-ui
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY)

serve-hapi-sql:
	make tables
	make hapi-sql
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY).sql

hapi-sql:
	make tables
	make table-ui
	make $(HAPIBODY).sql

$(HAPIBODY).sql: table-ui/ajax/sql/sql.py
	python table-ui/ajax/sql/sql.py $(HAPIHEAD) $(HAPIBODY)

