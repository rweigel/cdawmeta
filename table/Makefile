CDAWEBHEAD=../data/tables/cdaweb.table.head.json
CDAWEBBODY=../data/tables/cdaweb.table.body.json
CDAWEB=../data/cdaweb.json

table-ui:
	@- git clone https://github.com/rweigel/table-ui
	@- git pull https://github.com/rweigel/table-ui

serve-cdaweb-json:
	make table-ui
	make cdaweb-json
	cd table-ui/ajax; python server.py 8051 $(CDAWEBHEAD) $(CDAWEBBODY)

serve-cdaweb-sql:
	make table-ui
	make cdaweb-sql
	python table-ui/ajax/server.py 8051 $(CDAWEBHEAD) $(CDAWEBBODY).sql

cdaweb-json: $(CDAWEB)
	make $(CDAWEBBODY)

cdaweb-sql:
	make $(CDAWEBBODY).sql

$(CDAWEB):
	cd ../; make cdaweb

$(CDAWEBBODY): table-cdaweb.py $(CDAWEB)
	python table-cdaweb.py

$(CDAWEBBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBBODY)
	python table-ui/ajax/sql/sql.py $(CDAWEBHEAD) $(CDAWEBBODY)



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
