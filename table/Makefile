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

cdaweb-sql:
	make $(CDAWEBBODY).sql

cdaweb-json: $(CDAWEB)
	make $(CDAWEBBODY)

$(CDAWEB):
	cd ../; make cdaweb

$(CDAWEBBODY): table-cdaweb.py $(CDAWEB)
	python table-cdaweb.py

$(CDAWEBBODY).sql: table-ui/ajax/sql/sql.py $(CDAWEBBODY)
	python table-ui/ajax/sql/sql.py $(CDAWEBHEAD) $(CDAWEBBODY)



HAPIHEAD=../data/tables/hapi.table.head.json
HAPIBODY=../data/tables/hapi.table.body.json

serve-hapi:
	cd ../; make table-hapi
	make table-ui
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY)

serve-hapi-sql:
	cd ../; make table-hapi
	make hapi-sql
	python table-ui/ajax/server.py 8052 $(HAPIHEAD) $(HAPIBODY).sql

hapi-sql:
	cd ../; make table-hapi
	make table-ui
	make $(HAPIBODY).sql

$(HAPIBODY).sql: table-ui/ajax/sql/sql.py $(HAPIHEAD) $(HAPIBODY)
	python table-ui/ajax/sql/sql.py $(HAPIHEAD) $(HAPIBODY)

