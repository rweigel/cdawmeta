```
# Create ../data/tables/hapi.table.{body,header}.json
python table-hapi.py 
```

```
git clone https://github.com/rweigel/table-ui
cd table-ui
ln -s ../../data/tables cdaweb
python server.py cdaweb/hapi.table.header.json cdaweb/hapi.table.body.json
# Open http://0.0.0.0:8001
# Enter AC_OR_SSC in `id` column search input then hit enter.
```