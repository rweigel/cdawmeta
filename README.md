# Running

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta
make hapi-new
```

See the comments in `Makefile` for additional execution options.

**Scripts**

```
python cdaweb.py         # creates cdaweb/main.{json.log}
python hapi/hapi-new.py  # creates data/hapi/hapi-new.{json.log} using data/main.json
python hapi/hapi-nl.py   # creates data/hapi/hapi-nl.json using 
                         # https://cdaweb.gsfc.nasa.gov/hapi/{catalog,info}
```

# Compare

To compare the new HAPI metadata with Nand's, use

```
make compare [--include='PATTERN'] # creates hapi/compare.log
# PATTERN is a dataset ID regular expression, e.g., '^AC_'
```

# Browse and Search

See `table/README.md` for browsing and searching metadata from a web interface.
