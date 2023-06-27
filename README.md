# Running

```
git clone https://github.com/rweigel/cdaweb-hapi.git
cd cdaweb-hapi
make hapi-bw
```

# Notes

**Python files**

```
python all.py     # creates data/all-resolved.json containing master CDF and SPASE (if available) as JSON
python all-restructure.py # creates data/all-resolved.restructured.json using data/all-resolved.json
python hapi-bw.py # creates data/hapi-bw.json using data/all-resolved.restructured.json
python hapi-nl.py # creates data/hapi-nl.json using https://cdaweb.gsfc.nasa.gov/hapi/{catalog,info}
```

# TODO:

* Write one file per HAPI dataset

# Visualize/Compare

See `table/README.md` for visualizing/comparing results.
