# Running

```
git clone https://github.com/rweigel/cdaweb-hapi.git
cd cdaweb-hapi
python all.py     # creates data/all.json containing master CDF and SPASE (if available) as JSON
python hapi-bw.py # creates data/hapi-bw.json using data/all.json
python hapi-nl.py # creates data/hapi-nl.json using https://cdaweb.gsfc.nasa.gov/hapi/{catalog,info}
```

# Notes

The `omit` function in `hapi-bw.py` and `hapi-nl.py` controls which HAPI datasets are created.

The `omitids` list in `all.py` controls which master CDFs are omitted in creating `data/all.xml`.

`hapi-bw.py` does not yet include bins.

# TODO:

* Include bins
* Write one file per HAPI dataset

# Visualize/Compare

See `table/README.md` for visualizing/comparing results.
