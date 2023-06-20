The `omit` function in `hapi-bw.py` and `hapi-nl.py` controls which HAPI datasets are created.

The `omitids` list in `all.py` controls which master CDFs are omitted in creating `data/all.xml`.

`hapi-bw.py` does not yet include bins.

```
python all.py     # creates data/all.json
python hapi-bw.py # creates data/hapi-bw.json
python hapi-nl.py # creates data/hapi-nl.json
```

Optional:

See `table/README.md` for visualizing/comparing results.
