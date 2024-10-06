Each key in [units-CDFUNITS_to_SPASEUnit-map](units-CDFUNITS_to_SPASEUnit-map) is a unique CDF master unit found across all variables. The value is an object with keys of associated SPASE Units and values of counts.

So, for example

```
  "[fraction]": {
    "Ratio": 88,
    "(cm^2 s sr MeV)^-1)": 60
  }
```

means that the CDF `UNIT` string `[fraction]` was converted to a SPASE `Unit` string of `Ratio` 88 times and `(cm^2 s sr MeV)^-1)` 60 times. To see the actual CDF variables where `[fraction]` occurs, see [a search](https://hapi-server.org/meta/cdaweb/variable/#UNITS='[fraction]'), which shows that it occured 240 times. (The difference between 88+60=148 and 240 is due to the fact that not all CDF variables are represented in SPASE.)