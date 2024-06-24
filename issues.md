1\. These `FILLVALs` are suspect:

* [-9.999999796611898e-32](http://localhost:8051/#FILLVAL=-9.999999796611898e-32) (given [`-1e+31` returns ~50k hits](https://hapi-server.org/meta/cdaweb/#FILLVAL=-1e%2b31))

* [-9.999999680285692e+37](https://hapi-server.org/meta/cdaweb/#FILLVAL=-9.999999680285692e%2b37)

* [1.0000000331813535e+32](https://hapi-server.org/meta/cdaweb/#FILLVAL=1.0000000331813535e%2b32)


2\. Nand often has `FILL=-2.14748006E9` for integer type variables. To see the occurrences, [search the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `-2.14748006E9`. I suspect the reason is similar to that in item 3. below. I also see Nand with a fill of `99999.9` when the master has `100000.0` and also NaN when the master has `-1e+31`. [Search on the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `99999.9` and `NaN`).


3\. There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using non-CDF master metadata that differs. I am unsure if this is the type of error you correct, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master. To see the occurrences, search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `double`.


4\. Not all data variables have a `DEPEND_0` in [`BAR_2L_L2_HKPG`](https://hapi-server.org/meta/cdaweb/#datasetID=BAR_2L_L2_HKPG)


5\. In `WI_OR_DEF/SUN_VECTOR`, Nand has a description of

`'GCI Sun Position Vector'  ` 

but the master has 'Position' misspelled and right whitespace padding:

`'GCI Sun Postion Vector     '`

Given I am using masters, this is unexpected.


6\. My code (which uses the JSON representation of the master CDFs) claims that the units for `ISS_SP_FPMU/TCC` is the ASCII `null`. I see `"UNITS":"\u0000"` in a.json from

```
curl "https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json" > a.json
```

However, I don't see `"UNITS":"\u0000"` in the `VarAttributes` for `TCC` when I open [iss_sp_fpmu_00000000_v01.json](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json) in a browser. I don't understand this.


7\. If you search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/catalog-all.log) for `Error:`, you will see ~300 possible errors in the master CDFs associated with missing

1. `VarAttributes`

2. `VAR_TYPE` ([39 cases](https://hapi-server.org/meta/cdaweb/#VAR_TYPE=''))

3. `DimSizes` (I throw an error if a `DimsSizes` is not given for a parameter with a `DEPEND_{1,2,3}` because most do. I'm not sure if this is an error.)

   For example [SOLO_L2_RPW-LFR-SURV-BP2/BP2_RE_N_F0](https://hapi-server.org/meta/cdaweb/#VariableName='BP2_RE_N_F0')

and

4. `DEPEND_{1,2,3}` variable names that are not variables in the dataset.

    For example:

    ```
    PO_H2_TIM
      Error: Dropping variable "H_spins" because it has a DEPEND_0 "H_epoch" that is not in dataset
    ```

   The explanation is that [`PO_H2_TIM` has a variable named `H_Epoch`](https://hapi-server.org/meta/cdaweb/#datasetID=PO_H2_TIM) (upper case "E").


8\. [The logfile](http://mag.gmu.edu/git-data/cdawmeta/data/cdaweb.errors.log) has many errors associated with the fact that `spase_DatasetResourceID` does not start with `spase://`. I think an empty space is placed there because a SPASE record does not exist, which may be there to make validation pass on a validator that only checks for a non-empty string. It also has many `404` errors for URLs to `hpde.io` based on `spase_DatasetResourceID`.

9\.

From https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html, "ISTP attribute names must be capitalized". I find "sig_digits" and "SI_conv" confusing because it seems that it is an ISTP attribute name. Are some of the names on this page not ISTP attributes?

Also, a while back I mentioned the mixed usage of SI_conv and SI_conversion at https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html. Which is it?

10\.

Missing VarAttributes in ela_sun in ELA_L1_STATE_DEFN
Missing VarAttributes in elb_sun in ELB_L1_STATE_DEFN
Missing VarAttributes in STE_spectra_LABL_1 in STB_L1_STE