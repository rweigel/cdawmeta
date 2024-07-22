The following notes are based on results from the [`cdawmeta` package](https://github.com/rweigel/cdawmeta/), which was developed for creating metadata for the new CDAWeb HAPI server. The code in this package produces metadata stored in http://mag.gmu.edu/git-data/cdawmeta/data/ and the searchable [CDAWeb](https://hapi-server.org/meta/cdaweb/) and [HAPI](https://hapi-server.org/meta/hapi/) metadata tables.

1\. These `FILLVALs` are suspect:

* [-9.999999796611898e-32](https://hapi-server.org/meta/cdaweb/#FILLVAL=-9.999999796611898e-32) (given [`-1e+31` returns ~50k hits](https://hapi-server.org/meta/cdaweb/#FILLVAL=-1e%2b31))

* [-9.999999680285692e+37](https://hapi-server.org/meta/cdaweb/#FILLVAL=-9.999999680285692e%2b37)

* [1.0000000331813535e+32](https://hapi-server.org/meta/cdaweb/#FILLVAL=1.0000000331813535e%2b32)


2\. Nand often has `FILL=-2.14748006E9` for `integer` type variables. To see the occurrences, [search the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `-2.14748006E9`. I suspect the reason is similar to that in item 3. below. I also see Nand having a fill of `99999.9` when the master has `100000.0` and also NaN when the master has `-1e+31`. ([Search on the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `99999.9` and `NaN`).


3\. There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using different non-CDF master metadata. My understanding is that CDAWeb does not correct errors in non-master CDFs, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master (many do). To see the occurrences, search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `double`.


4\. Not all data variables have a `DEPEND_0` in [`BAR_2L_L2_HKPG`](https://hapi-server.org/meta/cdaweb/#datasetID=BAR_2L_L2_HKPG)


5\. In `WI_OR_DEF/SUN_VECTOR`, Nand has a description of

`'GCI Sun Position Vector'  ` 

but the master has 'Position' misspelled and right whitespace padding:

`'GCI Sun Postion Vector     '`

Given I am using masters, this is unexpected. Is this something that Nand corrects in his code, and if so, why was the correction not made in the master?

6\. My code (which uses the JSON representation of the master CDFs) claims that the units for `ISS_SP_FPMU/TCC` is the ASCII `null`. I see `"UNITS":"\u0000"` in a.json from

```
curl "https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json" > a.json
```

However, I don't see `"UNITS":"\u0000"` in the `VarAttributes` for `TCC` when I open [iss_sp_fpmu_00000000_v01.json](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json) in a browser. I don't understand this.


7\. If you search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/catalog-all.log) for `Error:`, errors in the master CDFs associated with missing

1. `VarAttributes`

2. `VAR_TYPE` (39 cases - this can also be seen in the [CDAWeb metadata table](https://hapi-server.org/meta/cdaweb/#VAR_TYPE=''))

3. `DimSizes` (I throw an error if a `DimSizes` is not given for a parameter with a `DEPEND_{1,2,3}` because most do. I'm not sure if this is an error.)

   For example [mms1_hpca_hplus_data_quality](https://hapi-server.org/meta/cdaweb/#VariableName=mms1_hpca_hplus_data_quality)

   In this case, `NumDims=0`, but there is a `DEPEND_1`.

and

4. `DEPEND_{1,2,3}` variables that are not variables in the dataset.

    For example:

    ```
    PO_H2_TIM
      Error: Dropping variable "H_spins" because it has a DEPEND_0 "H_epoch" that is not in dataset
    ```

   The explanation is that [`PO_H2_TIM` has a variable named `H_Epoch`](https://hapi-server.org/meta/cdaweb/#datasetID=PO_H2_TIM) (upper case "E").


8\. [The logfile](http://mag.gmu.edu/git-data/cdawmeta/data/cdaweb.errors.log) has many errors associated with the fact that `spase_DatasetResourceID` does not start with `spase://`. I think an empty space is placed there because a SPASE record does not exist, which may be there to make validation pass when using a validator that only checks for a non-empty string. There are also many `404` errors for URLs to `hpde.io` based on `spase_DatasetResourceID`.

9\.

From https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html, "ISTP attribute names must be capitalized". I find "sig_digits" and "SI_conv" confusing because it seems that it is an ISTP attribute name. Are some of the names on this page not ISTP attributes?

Also, a while back I mentioned the mixed usage of `SI_conv` and `SI_conversion` at https://spdf.gsfc.nasa.gov/istp_guide/vattributes.html. Which is it?

To create the [SQL table of CDAWeb metadata](https://hapi-server.org/meta/cdaweb/), I had to treat certain attribute names as equivalent (SQL column names are case insensitive, so one cannot create a column named `A` if a column named `a` is not allowed.).

Using [table-cdaweb.counts.csv](https://github.com/rweigel/cdawmeta/blob/main/table/table-cdaweb.counts.csv), I renamed many CDF attributes that differed by case (to address SQL column name constraint) or looked to be misspelled for equivalent to another more-used attribute name. The reaming mapping is in ([table-cdaweb.fixes.json](https://github.com/rweigel/cdawmeta/blob/main/table/table-cdaweb.fixes.json)).

10\.

There does not appear to be a consistent convention for `UNITS`. All of the unique units found in CDF masters are in [master-units.txt](https://github.com/rweigel/cdawmeta/blob/main/spase/master-units.txt).

It looks like the SPASE developers modified many of the units, but they, too, don't use a consistent convention. The left-hand column of [master2spase-units.txt](https://github.com/rweigel/cdawmeta/blob/main/spase/master2spase-units.txt) has unique units in CDAWeb masters. The right-hand side shows how they have been represented in SPASE.

It seems most sensible to use a consistent convention in the master CDFs (given that they are "masters"). Without this, the HAPI and SPASE codes that generate metadata from master CDFs would not need special replacement tables that need maintenance.

We've discussed before the idea of adding an ISTP attribute such as `UNITS_STANDARD` that would represent units according to an existing standard such as `udunits2`.

11\.

Are the `Error?:` cases in [catalog-all.log](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/catalog-all.log) errors?

12\.

SOLO is not listed at https://cdaweb.gsfc.nasa.gov/

14\.

No file list for DAWN_HELIO1DAY_POSITION

15\.

How are you able to show a menu for He energy center with options for telescope 1 and telescope 2 in the checkboxes

Given that He_energy_center does not have a DEPEND_2 or LABL_PTR_2?

https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/wi_epact_step-differential-ion-flux-1hr_00000000_v01.skt

It seems that He_energy_center should have a DEPEND_2 of TELESCOPE_index like H_energy_center.

Email thread:

https://mail.google.com/mail/u/0/#search/wi_epact_step-differential-ion-flux-1hr%2FHe_energy_center/QgrcJHsHsHbmrRRBtQKgQwxzsQHFxSjntRL
