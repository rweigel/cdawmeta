1. I think the `FILLVAL` on [these 5](https://hapi-server.org/meta/cdaweb/#VariableName=E1W_DTC_FLUX&FILLVAL=-9.999999796611898e-32) is wrong:

given [`-1e+31` returns ~50k hits](https://hapi-server.org/meta/cdaweb/#FILLVAL=-1e+31)

2. These `FILLVALs` are suspect:

   https://hapi-server.org/meta/cdaweb/#FILLVAL=-9.999999680285692e+37

   https://hapi-server.org/meta/cdaweb/#FILLVAL=1.0000000331813535e+32

3. Not all data variables have a `DEPEND_0` in `BAR_2L_L2_HKPG`. See https://hapi-server.org/meta/cdaweb/#datasetID=BAR_2L_L2_HKPG

4. In `WI_OR_DEF/SUN_VECTOR`, Nand has a description of

`'GCI Sun Position Vector'  ` 

but the master has 'Position' misspelled and right whitespace padding:

`'GCI Sun Postion Vector     '`

5. There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using non-CDF master metadata that differs. I am unsure if this is the type of error you correct, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master. To see the occurrences, search on `double` in http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log

6. Nand often has `FILL=-2.14748006E9` for integer type variables. To see the occurrences, search on `-2.14748006E9` in http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log. I suspect the reason is similar to that above. I also see Nand with a fill of `99999.9` when the master has `100000.0` and also NaN when the master has `-1e+31`. (Search on `99999.9` and `NaN` in http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log.)

7. My code (which uses the JSON representation of the master CDFs) claims that the units for `ISS_SP_FPMU/TCC` is the ASCII `null`. 
I see `"UNITS":"\u0000"` it in a.json from

```
curl "https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json" > a.json
```

However, I don't see it when I open [iss_sp_fpmu_00000000_v01.json](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/iss_sp_fpmu_00000000_v01.json) in a browser.

8. If you search on `Error:` in http://mag.gmu.edu/git-data/cdawmeta/data/hapi/catalog-all.log, you will see ~300 possible errors in the master CDFs associated with missing

  1. `VarAttributes`,
  2. `VAR_TYPE` [39 cases](https://hapi-server.org/meta/cdaweb/#VAR_TYPE='')
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

9. If you search on "Error getting" in http://mag.gmu.edu/git-data/cdawmeta/data/cdaweb.log you will see many errors associated with the fact that `spase_DatasetResourceID` does not start with `spase://`. I think an empty space is placed there because a SPASE record does not exist, which may be there to make validation pass on a validator that only checks for a non-empty string.
