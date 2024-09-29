f2c issues

https://github.com/rweigel/cdawmeta-additions/tree/main/reports

3\. There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using different non-CDF master metadata. My understanding is that CDAWeb does not correct errors in non-master CDFs, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master (many do). To see the occurrences, search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `double`.


7\. If you search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/catalog-all.log) for `Error:`, errors in the master CDFs associated with missing

1. `VarAttributes`

2. `VAR_TYPE` (39 cases - this can also be seen in the [CDAWeb metadata table](https://hapi-server.org/meta/cdaweb/#VAR_TYPE=''))

3. `DimSizes` (I throw an error if a `DimSizes` is not given for a parameter with a `DEPEND_{1,2,3}` because most do. I'm not sure if this is an error.)

   For example [mms1_hpca_hplus_data_quality](https://hapi-server.org/meta/cdaweb/#VariableName=mms1_hpca_hplus_data_quality)

   In this case, `NumDims=0`, but there is a `DEPEND_1`.

and

8\. [The logfile](http://mag.gmu.edu/git-data/cdawmeta/data/cdaweb.errors.log) has many errors associated with the fact that `spase_DatasetResourceID` does not start with `spase://`. I think an empty space is placed there because a SPASE record does not exist, which may be there to make validation pass when using a validator that only checks for a non-empty string. There are also many `404` errors for URLs to `hpde.io` based on `spase_DatasetResourceID`.

