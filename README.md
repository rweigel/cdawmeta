# About

This package is an interface to [CDAWeb's](https://cdaweb.gsfc.nasa.gov) metadata.

It was originally developed for upgrading the metadata from CDAWeb's HAPI server; the existing server only includes the minimum required metadata.

As discussed in the [SPASE](#SPASE) section, the code has been extended to remedy issues with existing SPASE metadata for CDAWeb datasets.

The code reads and combines information from

* [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which has dataset-level information about ~2,500 datasets;
* The [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/) files (represented in JSON) referenced in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which contain both  dataset-level metadata and variable metadata; and
* The list of URLs associated with all CDF files associated with a dataset using the CDASR [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) endpoint.
* A CDF file referenced in the [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) response (for computing cadence).

As discussed in the [SPASE](#SPASE) section, an attempt to use existing SPASE records was abandoned.

The code uses [requests-cache](https://github.com/requests-cache/requests-cache/) so re-downloads of any files are only made if the HTTP headers indicate it is needed. When metadata are downloaded, a diff is stored if it changed.

The output is

1. HAPI metadata, which is based on [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata. (The cadence of the measurements for each dataset by sampling the last CDF file associated with the dataset and a histogram of the differences in timesteps.)

2. SQL and MongoDB databases available with a search interface:
   * [CDAWeb datasets](https://hapi-server.org/meta/cdaweb/dataset/), which is based on content stored in [all.xml](http://mag.gmu.edu/git-data/cdawmeta/data/allxml) and [Masters CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * [CDAWeb variables](https://hapi-server.org/meta/cdaweb/variable/), which is based on content stored in [Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * [SPASE datasets](https://hapi-server.org/meta/spase/dataset/), which is based on content non-`Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * [SPASE parameters](https://hapi-server.org/meta/spase/parameter/), which is based on content `Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * [HAPI datasets](https://hapi-server.org/meta/hapi/parameter/), which is based on the non-`parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)
   * [HAPI parameters](https://hapi-server.org/meta/hapi/parameter/), which is based on the `parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)

Also, not yet available in table form is

3. proof-of-concept [SOSO](https://github.com/ESIPFed/science-on-schema.org) metadata (see [soso/info](http://mag.gmu.edu/git-data/cdawmeta/data/soso)). This metadata is derived using [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/), and [HAPI](http://mag.gmu.edu/git-data/cdawmeta/data/soso) metadata. We did not use SPASE records because of the issues described in the [SPASE](#SPASE) section. (Deriving part of the SOSO metadata from HAPI metadata is not ideal and was done for proof-of-concept purposes. The code could be modified to remove the dependency on HAPI metadata.)

and

4. proof-of-concept SPASE records that do not have most of the major issues described in [SPASE](#SPASE) section below (see [spase_auto/info](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto)).

<a id="SPASE"></a>
# SPASE

Our initial attempt was to generate HAPI metadata with SPASE records.

The primary issues related to HAPI are the first three. The others were noted in passing.

## 1 Completion

Only about 40% of CDAWeb datasets had SPASE records when we first considered using them for HAPI metadata in 2019. ~5 years later, there is ~70% coverage (however, as discussed below, the number of updated, correct, and without missing `Parameters` records is less). As a result, used metadata from [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/).

The implication is that CDAWeb `NumericalData` SPASE records cannot be used for one of the intended purposes, which is to provide a structured representation of CDAWeb metadata; we needed to duplicate much of the effort that went into creating CDAWeb SPASE records in order to create a complete set of HAPI metadata.

## 2 Updates

The SPASE metadata is not updated frequently. There are instances where variables have been added to CDAWeb datasets but the SPASE records do not have them. In some instances, CDAWeb `NumericalData` SPASE records are missing variables even for datasets that have not changed. Examples are given in the `Parameter` subsection.

The implication is that a scientist who executes a search backed by CDAWeb `NumericalData` SPASE records may erroneously conclude that variables or datasets are not available.

## 3 Units

We considered using SPASE `Units` for variables when they were available because although CDAWeb Master metadata has a `UNITS` attribute, no consistent convention is followed for the syntax and in some cases, `UNITS` are not a scientific unit but a label (e.g. `0=good` and `<|V|>`). This effort stopped when we noticed instances where the SPASE `Units` were wrong.

For example, `AC_H2_ULE/unc_H_S1`, has `UNITS = '[fraction]'` in the [CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/ac_h2_ule_00000000_v01.json) and `Units = '(cm^2 s sr MeV)^-1)'` [in SPASE](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json). See [a dump of the unique Master `UNITS` to SPASE `Units` pairs](https://github.com/rweigel/cdawmeta-additions/query/query-units.json), which is explained in [query-units.md](https://github.com/rweigel/cdawmeta-additions/query/query-units.md).

The implication is a scientist using SPASE `Units` to label their plots risks the plot being incorrect.

There was a second complicating factor. Some CDAWeb `NumericalData` SPASE records do not have `Parameters` for all `VAR_TYPE=data`.

Although there is more consistency in the strings used for SPASE `Units`, SPASE does not require the use of a standard for the syntax (such as [VOUnits](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html), [udunits2](https://docs.unidata.ucar.edu/udunits/current/#Database), or [QUDT](http://qudt.org/vocab/unit/)). HAPI has the option to state the standard used for `unit` strings so that a validator can check and units-aware software (e.g., the [AstroPy Units](https://eteq.github.io/astropy/units/index.html) module) can use it to make automatic unit conversions when mathematical operations on are performed.

We concluded that if we wanted to represent CDAWeb variables in HAPI with units that adhered to a syntax so the string could be validated, we would need to:

1. Determine the [VOUnit](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html) representation of all unique units (~1000), if possible. See [CDFUNITS_to_VOUNITS.csv](https://github.com/rweigel/cdawmeta-additions/CDFUNITS_to_VOUNITS.csv).

2. Determine the [VOUnit](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html) for all variables that do not have a `UNITS` attribute or a `UNITS` value that is all whitespace (~20,000), which we label as "missing"; see [Missing_UNITS.json](https://github.com/rweigel/cdawmeta-additions/Missing_UNITS.json). Although the ISTP conventions require units for variables with `VAR_TYPE = data`, ~20% of variables have "missing" `UNITS`.

3. Validating determinations made for 1. and 2. are correct. This could done in two ways: (a) Have two people independently make determinations and (b) for case 1., use AstroPy to compute the SI conversion and compare with the `SI_{conversion,conv,CONVERSION}` (all three versions are found in [CDF Masters](http://mag.gmu.edu/git-data/cdawmeta/data/table/cdaweb.variable.attribute_counts.csv) and the ISTP convention documentation) attribute value in the CDF. I emphasize that results must be checked and verified. Putting incorrect units in metadata is unacceptable.

Finally, I think that the correct source of the updated units is not SPASE—it should be the CDF Masters; SPASE records should draw this information from the CDF Masters. Many people use CDF Masters for metadata, and if the VOUnits only existed in SPASE, they would have access to them. (For example, [CDAWeb](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval1.cgi?index=sp_phys&group=ACE) links to the Master file in the Metadata links and Autoplot, HAPI, etc. used Master CDF metadata.)

## 4 `AccessInformation`

The `AccessInformation` nodes are structured in a way that is misleading and clarification is needed.

1. For example, [ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json
 ) indicates that the `Format` for the first four `AccessURL`s is `CDF`, which not correct. The `Name=CDAWeb` access URL has has many other format options. The `SSCWeb` access URL does not provide `CDF` and the names of the parameters at SSCWeb are not the same as those listed in `Parameters`; and more parameters are available from SSCWeb. Finally, `CSV` is listed as the format for the `Style=HAPI` `AccessURL`, but `Binary` and `JSON` are available.

   Note that Bernie Harris has a web service that that produces SPASE records with additional `AccessInformation` nodes, for example compare

   * [his ACE/Ephemeris/PT12M](https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M) with
    * [hpde.io ACE/Ephemeris/PT12M](https://hpde.io/ASA/NumericalData/ACE/Ephemeris/PT12M.json)

    I don't know Bernie's web service it is being used - although it is a service endpoint for `heliophysicsdata.gsfc.nasa.gov/`, it seems to not be used - for example, see the search result for [AC_OR_SSC](https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=AC_OR_SSC).

2. There are CDAWeb SPASE `NumericalData` records with the ACE Science Center listed as an `AccessURL` (e.g., [ACE/MAG/L2/PT16S](https://hpde.io/NASA/NumericalData/ACE/MAG/L2/PT16S.json)). The variable names used in the ACE Science Center files and metadata differ from those listed in the `Parameter` node. This is confusing.

## 5 `Parameter` content

CDAWeb datasets may have variables with different `DEPEND_0s`, and the `DEPEND_0` may have a different cadence. For example, `VOYAGER1_10S_MAG` has two `DEPEND_0s`:

* [`Epoch2`, with cadence `PT9.6S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader) and
* [`Epoch` with cadence `PT48S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader).

SPASE records can only have a cadence that applies to all parameters, but the listed cadence in the SPASE record for [`VOYAGER1_10S_MAG` is `PT9.6S`](https://hpde.io/NASA/NumericalData/Voyager1/MAG/CDF/PT9.6S.json) is not correct for the parameters with a `DEPEND_0` of `Epoch`. This is inaccurate and misleading and potentially confusing for the user who requests data for a parameter an expect a cadence based on the metadata and finds something quite different. The fact that some parameters have a different cadence should be noted in the SPASE record. Note that one could use `Cadence{Min,Max}`, but this is potentially confusing if the primary `DEPEND_0` has a fixed cadence.

It is often found that SPASE records contain information that is only available from one of the `AccessURLs`. For example,

* [ACE/MAG/KeyParameter/PT1H](https://hpde.io/NASA/NumericalData/ACE/MAG/KeyParameter/PT1H) references `Time PB5`. This is only available in the raw CDF files. No other `AccessURL`s provide access to it.

* [ACE/MAG/L2/PT16S](https://hpde.io//NASA/NumericalData/ACE/MAG/L2/PT16S)

  * references a [CDAWeb page](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval2.cgi?dataset=AC_H0_MFI&index=sp_phys) that has different names `B-field magnitude` vs. `Bmagnitude` and `Magnitude` in SPASE. Why?
  * `sigmaB` is mentioned at Caltech pages, not in SPASE.
  * What is the relationship between the Caltech data and CDAWeb data? Which should I use?
  * `Time PB5` is listed in SPASE record, it is not available from 4 of the 5 `AccessURL`s:

      ✓ https://spdf.gsfc.nasa.gov/pub/data/ace/mag/level_2_cdaweb/mfi_h0/

      X https://cdaweb.gsfc.nasa.gov/cgi-bin/eval2.cgi?dataset=AC_H0_MFI&index=sp_phys

      X https://cdaweb.gsfc.nasa.gov/hapi

      X ftp://mussel.srl.caltech.edu/pub/ace/level2/mag/

      X https://izw1.caltech.edu/ACE/ASC/level2/lvl2DATA_MAG.html

* In [OMNI/PT1H](https://hpde.io/NASA/NumericalData/OMNI/PT1H),

   * columns are referenced, but this does not apply to all `AccessURL`s,
   * the table is not same as shown in [omni2.text](https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2.text) (new annotations added), and
   * SPASE does not reference column 55, which is mentioned in [omni2.text](https://spdf.gsfc.nasa.gov/pub/data/omni/low_res_omni/omni2.text).

This is a complicated problem. We are considering also serving CDF data of type `VAR_DATA=support_data`. In this case, the HAPI metadata will reference many more parameters than found in SPASE and the SPASE record will be inconsistent with the HAPI metadata.

## 6 Odd differences in text

PIs writing has been modified (I'm assuming PI did not request the SPASE `Description` to be a modified version of what is in the CDF):

[`TEXT` node in the CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/uy_proton-moments_swoops_00000000_v01.json)

> This file contains the moments obtained from the distribution function of protons after deconvolution using the same magnetic field values used to construct the matrices. The vector magnetic field and the particle velocity are given in inertial RTN coordinates. ...

[`Description` node in the corresponding SPASE record](https://hpde.io/NASA/NumericalData/Ulysses/SWOOPS/Proton/Moments/PT4M)

> This File contains the Moments obtained from the Distribution Function of Protons after Deconvolution using the same Magnetic Field Values used to construct the Matrices. The Vector Magnetic Field and the Particle Velocity are given in Inertial RTN Coordinates. ...

My opinion is that only in rare circumstances should information not in all.xml, the Master CDF, a paper, or the PIs web page, or written by someone on the instrument team be in SPASE without. Also, when content is taken from papers and web pages, it should be indicated.

## 7 `StopDate`

The `StopDate`s are relative even though the actual stop date is available in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml). Given that many SPASE records have not been updated in years, it is likely that the relative `StopDate` is wrong in some cases (due to, for example, no more data being produced).

## 8 Conclusion

Although HAPI has an `additionalMetadata` attribute, we are reluctant to reference existing SPASE records due to these issues (primarily 2., 3., and 5.). We conclude that it makes more sense to link to less extensive but correct metadata (for example, to CDF Master metadata or documentation on the CDAWeb website<sup>*</sup>, than to more extensive SPASE metadata that is confusing (see 4.) or incomplete and in some cases incorrect (see items 2., 3., and 5.).

<sup>*</sup> Note that CDAWeb links to incorrect SPASE records.

However, CDAWeb includes links to SPASE records that

* are wrong. For example, [NotesA.html#AC_H2_SIS](https://cdaweb.gsfc.nasa.gov/misc/NotesA.html#AC_H2_SIS) links to [ACE/ULEIS/Ion/Fluxes/L2/PT1H.json](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json) which, as discussed earlier, has wrong units for some variables. SPASE records with missing parameters are also linked to.
* are missing information. For example, [NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) has a link to [OMNI/HighResolutionObservations/Version2/PT1M](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M), which has [a broken link](https://omniweb..sci.gsfc.nasa.gov/html/HROdocum.html) (it is likely that the broken link was corrected in the CDF metdata and the SPASE record was not updated). The documentation at [NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) could use improvement and the SPASE record has some improvements but at the cost of being outdated. Why not improve the source?

The primary problem with existing CDAWeb `NumericalData` SPASE records is

1. that they appear to have been created ad-hoc by different authors who follow different conventions and include different levels of detail, and
2. there is no automated mechanism for updating or syncing the SPASE records with CDAWeb metadata.

The problem _is not_ the difficulty in writing `NumericalData`. These should be primarily auto-generated. Having people write them by copy/paste of existing CDAWeb metadata ensures errors and discrepancies will be introduced and that they will be soon out-of-date. There is now in edit option for SPASE `NumericalData` records at https://hpde.io/ ([example](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M)). However, `NumericalData` records should be auto-generated and any additional information should be stored in an external, version controlled file. This file should be edited and daily updates of `NumericalData` records should draw from the file.

CDAWeb SPASE `NumericalData` records have been under development since 2009 and yet these problems persist. At their rate of generation, they will not be complete until 2030. I suggest a different approach is needed.

After encountering the issues described in parts 1.-3. of this section, we realized that solving all of the problems could be achieved with some additions to the existing CDAWeb to HAPI metadata code and the the creation of a table that contains metadata that does not exist, and is not desired to be in, CDAWeb metadata.

We suggest that CDAWeb SPASE metadata should be created by this automated process, which requires primarily existing CDAWeb metadata information and some additional metadata that can be stored in version controlled tables.

1. The primary additional table has columns of CDAWeb dataset ID, SPASE ID, DOI, Region, InstrumentID, and MeasurementType. Also, if the `Description` derived from all.xml is not acceptable, a column with an alternative description could be included (however, it seems to make more sense to modify the content of all.xml - why have two versions of the same thing?)

2. A table that contains additional `InformationURLs` (however, all.xml already has the equivalent, so perhaps additional URLs should be added in it).

3. Optionally, there could be a table that maps CDF `UNITS` to `VOUnits` for use in SPASE `Units`.

Some SPASE records have additional details about CDAWeb variables that the automated process does not capture. For example, `Qualifier`, `RenderingHints`, `CoordinateSystem`, `SupportQuantity`, and `Particle`, `Field`, etc. This could also be addressed by a table that has this information. However, there are over ~100,000 CDAWeb variables and the search use case for much of this information is not clear; not having this information should not prevent the 15-year effort to create correct and up-to-date SPASE `NumericalData` records. That is, if only 10% of SPASE records include this information, a search on it will not be useful. I would argue that `CoordinateSystem` is important, but why not add it to the CDF Masters so that the users of it have access?. Also, some of the additional metadata is not useful for search, such as `Valid{Min,Max}`, `FillValue`, and `RenderingHints`. This information would be useful if the SPASE record was being used for automated extraction and plotting. However, much more information is needed to enable automated extraction and even then (as we found with attempts to use SPASE for HAPI), given the issues described above, this may not be possible.

# Installing and Running

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta;
pip install -e .
```

In the following, use `--update` to update the input metadata (source data changes on the order of days).

See `python metadata.py --help` for more options, which include generation by an `id` regular expression.

**Examples**

Create and display all metadata types (see `python metadata.py --help` for a list)
```
python metadata.py --id AC_OR_SSC
```

Create and display HAPI metadata; the output of this command can be viewed at
[hapi/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info/AC_OR_SSC.json)
```
python metadata.py --id AC_OR_SSC --meta-type hapi
```

Create and display proof-of-concept SOSO; the output of this command can be viewed at
[soso/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/soso/info/AC_OR_SSC.json)
```
python metadata.py --id AC_OR_SSC --meta-type soso
```

Create and display proof-of-concept auto-generated SPASE; the output of this command can be viewed at
[spase_auto/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/AC_OR_SSC.json). See the `_Note` for context.
```
python metadata.py --id AC_OR_SSC --meta-type spase_auto
```