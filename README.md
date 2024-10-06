# `cdawmeta` version `0.0.1`

# About

This package uses [CDAWeb's](https://cdaweb.gsfc.nasa.gov) metadata to create HAPI `catalog` and `info` metadata and SPASE `NumericalData` metadata.

It was originally developed to upgrade the metadata from CDAWeb's HAPI server (the existing server only includes the minimum required metadata).

As discussed in the [SPASE](#SPASE) section, the code was extended to remedy issues with existing SPASE `NumericalData` metadata for CDAWeb datasets.

The code reads and combines information from

* [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which has dataset-level information for ~2,500 datasets;
* The [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/) files (we use the JSON representation) referenced in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which contain both  dataset-level metadata and variable metadata; 
* The list of URLs associated with all CDF files associated with a dataset using the CDASR [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) endpoint; and
* A CDF file referenced in the [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) response (for computing cadence and determining if the variables in the Master CDF match those in a data CDF).

Comments on issues with CDAWeb metadata are in the [CDAWeb](#CDAWeb) section.

As discussed in the [SPASE](#SPASE) section, we abandoned our attempt to use existing SPASE records.

The code uses [requests-cache](https://github.com/requests-cache/requests-cache/), so files are only re-downloaded if the HTTP headers indicate they are needed. When metadata are downloaded, a diff is stored if they changed.

The output is

1. HAPI metadata, which is based on [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata. (The cadence of the measurements for each dataset by sampling the last CDF file associated with the dataset and a histogram of the differences in timesteps.) This HAPI metadata is available in JSON in [hapi/info](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info))

2. Proof-of-concept SPASE records that do not have most of the major issues described in [SPASE](#SPASE) section below (These SPASE records are available in JSON in [spase_auto/info](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info)).

In addition, we have developed several tools for inspection and debugging. SQL databases are available with a search interface for

   * [CDAWeb datasets-level information](https://hapi-server.org/meta/cdaweb/dataset/), which is based on content stored in [all.xml](http://mag.gmu.edu/git-data/cdawmeta/data/allxml) and [Masters CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * [CDAWeb variable-level information](https://hapi-server.org/meta/cdaweb/variable/), which is based on content stored in [Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * `hpde.io` [SPASE dataset-level information](https://hapi-server.org/meta/spase/dataset/), which is based on content non-`Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * `hpde.io` [SPASE parameter-level information](https://hapi-server.org/meta/spase/parameter/), which is based on content `Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * [HAPI datasets-level information](https://hapi-server.org/meta/hapi/parameter/) (from the old and new server), which is based on the non-`parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)
   * [HAPI parameter-level information](https://hapi-server.org/meta/hapi/parameter/) (from the old and new server), which is based on the `parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)

Also, demonstration code for placing SPASE records into a MongoDB and executing a search is available in `query.py`.

# Installing and Running

(Formal unit tests using `pytest` are in development.)

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta;
pip install -e .
# Test commands in README. Should run without a stack trace (errors shown in
# red are encountered metadata errors).
make test-README
```

In the following, use `--update` to update the input metadata (source data changes on the order of days, typically in the mornings Eastern time on weekdays).

See `python metadata.py --help` for more options, including the generation of metadata for only `id`s that match a regular expression and skipping `ids`.

**Examples**

Create and display proof-of-concept auto-generated SPASE; the output of this command can be viewed at
[spase_auto/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/AC_OR_SSC.json) and [spase_auto/info/VOYAGER1_10S_MAG.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/VOYAGER1_10S_MAG.json). See the [`cdawmeta-spase` repository](https://github.com/rweigel/cdawmeta-spase) for metadata used that is not available in Master CDFs and/or `all.xml`.
```
mkdir -p ./data;
python metadata.py --id AC_OR_SSC --meta-type spase_auto
python metadata.py --id VOYAGER1_10S_MAG.json --meta-type spase_auto
```

Create and display HAPI metadata; the output of this command can be viewed at [hapi/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info/AC_OR_SSC.json) and [hapi/info/VOYAGER1_10S_MAG.json](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info/VOYAGER1_10S_MAG.json):
```
python metadata.py --id AC_OR_SSC --meta-type hapi
python metadata.py --id VOYAGER1_10S_MAG.json --meta-type hapi
```

Create and display proof-of-concept SOSO; the output of this command can be viewed at
[soso/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/soso/info/AC_OR_SSC.json)
```
python metadata.py --id AC_OR_SSC --meta-type soso
```

**Advanced Examples**

Insert SPASE documents for CDAWeb dataset names that match `^A` into a MongoDB and execute a query (requires [installation of MongoDB](https://www.mongodb.com/docs/manual/installation/) to `~/mongodb`):
```
python query.py --port 27018 --id '^A' \
 --filter '{"NumericalData.Parameter": { "$exists": true }}' \
 --mongod-binary ~/mongodb/bin/mongod # Change path as needed
# 45 documents found
# 40 documents match query
```

Create a report based on content of the [hpde.io repository](https://github.com/hpde/hpde.io). (Sample output in [`cdawmeta-spase/reports`](https://github.com/rweigel/cdawmeta-spase/tree/main/reports)). This file also builds the input files used for the automatic generation of SPASE records, which is described in the [`cdawmeta-spase` README](https://github.com/rweigel/cdawmeta-spase/).
```
python report.py --report-name hpde_io
```

<a id="CDAWeb"></a>

# CDAWeb

CDAWeb provides access to metadata used for its data services in the form of an [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/). Their software engineers have provided essential guidance and insight into the development of HAPI metadata.

Although CDF files uploaded to or pulled into CDAWeb from instrument teams typically are roughly compliant with their [ISTP metadata guidelines](https://spdf.gsfc.nasa.gov/istp_guide/istp_guide.html), there is a variability in the level of compliance. In many cases, "patches" to these CDF files are needed for the CDAWeb display and listing software to work. To address this, they create "Master CDFs". In addition, CDAWeb data service-specific metadata, such as plot rendering information used by their [IDL processing code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/), is included. Also, "virtual" variables used by the CDAWeb plotting software are often added. For example, suppose a variable that depends on time, energy, and pitch angle is in the dataset CDFs. In that case, they may add one variable per pitch angle by defining a "virtual" variables. The code need to produce a virtual variable is defined in [IDL code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/virtual_funcs.pro).

The Master CDFs are posted for external use, with caveats. From [0MASTERS/00readme.txt](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/00readme.txt):

> The following collections of Master CDF files were generated from a single data CDF or netCDF, for each dataset, for use in the  CDAWeb system (https://cdaweb.gsfc.nasa.gov).
>
> They are provided to the public for easier viewing/searching the metadata and quantities available in the data sets.
>
> In many cases the Master CDF is changed to improve the metadata in the original data files (especially to improve their compliance with the ISTP Metadata Guidelines), and often to add CDAWeb-specific metadata and addition plotting capabilities.
>
> Since the Master files are created using skeletontable/skeletoncdf tools from a data file and not necessarily reviewed and edited (especially for historical datasets), THEY SHOULD BE USED WITH CAUTION.

In attempting to create HAPI metadata from CDF Master, several issues were encountered, which have been posted to this repository [issue tracker](https://github.com/rweigel/cdawmeta/issues); many others were handled over email.

We suggest that the community would benefit if Master CDF metadata was improved. This would

1\. improve the quality of HAPI and SPASE metadata generated based on Master CDF metadata

2\. reduce duplication of effort by scientists and developers in handling non-compliance. For example,

* [pytplot](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L213) accounts for the fact that both `SI_CONVERSION` and `SI_CONV` are used as attributes in Master CDFs, but they missed `SI_conv`, which is [also found](https://github.com/rweigel/cdawmeta/issues/14).
* [pytplot](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L140) checks for only `DISPLAY_TYPE` but misses the fact that `Display_Type` and `DISPLAYTYPE` are also found in CDF Masters. The [CDAWeb IDL library](https://github.com/rweigel/cdawlib) [does not look for `DISPLAYTYPE`](https://github.com/search?q=repo%3Arweigel%2FCDAWlib%20DISPLAYTYPE&type=code) and neither does [ADAPT](https://github.com/search?q=repo%3Aspase-group%2Fadapt%20DISPLAY_TYPE&type=code). (Note that these links go to a personal repo with a copy of the CDAWeb IDL library, which is not available in a public repository that can be searched have files linked to by line.)
* [pytplot](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L158) accounts for `DEPEND_TIME` meaning the same thing as `DEPEND_0`. We missed this fact when developing HAPI metadata but could not find documentation to confirm it.
* [Autoplot/CdfUtil.java](https://github.com/autoplot/app/blob/master/CdfJavaDataSource/src/org/autoplot/cdf/CdfUtil.java) has worked around many CDF and Master CDF metadata issues. (See also [CdfVirtualVars.java](https://github.com/autoplot/app/blob/master/CdfJavaDataSource/src/org/autoplot/cdf/CdfVirtualVars.java)).
* The [CDAWeb HAPI server](https://git.smce.nasa.gov/spdf/hapi-nand/-/blob/main/src/java/org/hapistream/hapi/server/cdaweb/CdawebUtil.java?ref_type=heads) also contains workarounds.
* [The SPDF CDF Java library](https://github.com/autoplot/cdfj) (posted in this personal repo because it is not available in a public SPDF repo) catches some, but not all CDF metadata issues. For example, it catches `DEPEND_TIME`, but misses the fact that `Display_Type` and `DISPLAYTYPE` (it seems awkward for a CDF file format library to handle special metadata cases).
* In the early days of SPASE, Jan Merka was creating SPASE records using CDAWeb metadata, and he encountered many of the same issues we did (which we learned recently).
* The HAPI metadata generation code addresses many anomalies. See the files in the [attrib directory](https://github.com/rweigel/cdawmeta/tree/main/cdawmeta/attrib) and [hapi.py](https://github.com/rweigel/cdawmeta/blob/main/cdawmeta/_generate/hapi.py). [Logs](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/) of issues encountered that affected HAPI metadata generation encountered is generated by this code. These issues are tracked in the [cdawmeta issue tracker](https://github.com/rweigel/cdawmeta/issues), and we add information conveyed to us via email or on telecons.

We also recommend

* documentation of known issues and suggested workarounds - many developers who have re-discover issues, or missed issues, would benefit;
* a publicly visible issue tracker, and encouragement by the community to use it, for CDAWeb metadata (the [cdawmeta issue tracker](https://github.com/rweigel/cdawmeta/issues) serves this purpose now). Although CDAWeb is responsive to many reports on errors in Master CDFs, we have found many developers have encountered the same issues and workarounds and have not reported them. With such a tracker, other developers would benefit from accumulated knowledge of issues, and for issues that will not be fixed, they will benefit from the discussion on how to fully work around an issue;
* documentation of non-ISTP attributes so that users know if an attribute is important for interpretation; and
* a clearer indication of, or documentation about, attributes that are CDAWeb-software specific and the more general attributes.

Early indications are that much of this is out-of-scope of the CDAWeb project. For example, CDAWeb does not control the content or quality of the files that they host and improving the metadata for use by non-CDAWeb software is not supported. However, addressing these issues will greatly impact the quality of code and metadata downstream; if it is out-of-scope, leadership should find support for addressing these perennial issues.

<a id="SPASE"></a>

# SPASE

Our initial attempt was to generate HAPI metadata with SPASE records.

The primary issues related to HAPI are the first three. The others were noted in passing.

## 1 Completion

Only about 40% of CDAWeb datasets had parameter-level SPASE records when we first considered using them for HAPI metadata in 2019. ~5 years later, there is ~70% coverage (however, as discussed below, the number that are up-to-date and correct and and without missing parameters is less).

The implication is that CDAWeb `NumericalData` SPASE records cannot be used for one of the intended purposes, which is to provide a structured, correct, and complete representation of CDAWeb metadata; we needed to duplicate much of the effort that went into creating CDAWeb SPASE records in order to create a complete set of HAPI metadata.

## 2 Updates

The SPASE metadata is not updated frequently. There are instances where variables have been added to CDAWeb datasets but the SPASE records do not have them. Sometimes, SPASE records are missing variables, even for datasets that have not changed since the SPASE records were created. Examples are given in the `Parameter` subsection.

The implication is that a scientist who executes a search backed by SPASE records may erroneously conclude that variables or datasets are unavailable.

## 3 Units

We considered using SPASE `Units` when they were available because although CDAWeb Master metadata has a `UNITS` attribute, no consistent convention is followed for the syntax, and in some cases, `UNITS` are not a scientific unit but a label (e.g. `0=good` and `<|V|>`). This effort stopped when we noticed instances where the SPASE `Units` were wrong.

For example, `AC_H2_ULE/unc_H_S1`, has `UNITS = '[fraction]'` in the [CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/ac_h2_ule_00000000_v01.json) and `Units = '(cm^2 s sr MeV)^-1)'` [in SPASE](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json). See also [a dump of the unique Master `UNITS` to SPASE `Units` pairs](https://github.com/rweigel/cdawmeta-spase/blob/main/reports/units-CDFUNITS_to_SPASEUnit-map), which is explained in [units.md](https://github.com/rweigel/cdawmeta-spase/blob/main/reports/units.md). (Note that CDAWeb [includes a link to this SPASE record](https://cdaweb.gsfc.nasa.gov/misc/NotesA.html#AC_H2_ULE) and [elsewhere](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval1.cgi?index=sp_phys&group=ACE) to [a SKT file](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/ac_h2_ule_00000000_v01.skt) with different units.)

There was a second complicating factor. Some SPASE records do not have `Parameters` for all variables with `VAR_TYPE=data`. So we would need to use SPASE `Units` when available and otherwise use CDF Master units otherwise.

Although there is more consistency in the strings used for SPASE `Units`, SPASE does not require the use of a standard for the syntax (such as [VOUnits](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html), [udunits2](https://docs.unidata.ucar.edu/udunits/current/#Database), or [QUDT](http://qudt.org/vocab/unit/)). HAPI has the option to state the standard used for `unit` strings so that a validator can check them and units-aware software (e.g., the [AstroPy Units](https://eteq.github.io/astropy/units/index.html) module) can use them to make automatic unit conversions when mathematical operations on are performed.

We concluded that if we wanted to represent CDAWeb variables in HAPI with units that adhered to a syntax so the string could be validated, we would need to take the steps described in the [`cdawmeta-spase` repository]([`cdawmeta` repository README](https://github.com/rweigel/cdawmeta).)

## 4 `AccessInformation`

The `AccessInformation` nodes are structured in a way that is misleading and clarification is needed.

1. For example, [ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json) indicates that the `Format` for the first four `AccessURL`'s is `CDF`, which not correct. The `Name=CDAWeb` access URL has has many other format options. The `SSCWeb` access URL does not provide `CDF` and the names of the parameters at SSCWeb are not the same as those listed in `Parameters`; also, more parameters are available at SSCWeb. Finally, `CSV` is listed as the format for the `Style=HAPI` `AccessURL`, but `Binary` and `JSON` are available.

   Note that Bernie Harris has a web service that produces SPASE records with additional `AccessInformation` nodes, for example compare

   * [his ACE/Ephemeris/PT12M](https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M) with
   * [hpde.io ACE/Ephemeris/PT12M](https://hpde.io/ASA/NumericalData/ACE/Ephemeris/PT12M.json)

   I don't know Bernie's web service it is being used - although it is a service endpoint for [heliophysicsdata.gsfc.nasa.gov](heliophysicsdata.gsfc.nasa.gov), it seems to not to be used there - for example, see the search result for [AC_OR_SSC](https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=AC_OR_SSC).

2. There are CDAWeb SPASE `NumericalData` records with the ACE Science Center listed as an `AccessURL` (e.g., [ACE/MAG/L2/PT16S](https://hpde.io/NASA/NumericalData/ACE/MAG/L2/PT16S.json)). The variable names used in the ACE Science Center files and metadata differ from those listed in the `Parameter` node. This is confusing.

In the [`cdawmeta-spase` repository](https://github.com/rweigel/cdawmeta), we have a template that addresses some of these issues. This template is used to generate the `spase_auto` metadata described above.

It is often found that SPASE records contain information that is only available from one of the `AccessURLs`. For example,

* [ACE/MAG/KeyParameter/PT1H](https://hpde.io/NASA/NumericalData/ACE/MAG/KeyParameter/PT1H) references `Time PB5`. This is only available in the raw CDF files. No other `AccessURL's provide access to it.

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

This is a complicated problem. We are considering also serving CDF data of type `VAR_DATA=support_data`. In this case, the HAPI metadata will reference many more parameters than found in SPASE the SPASE record if it only lists variables available from the CDAWeb we services and the SPASE record will be inconsistent with the HAPI metadata.

## 5 `Parameter` content

Many SPASE records do not contain the full list of variables in a CDAWeb dataset. This issue was apparently noticed before - Bernie Harris has a [resolver](https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html) that will create a SPASE record with the correct variables.

For example, [NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) has a link to [OMNI/HighResolutionObservations/Version2/PT1M](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M), which has [a broken link](https://omniweb..sci.gsfc.nasa.gov/html/HROdocum.html) (it is likely that the broken link was corrected in the CDF metdata and the SPASE record was not updated). Why not improve the source and derive SPASE metadata from the source? Having two independent versions of the same thing often leads to a divergence in content, as was the case here. This is one of the reasons that the `spase_auto` code uses `all.xml` or Master CDF metadata in favor of SPASE content if they both contain similar information. Note that the parameter list generated by this process may differ from what is generated by the [resolver](https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html). We start with the full list of parameters but drop certain ones if there are issues with the metadata or CDF files that will prevent the data from being served. The list of dropped parameters is available in a [log file](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/). (It is straightforward to modify this behavior, however). Note that this issue is related to the "complicated problem" issue described in the `AccessInformation` subsection.

CDAWeb datasets may have variables with different `DEPEND_0s`, and the `DEPEND_0` may have a different cadence. For example, `VOYAGER1_10S_MAG` has two `DEPEND_0s`:

* [`Epoch2`, with cadence `PT9.6S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader) and
* [`Epoch` with cadence `PT48S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader).

However, the [SPASE record](https://hpde.io/NASA/NumericalData/Voyager1/MAG/CDF/PT9.6S.json) for this dataset, which is linked to from [CDAWeb](https://cdaweb.gsfc.nasa.gov/misc/NotesV.html#VOYAGER1_10S_MAG), lists both of these variables as having a cadence of `PT9.6S`. The `spase_auto` metadata described above addresses this issue.

The [Resource ID convention](https://spase-group.org/docs/conventions/index.html) suggests putting cadence in the `ResourceID` string. However, no convention is suggested for how the cadence is rendered. For example, should one day be given as `PT86400S` or `P1D`? No convention is suggested for the amount of precision to use. Our SPASE generation code computes the cadence of a dataset by computing the histogram of the difference in time step and the most frequent time step is used. We have found that this automated process often finds a cadence that does not match the cadence in the `ResourceID`.

## 6 Odd differences in text

PIs writing has been modified (I'm assuming PI did not request the SPASE `Description` to be a modified version of what is in the CDF):

[`TEXT` node in the CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/uy_proton-moments_swoops_00000000_v01.json)

> This file contains the moments obtained from the distribution function of protons after deconvolution using the same magnetic field values used to construct the matrices. The vector magnetic field and the particle velocity are given in inertial RTN coordinates. ...

[`Description` node in the corresponding SPASE record](https://hpde.io/NASA/NumericalData/Ulysses/SWOOPS/Proton/Moments/PT4M)

> This File contains the Moments obtained from the Distribution Function of Protons after Deconvolution using the same Magnetic Field Values used to construct the Matrices. The Vector Magnetic Field and the Particle Velocity are given in Inertial RTN Coordinates. ...

As a result, the `spase_auto` metadata used description from the Master CDF files. (Our opinion is that only in rare circumstances should information not in all.xml, the Master CDF, a paper, or the PIs web page, or written by someone on the instrument team be in SPASE. Also, when content is taken from papers and web pages an put in SPASE, it should be referenced.)

## 7 `StopDate`

The `StopDate`s are relative even though the actual stop date is available in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml). Given that many SPASE records have not been updated in years, it is likely that the relative `StopDate` is wrong in some cases (due to, for example, no more data being produced).

The `spase_auto` metadata described above addresses this issue and updates `StopDates` daily.

## 9 Inconsistent `ObservedRegion`s

Most CDAWeb datasets with ids in the form `a_b_c` should have the same `Region` as a dataset that starts with `a_y_z` (unless an instrument was not active while the spacecraft was in certain regions). This is frequently not the case; see the error messages in [hpde_io.log](https://github.com/rweigel/cdawmeta-spase/blob/main/reports/hpde_io.log).

For example
```
VOYAGER1_48S_MAG-VIM: ['Heliosphere.Outer', 'Heliosphere.Heliosheath']
VOYAGER2_PLS_COMPOSITION: ['Jupiter.Magnetosphere']
```

This issue is corrected in the `spase_auto` code and described in the [`cdawmeta-spase`](https://github.com/rweigel/cdawmeta-spase/) repository.

## 9 Inconsistent `InformationURL`s

[InformationURL.json](https://github.com/rweigel/cdawmeta-spase/blob/main/InformationURL.json) contains keys of a `URL` in an `InformationURL` node and an array with all CDAWeb datasets it is associated with. There are many instances where a URL should apply to additional datasets. For example, all dataset IDs that end in `_SSC`, `_DEF`, and `_POSITION` should be associated with https://sscweb.gsfc.nasa.gov. Also, the Master CDFs contain information URLs that do not appear in the associated SPASE `NumericalData` records. This represents an unnecessary loss of information.

## 10 Conclusion

Although HAPI has an `additionalMetadata` attribute, we are reluctant to reference existing SPASE records due to these issues (primarily 2., 3., and 5.). We conclude that it makes more sense to link to less extensive but correct metadata (for example, to CDF Master metadata or documentation on the CDAWeb website<sup>*</sup>) than to more extensive SPASE metadata that is confusing (see 4.) or incomplete and in some cases incorrect (see items 2., 3., and 5.).

<sup>*</sup> This is not quite possible - CDAWeb includes links to SPASE records that

The primary problems with existing CDAWeb `NumericalData` SPASE records is

1. that they appear to have been created ad-hoc by different authors who follow different conventions and include different levels of detail;
2. there is no automated mechanism for updating or syncing the SPASE records with CDAWeb metadata; and
3. there do not appear to be mechanisms in place to ensure the content of SPASE records is correct and consistent.

CDAWeb SPASE `NumericalData` records have been under development since 2009 and yet these problems persist. At the current rate of generation, they will not be complete until 2030. I suggest a different approach is needed.

We suggest that CDAWeb SPASE metadata should be created by this automated process (similar to how HAPI metadata is generated), which requires primarily existing CDAWeb metadata information and some additional metadata that can be stored in a few version controlled files. Thus information is described in the [cdawmeta-spase](https://github.com/rweigel/cdawmeta-addtions) repository. This approach would have prevented many of the errors and inconsistencies described above and further detailed in the [cdawmeta-spase README](https://github.com/rweigel/cdawmeta-addtions).

Another motivation for the urgency of having correct and complete SPASE `NumericalData` records is that there are several applications under development that will use SPASE records to provide search functionality. The quality of such applications is limited by the quality of the database it uses, and it is important that the database content is correct and consistent.