# `cdawmeta` version `0.0.1`

# About

This package uses [CDAWeb's](https://cdaweb.gsfc.nasa.gov) metadata to create HAPI `catalog` and `info`, SPASE `NumericalData`, and SOSO `dataset` metadata.

It was originally developed to upgrade the metadata from CDAWeb's HAPI server (the existing server only includes the minimum required metadata).

As discussed in the [SPASE](#SPASE) section, the code was extended to remedy issues with existing SPASE `NumericalData` metadata for CDAWeb datasets.

The code reads and combines information from

* [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which has dataset-level information about ~2,500 datasets;
* The [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/) files (we use the JSON representation) referenced in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which contain both  dataset-level metadata and variable metadata; and
* The list of URLs associated with all CDF files associated with a dataset using the CDASR [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) endpoint.
* A CDF file referenced in the [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) response (for computing cadence).

Comments on issues with CDAWeb metadata are in the [CDAWeb](#CDAWeb) section.

As discussed in the [SPASE](#SPASE) section, we abandoned our attempt to use existing SPASE records.

The code uses [requests-cache](https://github.com/requests-cache/requests-cache/), so files are only re-downloaded if the HTTP headers indicate they are needed. When metadata are downloaded, a diff is stored if they changed.

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

3. proof-of-concept [SOSO](https://github.com/ESIPFed/science-on-schema.org) metadata (see [soso/info](http://mag.gmu.edu/git-data/cdawmeta/data/soso)). This metadata is derived using [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/), and [HAPI](http://mag.gmu.edu/git-data/cdawmeta/data/soso) metadata. We did not use SPASE records because of the issues described in the [SPASE](#SPASE) section. (Deriving part of the SOSO metadata from HAPI metadata is not ideal and was done for proof-of-concept purposes. The code could be modified to remove the dependency on HAPI metadata and use the SPASE `NumericalData` metadata generated using code in this repository.)

and

4. proof-of-concept SPASE records that do not have most of the major issues described in [SPASE](#SPASE) section below (see [spase_auto/info](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto)).

# Installing and Running

Tested on Python 3.10.9

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta;
pip install -e .
```

In the following, use `--update` to update the input metadata (source data changes on the order of days, typically in the mornings Eastern time on weekdays).

See `python metadata.py --help` for more options, including the generation of metadata for only `id`s that match a regular expression.

**Examples**

Create and display proof-of-concept auto-generated SPASE; the output of this command can be viewed at
[spase_auto/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/AC_OR_SSC.json). See the [`cdawmeta` repository](https://github.com/rweigel/cdawmeta-additions) for metadata used that is not available in Master CDFs and/or `all.xml`.
```
mkdir -p ./data;
python metadata.py --id AC_OR_SSC --meta-type spase_auto

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

<a id="CDAWeb"></a>

# CDAWeb

CDAWeb provides access to metadata used for its data services in the form of an [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/). Their software engineers have provided essential guidance and insight into the development of HAPI metadata.

Although CDF files uploaded or pulled into CDAWeb from instrument teams typically are roughly compliant with their [ISTP metadata guidelines](https://spdf.gsfc.nasa.gov/istp_guide/istp_guide.html), there is a high variability. In many cases, "patches" to these CDF files are needed for the CDAWeb display and listing software to work. To address this, they create "Master CDFs,". In addition, CDAWeb data serice-specific metadata, such as plot rendering information used by their [IDL processing code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/), is included. Also, variables used by the CDAWeb plotting software are often added. For example, suppose a variable that depends on time, energy, and pitch angle is in the dataset CDFs. In that case, they may add one variable per pitch angle by defining a "VIRTUAL" variable, which is generated by a function defined in [IDL code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/virtual_funcs.pro).

The Master CDFs are posted for external use, with caveats. From [0MASTERS/00readme.txt](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/00readme.txt)

> The following collections of Master CDF files were generated from a single data CDF or netCDF, for each dataset, for use in the  CDAWeb system (https://cdaweb.gsfc.nasa.gov).
>
> They are provided to the public for easier viewing/searching the metadata and quantities available in the data sets.
>
> In many cases the Master CDF is changed to improve the metadata in the original data files (especially to improve their compliance with the ISTP Metadata Guidelines), and often to add CDAWeb-specific metadata and addition plotting capabilities.
>
> Since the Master files are created using skeletontable/skeletoncdf tools from a data file and not necessarily reviewed and edited (especially for historical datasets), THEY SHOULD BE USED WITH CAUTION.

We have found that man Master CDFs are not fully [ISTP convention compliant](https://spdf.gsfc.nasa.gov/istp_guide/istp_guide.html). Issues encountered have been posted to this repository [issue tracker](https://github.com/rweigel/cdawmeta/issues) and many others were handled over email.

We suggest that the community would benefit if Master CDF metadata was improved. This would

1. improve the quality of HAPI and SPASE metadata generated based on Master CDF metadata
2. reduce duplication of effort by scientists and developers in handling non-compliance. For example,
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
* an issue tracker, and encouragement by the community to use it, for CDAWeb metadata (the [cdawmeta issue tracker](https://github.com/rweigel/cdawmeta/issues) serves this purpose now). Although CDAWeb is responsive to many reports on errors in Master CDFs, we have found in discussions that many developers have encountered the same issues and workarounds and have not reported them. With such a tracker, other developers would benefit from accumulated knowledge of issues, and for issues that will not be fixed, they will benefit from the discussion on how to fully work around an issue; and
* documentation of non-ISTP attributes so that users know if an attribute is important for interpretation.
* a clearer indication of, or documentation about, attributes that are CDAWeb-software specific and the more general attributes.

Early indications are that much of this is out-of-scope of the CDAWeb project. For example, CDAWeb does not control the content or quality of the files that they host and improving the metadata for use by non-CDAWeb software is not supported. However, addressing these issues will greatly impact the quality of code and metadata downstream; if it is out-of-scope, leadership should find support for addressing these perennial issues.

<a id="SPASE"></a>

# SPASE

Our initial attempt was to generate HAPI metadata with SPASE records.

The primary issues related to HAPI are the first three. The others were noted in passing.

## 1 Completion

Only about 40% of CDAWeb datasets had SPASE records when we first considered using them for HAPI metadata in 2019. ~5 years later, there is ~70% coverage (however, as discussed below, the number of updated, correct, and without missing `Parameters` records is less). As a result, used metadata from [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/).

The implication is that CDAWeb `NumericalData` SPASE records cannot be used for one of the intended purposes, which is to provide a structured representation of CDAWeb metadata; we needed to duplicate much of the effort that went into creating CDAWeb SPASE records in order to create a complete set of HAPI metadata.

## 2 Updates

The SPASE metadata is not updated frequently. There are instances where variables have been added to CDAWeb datasets but the SPASE records do not have them. Sometimes, SPASE records are missing variables, even for datasets that have not changed. Examples are given in the `Parameter` subsection.

The implication is that a scientist who executes a search backed by SPASE records may erroneously conclude that variables or datasets are unavailable.

Note that Bernie Harris has a [resolver](https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html) that could be used to insert the most up-to-date dataset variables.

## 3 Units

We considered using SPASE `Units` for variables when they were available because although CDAWeb Master metadata has a `UNITS` attribute, no consistent convention is followed for the syntax and in some cases, `UNITS` are not a scientific unit but a label (e.g. `0=good` and `<|V|>`). This effort stopped when we noticed instances where the SPASE `Units` were wrong.

For example, `AC_H2_ULE/unc_H_S1`, has `UNITS = '[fraction]'` in the [CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/ac_h2_ule_00000000_v01.json) and `Units = '(cm^2 s sr MeV)^-1)'` [in SPASE](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json). See [a dump of the unique Master `UNITS` to SPASE `Units` pairs](https://github.com/rweigel/cdawmeta-additions/blob/main/reports/units-CDFUNITS_to_SPASEUnit-map), which is explained in [units.md](https://github.com/rweigel/cdawmeta-additions/blob/main/reports/units.md).

The implication is a scientist using SPASE `Units` to label their plots risks the plot being incorrect.

CDAWeb includes links to SPASE records with incorrect units. For example, [NotesA.html#AC_H2_SIS](https://cdaweb.gsfc.nasa.gov/misc/NotesA.html#AC_H2_SIS) links to 
[ACE/ULEIS/Ion/Fluxes/L2/PT1H.json](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json).

There was a second complicating factor. Some SPASE records do not have `Parameters` for all `VAR_TYPE=data`. So we would need to use SPASE `Units` when available and otherwise use CDF Master units otherwise.

Although there is more consistency in the strings used for SPASE `Units`, SPASE does not require the use of a standard for the syntax (such as [VOUnits](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html), [udunits2](https://docs.unidata.ucar.edu/udunits/current/#Database), or [QUDT](http://qudt.org/vocab/unit/)). HAPI has the option to state the standard used for `unit` strings so that a validator can check and units-aware software (e.g., the [AstroPy Units](https://eteq.github.io/astropy/units/index.html) module) can use it to make automatic unit conversions when mathematical operations on are performed.

We concluded that if we wanted to represent CDAWeb variables in HAPI with units that adhered to a syntax so the string could be validated, we would need to:

1. Determine the VOUnit representation of all unique units (~1000), if possible. See [CDFUNITS_to_VOUNITS.csv](https://github.com/rweigel/cdawmeta-additions/blob/main/CDFUNITS_to_VOUNITS.csv).

2. Determine the VOUnit for all variables that do not have a `UNITS` attribute or a `UNITS` value that is all whitespace (~20,000), which we label as "missing"; see [the log file](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/hapi.errors.ISTP.UNITS.log). Although the ISTP conventions require units for variables with `VAR_TYPE = data`, ~20% of variables have "missing" `UNITS`.

3. Validating determinations made for 1. and 2. are correct. This could done in two ways: (a) Have two people independently make determinations and (b) for case 1., use AstroPy to compute the SI conversion and compare with the `SI_{conversion,conv,CONVERSION}` (all three versions are found in [CDF Masters](http://mag.gmu.edu/git-data/cdawmeta/data/table/cdaweb.variable.attribute_counts.csv) and the [ISTP convention documentation](https://github.com/rweigel/cdawmeta/issues/14). I emphasize that results must be checked and verified. Putting incorrect units in metadata is unacceptable.

Finally, I think that the correct source of the updated units is not SPASE—it should be the CDF Masters; SPASE records should draw this information from the CDF Masters. Many people use CDF Masters for metadata, and if the VOUnits only existed in SPASE, they would have access to them. (For example, [CDAWeb](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval1.cgi?index=sp_phys&group=ACE) links to the Master file in the Metadata links and Autoplot, HAPI, etc. used Master CDF metadata.)

## 4 `AccessInformation`

The `AccessInformation` nodes are structured in a way that is misleading and clarification is needed.

1. For example, [ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json) indicates that the `Format` for the first four `AccessURL`'s is `CDF`, which not correct. The `Name=CDAWeb` access URL has has many other format options. The `SSCWeb` access URL does not provide `CDF` and the names of the parameters at SSCWeb are not the same as those listed in `Parameters`; and more parameters are available from SSCWeb. Finally, `CSV` is listed as the format for the `Style=HAPI` `AccessURL`, but `Binary` and `JSON` are available.

   Note that Bernie Harris has a web service that produces SPASE records with additional `AccessInformation` nodes, for example compare

   * [his ACE/Ephemeris/PT12M](https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M) with
   * [hpde.io ACE/Ephemeris/PT12M](https://hpde.io/ASA/NumericalData/ACE/Ephemeris/PT12M.json)

   I don't know Bernie's web service it is being used - although it is a service endpoint for [heliophysicsdata.gsfc.nasa.gov](heliophysicsdata.gsfc.nasa.gov), it seems to not be used - for example, see the search result for [AC_OR_SSC](https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=AC_OR_SSC).

2. There are CDAWeb SPASE `NumericalData` records with the ACE Science Center listed as an `AccessURL` (e.g., [ACE/MAG/L2/PT16S](https://hpde.io/NASA/NumericalData/ACE/MAG/L2/PT16S.json)). The variable names used in the ACE Science Center files and metadata differ from those listed in the `Parameter` node. This is confusing.

## 5 `Parameter` content

CDAWeb datasets may have variables with different `DEPEND_0s`, and the `DEPEND_0` may have a different cadence. For example, `VOYAGER1_10S_MAG` has two `DEPEND_0s`:

* [`Epoch2`, with cadence `PT9.6S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader) and
* [`Epoch` with cadence `PT48S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader).

However, the [SPASE record](https://hpde.io/NASA/NumericalData/Voyager1/MAG/CDF/PT9.6S.json), which is linked to from [CDAWeb](https://cdaweb.gsfc.nasa.gov/misc/NotesV.html#VOYAGER1_10S_MAG), lists both of these variables as having a cadence of `PT9.6S`.

The [Resource ID convention](https://spase-group.org/docs/conventions/index.html) suggests putting cadence in the `ResourceID` string. However, no convention is suggested for how the cadence is rendered. For example, should one
day be given as `PT86400S` or `P1D`? No convention is suggested for the amount of precision to use. Our SPASE generation code computes the cadence of a dataset by computing the histogram of the difference in time step and the most frequent time step is used. We have found that this automated process often finds a cadence that does not match the cadence in the `ResourceID`.

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

## 6 Odd differences in text

PIs writing has been modified (I'm assuming PI did not request the SPASE `Description` to be a modified version of what is in the CDF):

[`TEXT` node in the CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/uy_proton-moments_swoops_00000000_v01.json)

> This file contains the moments obtained from the distribution function of protons after deconvolution using the same magnetic field values used to construct the matrices. The vector magnetic field and the particle velocity are given in inertial RTN coordinates. ...

[`Description` node in the corresponding SPASE record](https://hpde.io/NASA/NumericalData/Ulysses/SWOOPS/Proton/Moments/PT4M)

> This File contains the Moments obtained from the Distribution Function of Protons after Deconvolution using the same Magnetic Field Values used to construct the Matrices. The Vector Magnetic Field and the Particle Velocity are given in Inertial RTN Coordinates. ...

My opinion is that only in rare circumstances should information not in all.xml, the Master CDF, a paper, or the PIs web page, or written by someone on the instrument team be in SPASE without. Also, when content is taken from papers and web pages, it should be indicated.

## 7 `StopDate`

The `StopDate`s are relative even though the actual stop date is available in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml). Given that many SPASE records have not been updated in years, it is likely that the relative `StopDate` is wrong in some cases (due to, for example, no more data being produced).

## 8 Allowing queries of content of hpde.io repository

At least four developers have written code to ingest the contents of the [`hpde.io` git repository](https://github.com/hpde/hpde.io/) to extract information. The contents of the `hpde.io` represent a database and as such should be stored in one (a source code repository is not a database). We suggest that (hpde.io)(https://hpde.io/) provide a query interface so that, e.g.,

> https:/hpde.io/?q={"Version": 2.4.1}

would be allowed, using, for example [MongoDB](https://mongodb.com/), [eXist-db](https://exist-db.org/), or [PostgreSQL](https://www.postgresql.org/). Note that Bernie Harris already runs an eXist database containing SPASE:

> ... it searches the spase documents for ones with //AccessInformation/AccessURL[name = ‘CDAWeb’ and ProductKey = ‘whatever’]. "Sufficiently described" meant that the cdaweb information was in the spase documents. At the time that code was written, there were many cdaweb datasets that didn't have spase descriptions or the spase descriptions didn't contain the cdaweb access information. Even now, spase is usually missing the most recent cdaweb datasets but it's not too far behind.
>
> This https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html describes a resolver service. To get all datasets at once, you might want to use https://heliophysicsdata.gsfc.nasa.gov/queries/CDAWeb_SPASE.html. Also note that https://cdaweb.gsfc.nasa.gov/WebServices/REST/#Get_Datasets returns the spase ResourceID.  For example,
>
>$ curl -s -H "Accept: application/json" "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets?idPattern=AC_H0_MFI" | jq -jr '.DatasetDescription[]|(.Id,", ",.SpaseResourceId,"\n")'

## 9 Inconsistent `ObservedRegion`

Each CDAWeb dataset in the form `a_b_c` should have the same `Region` as a dataset that starts with `a_y_z` (unless an instrument was not active while the spacecraft was in certain regions). This is frequently not the case; see the error messages in [hpde_io.log](https://github.com/rweigel/cdawmeta-additions/blob/main/reports/hpde_io.log).

For example
```
VOYAGER1_48S_MAG-VIM: ['Heliosphere.Outer', 'Heliosphere.Heliosheath']
VOYAGER2_PLS_COMPOSITION: ['Jupiter.Magnetosphere']
```

## 10 Inconsistent `InformationURL`s

[InformationURL.json](https://github.com/rweigel/cdawmeta-additions/blob/main/InformationURL.json) contains keys of a `URL` in an `InformationURL` node and an array with all CDAWeb datasets it is associated with. There are many instances where a URL should apply to additional datasets. For example, all dataset IDs that end in `_SSC`, `_DEF`, and `_POSITION` should be associated with https://sscweb.gsfc.nasa.gov. Also, the Master CDFs contain information URLs that do not appear in CDAWeb SPASE NumericalData records. This represents an unnecessary loss of information.

## 11 Conclusion

Although HAPI has an `additionalMetadata` attribute, we are reluctant to reference existing SPASE records due to these issues (primarily 2., 3., and 5.). We conclude that it makes more sense to link to less extensive but correct metadata (for example, to CDF Master metadata or documentation on the CDAWeb website<sup>*</sup>, than to more extensive SPASE metadata that is confusing (see 4.) or incomplete and in some cases incorrect (see items 2., 3., and 5.).

<sup>*</sup> This is not quite possible - CDAWeb includes links to SPASE records that

* are wrong. For example, [NotesA.html#AC_H2_SIS](https://cdaweb.gsfc.nasa.gov/misc/NotesA.html#AC_H2_SIS) links to [ACE/ULEIS/Ion/Fluxes/L2/PT1H.json](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json) which, as discussed earlier, has wrong units for some variables. SPASE records with missing parameters are also linked to.
* are missing information. For example, 
   1. [NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) has a link to [OMNI/HighResolutionObservations/Version2/PT1M](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M), which has [a broken link](https://omniweb..sci.gsfc.nasa.gov/html/HROdocum.html) (it is likely that the broken link was corrected in the CDF metdata and the SPASE record was not updated).
   2. The documentation at [NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) could use improvement and the SPASE record has some improvements but at the cost of being outdated. Why not improve the source?

The primary problem with existing CDAWeb `NumericalData` SPASE records is

1. that they appear to have been created ad-hoc by different authors who follow different conventions and include different levels of detail, and
2. there is no automated mechanism for updating or syncing the SPASE records with CDAWeb metadata.

The problem _is not_ the difficulty in writing `NumericalData`. These should be primarily auto-generated. Having people write them by copy/paste of existing CDAWeb metadata ensures errors and discrepancies will be introduced and that they will be soon out-of-date. There is now in edit option for SPASE `NumericalData` records at https://hpde.io/ ([example](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M)). However, `NumericalData` records should be auto-generated and any additional information should be stored in an external, version controlled file. This file should be edited and daily updates of `NumericalData` records should draw from the file.

CDAWeb SPASE `NumericalData` records have been under development since 2009 and yet these problems persist. At their rate of generation, they will not be complete until 2030. I suggest a different approach is needed.

After encountering the issues described in parts 1.-3. of this section, we realized that solving all of the problems could be achieved with some additions to the existing CDAWeb to HAPI metadata code and the the creation of a table that contains metadata that does not exist, and is not desired to be in, CDAWeb metadata.

We suggest that CDAWeb SPASE metadata should be created by this automated process, which requires primarily existing CDAWeb metadata information and some additional metadata that can be stored in a few version controlled. Thus information is described in the [cdawmeta-additions](https://github.com/rweigel/cdawmeta-addtions) repository. This approach would have prevented the errors and inconsistencies described above and further detailed in the [cdawmeta-additions README](https://github.com/rweigel/cdawmeta-addtions)

Note that not all existing content in `hpde.io` is yet used by the automated process. For example, some SPASE records have additional details about CDAWeb variables that the automated process does not use. For example, `Qualifier`, `RenderingHints`, `CoordinateSystem`, `SupportQuantity`, and `Particle`, `Field`, etc. This could also be addressed by a table that has this information. However, there should be a discussion of this; there are over ~100,000 CDAWeb variables, and the search use case for much of this information is not clear; not having this information should not prevent the 15-year effort to create correct and up-to-date SPASE `NumericalData` records. That is, if only 10% of SPASE records have a given attribute, a search on it will not be useful. Also, some of the not-yet used metadata is not useful for search, such as `Valid{Min,Max}`, `FillValue`, and `RenderingHints`. This information would be useful if the SPASE record was being used for automated extraction and plotting. However, much more information is needed to enable automated extraction and even then (as we found with attempts to use SPASE for HAPI), given the issues described above, this may not be possible or too time-consuming. If it were possible, an application that uses it and could test the results should be identified first. As noted above, metadata that is not used by an application is likely to have problems.