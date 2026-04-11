<!-- TOC -->
[1 CDAWeb](#1-cdaweb)<br/>
&nbsp;&nbsp;&nbsp;[1.1 Overview](#11-overview)<br/>
&nbsp;&nbsp;&nbsp;[1.2 Issues](#12-issues)<br/>
&nbsp;&nbsp;&nbsp;[1.3 Conclusion and Recommendations](#13-conclusion-and-recommendations)<br/>
[2 SPASE](#2-spase)<br/>
&nbsp;&nbsp;&nbsp;[2.1 Overview](#21-overview)<br/>
&nbsp;&nbsp;&nbsp;[2.2 Issues](#22-issues)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.1 Completion](#221-completion)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.2 Updates](#222-updates)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.3 Units](#223-units)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.4 AccessInformation](#224-accessinformation)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.5 Parameter content](#225-parameter-content)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.6 Out-of-sync Description and Differences in Text](#226-out-of-sync-description-and-differences-in-text)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.7 Use of Relative StopDate](#227-use-of-relative-stopdate)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.8 Inconsistent ObservedRegions](#228-inconsistent-observedregions)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.9 Inconsistent InformationURLs](#229-inconsistent-informationurls)<br/>
&nbsp;&nbsp;&nbsp;[2.3 Conclusion and Recommendations](#23-conclusion-and-recommendations)<br/>
[3 ISTP to SPASE](#3-istp-to-spase)
<!-- \TOC -->
# 1 CDAWeb

## 1.1 Overview

CDAWeb provides access to metadata used for its data services in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml) and [Master CDFs](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/). Their software engineers have provided essential guidance and insight into the development of HAPI metadata.

Although CDF files uploaded to or pulled into CDAWeb from instrument teams typically are roughly compliant with their [ISTP metadata guidelines](https://spdf.gsfc.nasa.gov/istp_guide/istp_guide.html), there is variability in the level of compliance. In many cases, "patches" to these CDF files are needed for the CDAWeb display and listing software to work. To address this, they create "Master CDFs". In addition, CDAWeb web service-specific metadata, such as plot rendering information used by their [IDL processing code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/), is included in the master CDFs. Also, "virtual" variables used by the CDAWeb plotting software are often added. For example, suppose a variable that depends on time, energy, and pitch angle is in the dataset CDFs. In that case, they may add one variable per pitch angle by defining "virtual" variables. The code needed to produce a virtual variable is defined in [IDL code](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/source/virtual_funcs.pro).

The Master CDFs are posted for external use, with caveats. From [0MASTERS/00readme.txt](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0MASTERS/00readme.txt):

> The following collections of Master CDF files were generated from a single data CDF or netCDF, for each dataset, for use in the  CDAWeb system (https://cdaweb.gsfc.nasa.gov).
>
> They are provided to the public for easier viewing/searching the metadata and quantities available in the data sets.
>
> In many cases the Master CDF is changed to improve the metadata in the original data files (especially to improve their compliance with the ISTP Metadata Guidelines), and often to add CDAWeb-specific metadata and addition plotting capabilities.
>
> Since the Master files are created using skeletontable/skeletoncdf tools from a data file and not necessarily reviewed and edited (especially for historical datasets), THEY SHOULD BE USED WITH CAUTION.

## 1.2 Issues

In attempting to create HAPI metadata from CDF Master, several issues were encountered, which have been posted to this repository [issue tracker](https://github.com/rweigel/cdawmeta/issues); many others were handled over email. We are working with the CDAWeb developers to resolve issues relevant to HAPI, and we have also documented other issues that may affect other users of CDF Master or CDF data files.

## 1.3 Conclusion and Recommendations

We suggest that the community would benefit if Master CDF metadata was improved. This would

1\. improve the quality of HAPI and SPASE metadata generated based on Master CDF metadata

2\. reduce duplication of effort by scientists and developers in handling non-compliance. For example,

* [`pytplot`](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L213) accounts for the fact that both `SI_CONVERSION` and `SI_CONV` are used as attributes in Master CDFs, but they missed `SI_conv`, which is [also found](https://github.com/rweigel/cdawmeta/issues/14).
* [`pytplot`](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L140) checks for only `DISPLAY_TYPE` but misses the fact that `Display_Type` and `DISPLAYTYPE` are also found in CDF Masters. The [CDAWeb IDL library](https://github.com/rweigel/cdawlib) [does not look for `DISPLAYTYPE`](https://github.com/search?q=repo%3Arweigel%2FCDAWlib%20DISPLAYTYPE&type=code) and neither does [ADAPT](https://github.com/search?q=repo%3Aspase-group%2Fadapt%20DISPLAY_TYPE&type=code). (Note that these links go to a personal repo with a copy of the CDAWeb IDL library, which is not available in a public repository that can be searched have files linked to by line.)
* [`pytplot`](https://github.com/MAVENSDC/PyTplot/blob/master/pytplot/importers/cdf_to_tplot.py#L158) accounts for `DEPEND_TIME` meaning the same thing as `DEPEND_0`. We missed this fact when developing HAPI metadata but could not find documentation to confirm it.
* [`Autoplot/CdfUtil.java`](https://github.com/autoplot/app/blob/master/CdfJavaDataSource/src/org/autoplot/cdf/CdfUtil.java) has worked around many CDF and Master CDF metadata issues. (See also [CdfVirtualVars.java](https://github.com/autoplot/app/blob/master/CdfJavaDataSource/src/org/autoplot/cdf/CdfVirtualVars.java)).
* The [`CdawebUtil.java` for the CDAWeb HAPI server](https://git.smce.nasa.gov/spdf/hapi-nand/-/blob/main/src/java/org/hapistream/hapi/server/cdaweb/CdawebUtil.java?ref_type=heads) also contains workarounds.
* [The SPDF CDF Java library](https://github.com/autoplot/cdfj) (posted in this personal repo because it is not available in a public SPDF repo) catches some, but not all CDF metadata issues. For example, it catches `DEPEND_TIME`, but misses the fact that `Display_Type` and `DISPLAYTYPE` (it seems awkward for a CDF file format library to handle special metadata cases).
* In the early days of SPASE, Jan Merka was creating SPASE records using CDAWeb metadata, and he encountered many of the same issues we did (which we learned recently).
* The HAPI metadata generation code addresses many anomalies. See the files in the [attrib directory](https://github.com/rweigel/cdawmeta/tree/main/cdawmeta/attrib) and [hapi.py](https://github.com/rweigel/cdawmeta/blob/main/cdawmeta/generators/hapi.py). [Logs](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/) of issues encountered that affected HAPI metadata generation encountered is generated by this code. These issues are tracked in the [cdawmeta issue tracker](https://github.com/rweigel/cdawmeta/issues), and we add information conveyed to us via email or on telecons to the issues threads.

We also recommend

* documentation of known issues and suggested workarounds - many developers who have re-discover issues, or missed issues, would benefit;
* a publicly visible issue tracker, and encouragement by the community to use it, for CDAWeb metadata (the [cdawmeta issue tracker](https://github.com/rweigel/cdawmeta/issues) serves this purpose now). Although CDAWeb is responsive to many reports on errors in Master CDFs, we have found many developers in the community who have encountered the same issues and workarounds and have not reported them. With such a tracker, other developers would benefit from accumulated knowledge of issues, and for issues that will not be fixed, they will benefit from the discussion on how to fully work around an issue;
* documentation of non-ISTP attributes so that users know if an attribute is important for interpretation;
* a clearer indication of, or documentation of, attributes that are CDAWeb-software specific; and
* tests on Master CDFs and newly uploaded data CDFs that catch problems that will cause downstream software to fail, some of those listed in the [issue tracker](https://github.com/rweigel/cdawmeta/issues) fall in this category; a examples include issues with recently updated [PSP data CDF files](https://github.com/rweigel/cdawmeta/issues/12) and [incorrect `SI_CONVERSION` factors](https://github.com/rweigel/cdawmeta/issues/29).
* Standards for the representation of CDF in JSON (there exists one for XML, CDFML) and as a Python dictionary. The code used in this project assumes the JSON structure of the Masters will not change and the data structures returned by `cdflib` will not change. Effort was needed to modify the data structures returned by `cdflib` to match that found in the Masters. 

Early indications are that much of this is out-of-scope of the CDAWeb project. For example, CDAWeb does not control the content or quality of the files that they host and improving the metadata for use by non-CDAWeb software may not be supported. However, addressing these issues will greatly impact the quality of code and metadata downstream (e.g., HAPI, SPASE, SOSO, etc.); if it is out-of-scope, we should find support for addressing these perennial issues.

# 2 SPASE

## 2.1 Overview

Our initial attempt was to generate HAPI metadata with SPASE records.

The primary issues that we encountered related to HAPI are the first three discussed in the following section. The others were noticed in passing; many are addressed by the [`spase_auto.py`](https://github.com/rweigel/cdawmeta/blob/main/cdawmeta/generators/spase_auto.py) code that draws information from the [`cdawmeta-spase`](https://github.com/rweigel/cdawmeta-spase) repository.

In addition, we doubt that new efforts that use CDAWeb SPASE records for search (either with or without `Parameter`-level information) will be useful without addressing the issues described in this section.

## 2.2 Issues

### 2.2.1 Completion

Only about 40\% of CDAWeb datasets had parameter-level SPASE records when we first considered using them for HAPI metadata in 2019. Approximately five years later, there is only [~66\% coverage](https://github.com/rweigel/cdawmeta-spase/blob/main/statistics.txt) (however, as discussed below, the number that are up-to-date, correct, and without missing parameters is less).

The implication is that CDAWeb `NumericalData` SPASE records cannot be used for one of the intended purposes, which is to provide a structured, correct, and complete representation of CDAWeb metadata; we needed to duplicate much of the effort that went into creating CDAWeb SPASE records over the past 20 years in order to create a complete set of HAPI metadata.

### 2.2.2 Updates

The CDAWeb SPASE metadata is not updated frequently. There are instances where variables have been added to CDAWeb datasets but the SPASE records do not have them. There are also cases where SPASE records are missing variables for datasets that have not changed since the SPASE records were created. Examples are given in the `Parameter` subsection.

The implication is that a scientist who executes a search backed by SPASE records may erroneously conclude that variables or datasets are unavailable.

### 2.2.3 Units

We considered using SPASE `Units` when they were available because although CDAWeb Master metadata has a `UNITS` attribute, no consistent convention is followed for the syntax, and in some cases, `UNITS` are not a scientific unit but a label (e.g. `0=good` and `<|V|>`).

Using SPASE `Units` was complicated by the fact that many CDAWeb datasets do not have SPASE records and ones with SPASE records do not always have `Parameter`s. So we would need to use SPASE `Units` when available and CDF Master units otherwise.

We abandoned the use of SPASE `Units` when we noticed instances where the SPASE `Units` were wrong.

For example, `AC_H2_ULE/unc_H_S1`, has `UNITS = '[fraction]'` in the [CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/ac_h2_ule_00000000_v01.json) and `Units = '(cm^2 s sr MeV)^-1)'` [in SPASE](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json). See also [a dump of the unique Master `UNITS` to SPASE `Units` pairs](http://mag.gmu.edu/git-data/cdawmeta/data/reports/units-CDFUNITS_to_SPASEUnit-map.json), which is explained in [units.md](http://mag.gmu.edu/git-data/cdawmeta/data/reports/units-CDFUNITS_to_SPASEUnit-map.json). (Note that CDAWeb [includes a link to this SPASE record](https://cdaweb.gsfc.nasa.gov/misc/NotesA.html#AC_H2_ULE) and [elsewhere](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval1.cgi?index=sp_phys&group=ACE) to [a SKT file](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0SKELTABLES/ac_h2_ule_00000000_v01.skt) with different units.)

We concluded that if we wanted to represent CDAWeb variables in HAPI with units that adhered to a syntax so the string could be validated, we would need to take the steps described in the [`cdawmeta-spase` repository README](https://github.com/rweigel/cdawmeta).

### 2.2.4 AccessInformation

Some `AccessInformation` nodes are structured in a way that is misleading.

For example, [ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json) indicates that the `Format` for the first four `AccessURL`s is `CDF`, which not correct. The `Name=CDAWeb` access URL has has many other format options. The `SSCWeb` access URL does not provide `CDF` and the names of the parameters at SSCWeb are not the same as those listed in `Parameters`; also, more parameters are available at SSCWeb. Finally, `CSV` is listed as the format for the `Style=HAPI` `AccessURL`, but `Binary` and `JSON` are available.

Note that Bernie Harris has a web service that produces SPASE records with additional `AccessInformation` nodes, for example, compare

* [his ACE/Ephemeris/PT12M](https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M) with
* [hpde.io ACE/Ephemeris/PT12M](https://hpde.io/ASA/NumericalData/ACE/Ephemeris/PT12M.json)

I don't know if Bernie's web service it is being used - although it is under [heliophysicsdata.gsfc.nasa.gov](heliophysicsdata.gsfc.nasa.gov), it seems to not be used there - for example, see the [`heliophysicsdata.gsfc.nasa.gov` search result for AC_OR_SSC](https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=AC_OR_SSC).

It is often found that SPASE records contain parameters that are only available from one of the `AccessURLs`. For example,

* [ACE/MAG/L2/PT16S](https://hpde.io//NASA/NumericalData/ACE/MAG/L2/PT16S)

  * references a [CDAWeb page](https://cdaweb.gsfc.nasa.gov/cgi-bin/eval2.cgi?dataset=AC_H0_MFI&index=sp_phys) that has different names, e.g., `B-field magnitude` vs. `Bmagnitude` and `Magnitude` in SPASE. Why?
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

This is a complicated problem. We are also considering serving CDF data of type `VAR_DATA=support_data`. In this case, the HAPI metadata will reference many more parameters available from the CDAWeb web service, which only provides access to `VAR_DATA=data` variables.

In the [`cdawmeta-spase` repository](https://github.com/rweigel/cdawmeta-spase), we have a template that addresses some of these issues, including the addition of a note on the parameter names and the fact that all parameters may not be available from all `AccessURL`s. This template is used to generate the `spase_auto` metadata. Examples: 

* [spase_auto/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/AC_OR_SSC.json)
* [spase_auto/info/VOYAGER1_10S_MAG.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/VOYAGER1_10S_MAG.json)


Note that the parameter list generated by `spase_auto` may differ from what is generated by the [resolver](https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html). We start with the full list of parameters but drop certain ones if there are issues with the metadata or CDF files that will prevent the data from being served. The list of dropped parameters is available in a [log file](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/). (It is straightforward to modify this behavior, however).

### 2.2.5 Parameter content

Many SPASE records do not contain the full list of variables available from the [CDAWeb web service](https://cdaweb.gsfc.nasa.gov/WebServices/). This issue was apparently noticed before - Bernie Harris has a [resolver](https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html) that will create a SPASE record with the variables available from the [CDAWeb web service](https://cdaweb.gsfc.nasa.gov/WebServices/) (but some variables available in the raw CDFs are excluded).

CDAWeb datasets may have variables with different `DEPEND_0s`, and the `DEPEND_0` may have a different cadence. For example, `VOYAGER1_10S_MAG` has two `DEPEND_0s`:

* [`Epoch` with cadence `PT48S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader) and
* [`Epoch2`, with cadence `PT9.6S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader).

However, the [SPASE record](https://hpde.io/NASA/NumericalData/Voyager1/MAG/CDF/PT9.6S.json) for this dataset, which is linked to from [CDAWeb](https://cdaweb.gsfc.nasa.gov/misc/NotesV.html#VOYAGER1_10S_MAG), lists both of these variables and their dependents as having a cadence of `PT9.6S`. The `spase_auto` metadata described above addresses this issue.

The [Resource ID convention](https://spase-group.org/docs/conventions/index.html) suggests putting cadence in the `ResourceID` string. However, no convention is suggested for how the cadence is rendered. For example, should one day be given as `PT86400S` or `P1D`? No convention is suggested for the amount of precision to use. Our SPASE generation code computes the cadence of a dataset by computing the histogram of the difference in time step and the most frequent time step is used. We have found that this automated process sometimes finds a cadence that does not match the cadence in the `ResourceID`.

### 2.2.6 Out-of-sync Description and Differences in Text

[NotesO.html#OMNI_HRO2_1MIN](https://cdaweb.gsfc.nasa.gov/misc/NotesO.html#OMNI_HRO2_1MIN) has a link to [OMNI/HighResolutionObservations/Version2/PT1M](https://hpde.io/NASA/NumericalData/OMNI/HighResolutionObservations/Version2/PT1M), which has [a broken link](https://omniweb..sci.gsfc.nasa.gov/html/HROdocum.html) (it is likely that the broken link was corrected in the CDF metadata and the SPASE record was not updated).

Although improvements were made in the presentation in the SPASE version, why not improve the source metadata and derive SPASE metadata from the source? Having two independent versions of the same thing often leads to a divergence in content, as was the case here and probably has occurred elsewhere (we have only visually inspected ~20 SPASE records). This is one of the reasons that the `spase_auto` code uses `all.xml` or Master CDF metadata in favor of SPASE content if they both contain similar information.

PI's writing seems to have been modified (assuming PI did not request the SPASE `Description` to be a modified version of what is in the CDF):

[`TEXT` node in the CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/uy_proton-moments_swoops_00000000_v01.json):

> This file contains the moments obtained from the distribution function of protons after deconvolution using the same magnetic field values used to construct the matrices. The vector magnetic field and the particle velocity are given in inertial RTN coordinates. ...

[`Description` node in the corresponding SPASE record](https://hpde.io/NASA/NumericalData/Ulysses/SWOOPS/Proton/Moments/PT4M):

> This File contains the Moments obtained from the Distribution Function of Protons after Deconvolution using the same Magnetic Field Values used to construct the Matrices. The Vector Magnetic Field and the Particle Velocity are given in Inertial RTN Coordinates. ...

Our opinion is that only in rare circumstances should descriptive information not in all.xml, the Master CDF, a journal article, instrument documentation, or the PI's web page, or written by someone on the instrument team be in SPASE. Also, when content is taken from papers and web pages an put in SPASE by non-instrument team members, it should be referenced. When we were creating SPASE records as part of the Virtual Radiation Belt Observatory, I argued that the fact that I was awarded the grant did not give me the authority to write documentation for radiation belt--related instruments. Such authority requires experience with the instrument and any non--trivial description or documentation that could not be quoted should be approved by an instrument team member.

### 2.2.7 Use of Relative StopDate

The `StopDate`s are relative even though the actual stop date is available in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml). Given that many SPASE records have not been updated in years, it is likely that the relative `StopDate` is wrong in some cases (due to, for example, no more data being produced).

The `spase_auto` metadata described above addresses this issue and updates `StopDates` daily.

### 2.2.8 Inconsistent ObservedRegions

Most CDAWeb datasets with ids in the form `a_b_c` should have the same `ObservedRegion` as a dataset that starts with `a_y_z` (unless an instrument was not active while the spacecraft was in certain regions). This is frequently not the case; see the error messages in [hpde_io.log](http://mag.gmu.edu/git-data/cdawmeta/data/reports).

For example
```
AC_OR_DEF: ['Heliosphere', 'Heliosphere.NearEarth', 'Heliosphere.Inner']
AC_H2_CRIS: ['Heliosphere.NearEarth']
```

The implication of this for search is that a user may make an incorrect conclusion about the number of instruments that made measurements in a given region.

The `spase_auto` code applies `ObservedRegion` corrections as described in the [`cdawmeta-spase`](https://github.com/rweigel/cdawmeta-spase/) repository. This code is incomplete - there are instances when datasets with ids in the form `a_b_c` do not have the same observed region as all datasets that start with `a_y_z`. For example, `VOYAGER1_PLS` and `VOYAGER2_PLS`; this case is handled, but there may be others.

### 2.2.9 Inconsistent InformationURLs

[InformationURL.json](https://github.com/rweigel/cdawmeta-spase/blob/main/InformationURL.json) contains keys of a `URL` in an `InformationURL` node and an array with all CDAWeb datasets it is associated with. There are many instances where a URL should apply to additional datasets. For example, all dataset IDs that end in `_SSC`, `_DEF`, and `_POSITION` should be associated with https://sscweb.gsfc.nasa.gov. This issue is corrected in the `spase_auto` code.

Also, the Master CDFs contain informational URLs that do not appear in the associated SPASE `NumericalData` records. This represents an unnecessary loss of information. The merger of Master URLs with SPASE URLs in `spase_auto` is not complete.

## 2.3 Conclusion and Recommendations

Although HAPI has an `additionalMetadata` attribute, we are reluctant to reference existing SPASE records due to these issues (primarily 2., 3., and 5.). We conclude that it makes more sense to link to less extensive but correct metadata (for example, to CDF Master metadata or documentation on the CDAWeb website<sup>*</sup>) than to more extensive SPASE metadata that is confusing (see 4.) or incomplete and in some cases incorrect (see items 2., 3., and 5.).

<sup>*</sup> This is not quite possible - CDAWeb includes links to SPASE with incorrect information, for example, ones with incorrect units or a list of parameters that is not the same as what is shown in their data selection menu.

The primary problems with existing CDAWeb `NumericalData` SPASE records are

1. they appear to have been created ad-hoc by different authors who follow different conventions and include different levels of detail;
2. there is no automated mechanism for updating or syncing the SPASE records with CDAWeb metadata; and
3. there do not appear to be mechanisms in place to ensure the content of SPASE records is correct, consistent, and not confusing.

We suggest that there is urgency of having correct and complete SPASE `NumericalData` records because there are several applications under development that will use SPASE records to provide search functionality. The quality of such applications is limited by the quality of the database it uses, and it is important that the database content is correct and consistent.

CDAWeb SPASE `NumericalData` records have been under development since 2009 and yet these problems persist. At the current rate of generation, they may not be complete for another 5-10 years. We suggest a different approach is needed.

We suggest that CDAWeb SPASE metadata should be created by an automated process similar to that used by `spase_auto` (which is also how HAPI metadata is generated). This code primarily requires existing CDAWeb metadata information and some additional metadata that is stored in a few version-controlled files. This information is described in the [cdawmeta-spase](https://github.com/rweigel/cdawmeta-spase) repository and the SPASE generation code that is needed in addition to the code used for creating HAPI metadata is ~500 lines (see [`spase_auto.py`](https://github.com/rweigel/cdawmeta/blob/main/cdawmeta/generators/spase_auto.py)). This approach would have prevented many of the errors and inconsistencies described above and further detailed in the [`cdawmeta-spase` README](https://github.com/rweigel/cdawmeta-spase).

# 3 ISTP to SPASE

Two versions of ISTP->SPASE exist [one used in the Javascript ISTP editor](https://git.smce.nasa.gov/spdf/skteditor/-/blob/main/ISTP_to_SPASE.txt) | [one in a recent paper](https://www.sciencedirect.com/science/article/pii/S0273117723008025)).

Rebecca R. has created [notes](https://docs.google.com/document/d/1hd2On4uYQAPNc0xEgZ4XHBOQ8V-x0Pkh5UNtXrkgl7Y/edit?tab=t.0#heading=h.t1lddh51itde) for documenting the mappings.

