# About

This package is an interface to [CDAWeb's](https://cdaweb.gsfc.nasa.gov) metadata.

It was originally developed for upgrading the metadata from CDAWeb's HAPI servers; the existing server only includes the minimum required metadata. 

As discussed in the (SPASE)[#SPASE] section, the code has been extended to remedy issues with existing SPASE metadata for CDAWeb datasets.

The code reads and combines metadata from

* [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which has dataset-level information about ~2,500 datasets;
* The [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/) files (represented in JSON) referenced in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which contain both  dataset-level metadata and variable metadata; and
* The list of URLs associated with all CDF files associated with a dataset using the [CDASR orig_data endpoint](https://cdaweb.gsfc.nasa.gov/WebServices/).

As discuss in the (SPASE)[#SPASE] section, an attempt to use existing SPASE records was abandoned.

<!--* SPASE records referenced in the Master CDF files, which are read by a request to [hpde.io](https://hpde.io/).-->

The code uses [requests-cache](https://github.com/requests-cache/requests-cache/) so re-downloads of any of the above metadata are only made if the HTTP headers indicate it is needed. When any metadata are downloaded, a diff is stored.

The output is

1. HAPI metadata, which is based on [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata. (The cadence of the measurements for each dataset by sampling the last CDF file associated with the dataset (based on result from the [CDASR orig_data endpoint](https://cdaweb.gsfc.nasa.gov/WebServices/)) and computing a histogram of the differences in timesteps.)

2. SQL and MongoDB databases available with a search interface:
  * [CDAWeb datasets](https://hapi-server.org/meta/cdaweb/dataset/), which is based on content stored in [all.xml](http://mag.gmu.edu/git-data/cdawmeta/data/allxml) and [Masters CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
  * [CDAWeb variables](https://hapi-server.org/meta/cdaweb/variable/), which is based on content stored in [Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
  * [SPASE datasets](https://hapi-server.org/meta/spase/dataset/), which is based on content non--`Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
  * [SPASE parameters](https://hapi-server.org/meta/spase/parameter/), which is based on content `Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
  * [HAPI datasets](https://hapi-server.org/meta/hapi/parameter/), which is based on [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata.
  * [HAPI parameters](https://hapi-server.org/meta/hapi/parameter/), which is based on [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata.

Also, not yet available in table form is
* draft [SOSO](https://github.com/ESIPFed/science-on-schema.org) metadata (see http://mag.gmu.edu/git-data/cdawmeta/data/soso). This metadata is derived using [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/), and [HAPI](http://mag.gmu.edu/git-data/cdawmeta/data/soso) metadata. (This is not ideal and this implementation exists because this repository was originally intended to create only HAPI metadata; A better approach would be to combine [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) metadata into a generic schema and derive HAPI, SOSO, and SPASE from it.) and
* draft SPASE records that do not have the flaws described in SPASE section below.

# SPASE

Our initial attempt was to generate HAPI metadata with SPASE records. Several issues were encountered:

1. Only about 40% of CDAWeb datasets had SPASE records. ~5 years later, there is ~60% coverage. As a result, we had to use metadata from [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/), and [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/).

   The implication is that CDAWeb SPASE records cannot be used for one of the intended purposes of SPASE, which is to provide a structured representation of CDAWeb metadata; we needed to duplicate much of the effort that went into creating CDAWeb SPASE records in order to create a complete set of HAPI metadata.

2. The SPASE metadata is not updated frequently. There are instances where variables have been added to CDAWeb datasets but the SPASE records do not have this. In some instances, SPASE records are missing variables even for datasets that that have not changed.

   The implication is a scientist who executes a search backed by SPASE records may erroneously conclude variables or datasets are not available.

3. We considered using SPASE `Units` for variables when they were available because although CDAWeb Master metadata has a `UNITS` attribute, no consistent convention is followed for the syntax and in some cases, `UNITS` are not a scientific unit but a label (e.g. `0=good` and `<|V|>`). This effort stopped when we noticed [instances where the SPASE `Units` were wrong]
For example, AC_H2_ULE/unc_H_S1, has UNITS = '[fraction]' in the [CDF Master](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/ac_h2_ule_00000000_v01.json) and Units =  '(cm^2 s sr MeV)^-1)' [in SPASE](https://hpde.io/NASA/NumericalData/ACE/ULEIS/Ion/Fluxes/L2/PT1H.json). The implication is a scientist using SPASE `Units` to label their plots risks the plot being incorrect.

   There was a second complicating factor. Some SPASE records did not have Parameters for all `VAR_TYPE=data` CDAWeb variables in a given dataset. So to use SPASE, we would need to determine if a CDAWeb dataset had a SPASE record, had a Parameters node, and had the variable in the Parameters node.

   See [a dump of the unique Master `UNITS` to SPASE `Units` pairs](http://mag.gmu.edu/git-data/cdawmeta/data/cdawmeta-additions/query/query-units.json), which is explained in [query-units.md](http://mag.gmu.edu/git-data/cdawmeta/data/cdawmeta-additions/query/query-units.md)).

   In addition, although there is more consistency in the strings used for SPASE `Units`, SPASE does not require the use of a standard for the syntax (such as [VOUnits](https://www.ivoa.net/documents/VOUnits/20231215/REC-VOUnits-1.1.html), [udunits2](https://docs.unidata.ucar.edu/udunits/current/#Database), or [QUDT](http://qudt.org/vocab/unit/)). HAPI has an option to state the standard used for `unit` strings so that a validator can check and units-aware software (e.g., the [AstroPy](https://eteq.github.io/astropy/units/index.html) module) can use it to make automatic unit conversions when mathematical operations on are performed.

   We concluded that if we wanted to represent CDAWeb variables in HAPI with units that adhered to a syntax so the string could be validated, we need to write map each unique CDAWeb `UNIT` to a standard.

   This task will require

   1. Mapping all unique units (~800) to a standard, if possible. See [CDFUNITS_to_VOUNITS.csv](http://mag.gmu.edu/git-data/cdawmeta/data/cdawmeta-additions/CDFUNITS_to_VOUNITS.csv) where I have done a few mappings. This table is updated daily so that if a new unit is found, a warning is generated that indicates a new units string was found that needs a mapping.

   2. Determining the units for all variables that do not have a `UNITS` attribute or a `UNITS` value that is all whitespace. See [Missing_UNITS.json](http://mag.gmu.edu/git-data/cdawmeta/data/cdawmeta-additions/Missing_UNITS.json).

   3. Setting up a workflow that validates both of the above files are valid VOUnits.

   4. Validating that the mapping is correct. This could done in two ways: (a) Have two people independently write the mapping and (b) Use AstroPy to compute the SI conversion and compare with the `SI_{Conversion,conv,CONVERSION}` attribute value in the CDF. Creating the mapping will be straightforward, but I emphasize that we really need to verify the results. Putting incorrect units in metadata is unacceptable.

   Finally, I think that the correct source of the mapped units is not the files linked to above or SPASE - it should be the CDF Masters. Many people use CDF Masters for metadata and if the mapped units only existed in SPASE, they would have have access to them. In addition, the step of updating the files discussed in 1. above would not be needed. However, this would put a burden on CDAWeb to add VOUnits to existing masters and add VOUnits to new metadata. For new metadata, this approach is more robust - CDAWeb can work with the mission scientists who can confirm that their VOUnits mapping is equivalent to what the scientist chose to enter as a `UNIT` in the CDF metadata.

Other issues not related to the generation of HAPI metadata were also noticed.

4. The `AccessInformation` nodes are structured in a way that is misleading and clarification is needed.

   1. For example, [ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json
   ) indicates that the `Format` for all `AccessURL` is `CDF` for all four `AccessURLs`, which not correct. The `CDAWeb` access URL has has other format options. Also, HAPI is not listed. In other SPASE records where it is listed, only `Text` is listed as format, but `Binary` and `JSON` are available. The `SSCWeb` access URL does not provide `CDF`.

   Note that Bernie Harris has a web service that that produces SPASE records with additional `AccessInformation` nodes

      * [his ACE/Ephemeris/PT12M](https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M) with
      * [hpde.io ACE/Ephemeris/PT12M](https://hpde.io/NASA/NumericalData/ACE/Ephemeris/PT12M.json

   I don't know Bernie's web service it is being used - it seems to to be used at [heliophysicsdata.gsfc.nasa.gov](https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=AC_OR_SSC), which links to the hpde.io record.

   2. The names of the parameters at SSCWeb are not the same as those at CDAWeb; also, more parameters are available from CDAWeb.

   3. There are SPASE records with the ACE Science Center listed as an `AccessURL` (e.g., [ACE/MAG/L2/PT16S](https://hpde.io/NASA/NumericalData/ACE/MAG/L2/PT16S.json)). The variable names used in the ACE Science Center files and metadata differ from those listed in the `Parameter` node. This is confusing.

5. CDAWeb datasets may have variables with different `DEPEND_0s`, and the `DEPEND_0` may have a different cadence. For example, `VOYAGER1_10S_MAG` has two `DEPEND_0s`

   * [`Epoch2`, with cadence `PT9.6S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader) and
   * [`Epoch` with cadence `PT48S`](https://hapi-server.org/servers/#server=CDAWeb&dataset=VOYAGER1_10S_MAG@1&parameters=Time&start=1977-09-05T14:19:47Z&stop=1977-09-07T14:19:47.000Z&return=data&format=csv&style=noheader).

   SPASE records can only have a cadence that applies to all parameters, but the listed cadence in the SPASE record for [`VOYAGER1_10S_MAG` is `PT9.6S`](https://hpde.io/NASA/NumericalData/Voyager1/MAG/CDF/PT9.6S.json) is not correct for the parameters with a `DEPEND_0` of `Epoch`. This is inaccurate and misleading and potentially confusing for the user. The fact that some parameters have a different cadence should be noted in the SPASE record.

6. PIs writing has been modified (I'm assuming PI did not request the SPASE Description to be a modified version of what is in the CDF):

   `TEXT` node in https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/uy_proton-moments_swoops_00000000_v01.json

   > This file contains the moments obtained from the distribution function of protons after deconvolution using the same magnetic field values used to construct the matrices. The vector magnetic field and the particle velocity are given in inertial RTN coordinates.

   `Description` node in https://hpde.io/NASA/NumericalData/Ulysses/SWOOPS/Proton/Moments/PT4M

   > This File contains the Moments obtained from the Distribution Function of Protons after Deconvolution using the same Magnetic Field Values used to construct the Matrices. The Vector Magnetic Field and the Particle Velocity are given in Inertial RTN Coordinates.

   My opinion is that it it is not in all.xml, the Master CDF, a paper, or the PIs web page, or written by someone on the instrument team, it should not be in SPASE. Also, content taken from papers and web pages should be cited. Based on this and some of the other issues mentioned, I have low trust in the content of CDAWeb SPASE records.

7. The `StopDate`s are relative even though the actual stop date is available. Given that many SPASE records have not been updated in years, it is likely that the relative `StopDate` is wrong in some cases (due to, for example, no more data being produced).

Although HAPI has an `additionalMetadata` attribute, we are reluctant to reference existing SPASE records due to these issues (primarily 2., 3., and 6.). We conclude that it makes more sense to link to less extensive but correct metadata (for example, to CDF Master metadata or documentation on the CDAWeb website, than to more extensive SPASE metadata that is confusing (see 4.) or incomplete and in some cases incorrect (see items 2., 3., and 6.).

After encountering these issues, realized that solving all of the problems could be achieved with some additions to the existing CDAWeb to HAPI metadata code.

## Comments

* The generation of SPASE metadata should be split
  1. Someone who writes code to generate and check the metadata.
  2. Domain scientists who create additional metadata. 

An example of decoupling: Initially we had used `spase_DatasetResourceID` in CDF Masters to find SPASE records associated with a CDAWeb dataset. We were later told that not all available SPASE records are listed in CDF Masters. So we had to switch over to drawing directly from the hpde.io repository. 

# Running

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta
make hapi
```

See the comments in `Makefile` for additional execution options.

The Makefile command above executes the following

```
python cdaweb.py         # creates data/cdaweb.json
python hapi/hapi-new.py  # creates data/hapi/hapi-new.json and
                         # data/info/*.json using data/catalog-all.json
```

# Compare

To compare the new HAPI metadata with Nand's, first execute `cdaweb.py` then

```
python hapi/hapi-nl.py   # creates data/hapi/catalog-all.nl.json using requests to
                         # https://cdaweb.gsfc.nasa.gov/hapi/{catalog,info}
make compare [--include='PATTERN'] # creates data/hapi/compare.log
# PATTERN is a dataset ID regular expression, e.g., '^AC_'
```

# Browse and Search

See `table/README.md` for browsing and searching metadata from a web interface.
