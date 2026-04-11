`cdawmeta` version `0.0.2`

<!-- TOC -->
[1 About](#1-about)<br/>
[2 Installing and Running](#2-installing-and-running)<br/>
&nbsp;&nbsp;&nbsp;[2.1 Examples](#21-examples)<br/>
&nbsp;&nbsp;&nbsp;[2.2 Generators](#22-generators)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.1 start_stop](#221-start_stop)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.2 cadence](#222-cadence)<br/>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[2.2.3 sample_links](#223-sample_links)
<!-- \TOC -->

# 1 About

This Python package uses [CDAWeb's](https://cdaweb.gsfc.nasa.gov) metadata to create HAPI `catalog` and `info` metadata and SPASE `NumericalData` metadata.

It was originally developed to upgrade the metadata from CDAWeb's HAPI server (the existing server only includes the minimum required metadata).

As discussed in the [notes](https://github.com/rweigel/cdawmeta/blob/main/Notes.md) document, the code was extended to remedy major issues with existing SPASE `NumericalData` metadata for CDAWeb datasets. (We abandoned our attempt to use existing SPASE records to create HAPI metadata due to these issues.)

The code reads and combines information from

* [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which has dataset-level information for approximately 2,700 datasets;
* [Master CDF](https://cdaweb.gsfc.nasa.gov/pub/software/cdawlib/0JSONS/) files (we use the JSON representation) referenced in [all.xml](https://spdf.gsfc.nasa.gov/pub/catalogs/all.xml), which contain both  dataset-level metadata and variable metadata;
* The list of URLs for CDF files associated with each dataset using the CDASR [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) endpoint (due to the amount of time use of this endpoint took, we now use [sp_phys_cdfmetafile.txt](https://cdaweb.gsfc.nasa.gov/~tkovalic/metadata/sp_phys_cdfmetafile.txt)); and
* A CDF file referenced in the [orig_data](https://cdaweb.gsfc.nasa.gov/WebServices/) response (for computing cadence and determining if the variable names in the Master CDF match those in a data CDF).

The code uses [requests-cache](https://github.com/requests-cache/requests-cache/), so files are only re-downloaded if the HTTP headers indicate they are needed. When metadata are downloaded, a diff is stored if they changed.

The output is

1. HAPI metadata, which is available in [hapi/info](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info)

2. Proof-of-concept SPASE records that do not have most of the major issues described in [Notes](https://github.com/rweigel/hxform/blob/main/Notes.md). (These SPASE records are available in JSON in [spase_auto/info](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info).)

In addition, we have developed several tools for inspection and debugging. SQL databases are available with a search interface for

   * [CDAWeb dataset-level information](https://hapi-server.org/meta/cdaweb/dataset/), which is based on content stored in [all.xml](http://mag.gmu.edu/git-data/cdawmeta/data/allxml) and [Masters CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * [CDAWeb variable-level information](https://hapi-server.org/meta/cdaweb/variable/), which is based on content stored in [Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/master)
   * `hpde.io` [SPASE dataset-level information](https://hapi-server.org/meta/cdaweb/spase/dataset/), which is based on content non-`Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * `hpde.io` [SPASE parameter-level information](https://hapi-server.org/meta/cdaweb/spase/parameter/), which is based on content `Parameter` nodes of [SPASE records referenced in the Master CDFs](http://mag.gmu.edu/git-data/cdawmeta/data/spase)
   * [HAPI dataset-level information](https://hapi-server.org/meta/cdaweb/hapi/dataset/), which is based on the non-`parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)
   * [HAPI parameter-level information](https://hapi-server.org/meta/cdaweb/hapi/parameter/) (from the old and new server), which is based on the `parameter` nodes in [hapi info requests](http://mag.gmu.edu/git-data/cdawmeta/data/hapi)

Also, demonstration code for placing SPASE records into a MongoDB and executing a search is available in `query.py`.

# 2 Installing and Running

(Formal unit tests using `pytest` are in development.)

```
git clone https://github.com/rweigel/cdawmeta.git
cd cdawmeta;
pip install -e .

# Test commands in README. (errors shown in red are encountered metadata errors).
make test-README
```

In the examples, use `--update` to update the input metadata (source data changes on the order of days, typically in the mornings Eastern time on weekdays).

See `python metadata.py -h` for more options, including the generation of metadata for only `id`s that match a regular expression and skipping `ids`.

## 2.1 Examples

Create and display proof-of-concept auto-generated SPASE; the output of this command can be viewed at
[spase_auto/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/AC_OR_SSC.json) and [spase_auto/info/VOYAGER1_10S_MAG.json](http://mag.gmu.edu/git-data/cdawmeta/data/spase_auto/info/VOYAGER1_10S_MAG.json). See the [`cdawmeta-spase` repository](https://github.com/rweigel/cdawmeta-spase) for metadata used that is not available in Master CDFs and/or `all.xml`. (Remove `--meta-type spase_auto` to see all generated metadata types described in the next subsection.)

Note that the first execution will take ~60 seconds because a large metadata file must be downloaded from CDAWeb.
```
python metadata.py --id AC_OR_SSC --meta-type spase_auto
python metadata.py --id VOYAGER1_10S_MAG --meta-type spase_auto
```

Create and display HAPI metadata; the output of these commands can be viewed at [hapi/info/AC_OR_SSC.json](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info/AC_OR_SSC.json) and [hapi/info/VOYAGER1_10S_MAG.json](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/info/VOYAGER1_10S_MAG.json):
```
python metadata.py --id AC_OR_SSC --meta-type hapi
python metadata.py --id VOYAGER1_10S_MAG --meta-type hapi
```

## 2.2 Generators

`cdawmeta` uses ["generator" functions](https://github.com/rweigel/cdawmeta/tree/main/cdawmeta/generators) to create parts used in high-level metadata such as HAPI and SPASE. Each generator takes inputs that include its dependencies a produce new metadata. For example `start_stop.py` uses the output of `orig_data` to determine a `sample{StartStop}Date` to include in HAPI metadata.

### 2.2.1 start_stop

Used in HAPI.

```
python metadata.py --id VOYAGER1_10S_MAG --meta-type start_stop
```

Produces the following output, which can be [downloaded directly](http://mag.gmu.edu/git-data/cdawmeta/data/start_stop/info/VOYAGER1_10S_MAG.json).
<details>
<summary>Output</summary>
<pre>
{
  "id": "VOYAGER1_10S_MAG",
  "start_stop": {
    "id": "VOYAGER1_10S_MAG",
    "data-file": "./data/start_stop/info/VOYAGER1_10S_MAG.json",
    "data": {
      "sampleStartDate": "1991-10-28T04:59:54.000Z",
      "sampleStopDate": "1991-11-26T19:09:30.000Z",
      "note": "sample{Start,Stop}Date corresponds to the time range spanned by the penultimate file in the reponse from https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets/VOYAGER1_10S_MAG/orig_data/19770905T141947Z,19911227T000042Z, where the start/end in this URL correponds to the start/end of the dataset."
    }
  }
}
</pre>
</details>

### 2.2.2 cadence

Used in HAPI and SPASE.

```
python metadata.py --id VOYAGER1_10S_MAG --meta-type start_stop
```
Produces the following output (full output can be [downloaded directly](http://mag.gmu.edu/git-data/cdawmeta/data/cadence/info/VOYAGER1_10S_MAG.json)).

<details>
<summary>Output</summary>
<pre>
{
  "id": "VOYAGER1_10S_MAG",
  "cadence": {
    "id": "VOYAGER1_10S_MAG",
    "data-file": "./data/cadence/info/VOYAGER1_10S_MAG.json",
    "data": {
      "id": "VOYAGER1_10S_MAG",
      "cadence": {
        "Epoch2": {
          "url": "https://cdaweb.gsfc.nasa.gov/sp_phys/data/voyager/voyager1/magnetic_fields_cdaweb/mag_10s/1977/voyager1_10s_mag_19770905_v01.cdf",
          "note": "Cadence based on variable 'Epoch2' in https://cdaweb.gsfc.nasa.gov/sp_phys/data/voyager/voyager1/magnetic_fields_cdaweb/mag_10s/1977/voyager1_10s_mag_19770905_v01.cdf. This most common cadence occurred for 98.8448% of the 20964 timesteps. Cadence = 9600 [ms] = PT9.6S.",
          "counts": [
            {
              "count": 20964,
              "duration": 9600,
              "duration_unit": "ms",
              "duration_iso8601": "PT9.6S",
              "fraction": 0.9884483002498939
            },
            {
              "count": 194,
              "duration": 9601,
              "duration_unit": "ms",
              "duration_iso8601": "PT9.601S",
              "fraction": 0.009147060210288086
            },
         ...
         "Epoch": {
            "url": "https://cdaweb.gsfc.nasa.gov/sp_phys/data/voyager/voyager1/magnetic_fields_cdaweb/mag_10s/1977/voyager1_10s_mag_19770905_v01.cdf",
            "note": "Cadence based on variable 'Epoch' in https://cdaweb.gsfc.nasa.gov/sp_phys/data/voyager/voyager1/magnetic_fields_cdaweb/mag_10s/1977/voyager1_10s_mag_19770905_v01.cdf. This most common cadence occured for 94.2231% of the 3996 timesteps. Cadence = 48000 [ms] = PT48S.",
            "counts": [
               {
               "count": 3996,
               "duration": 48000,
               "duration_unit": "ms",
               "duration_iso8601": "PT48S",
               "fraction": 0.9422306059891535
               },
               {
               "count": 194,
               "duration": 48001,
               "duration_unit": "ms",
               "duration_iso8601": "PT48.001S",
               "fraction": 0.04574392831879274
               },
               ...
            ]
         }
      }
   }
}
</pre>
</details>

### 2.2.3 sample_links

Created to support link testing (several projects have involved testing links, and the generation of appropriate links is not trivial).

```
python metadata.py --id VOYAGER1_10S_MAG --meta-type sample_links
```
Produces a [JSON file](http://mag.gmu.edu/git-data/cdawmeta/data/sample_links/info/VOYAGER1_10S_MAG.json) with many test links.

