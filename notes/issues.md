There are a few other issues.

* The P1D file says the format is `CDF` for `AccessURL/Name=CDAWeb`, but the `PT1H` says Text. The `URL` provides both (and more).
* Descriptions for H3 has `hourly` in several places (and is inconsistent with what is in `all.xml`)
* One has a note that data are presently 2-3 weeks delayed but has a relative stop of two months.
* The format for the Caltech link is `Text`, but I could only find HDF linked to [that page](https://izw1.caltech.edu/ACE/ASC/level2/lvl2DATA_CRIS.html)
* One has a `Time_PB5` parameter and the other does not. `Time_PB5` is not a parameter in many of the files or web service outputs found by following AccessURL links.

https://github.com/hpde/NASA/blob/master/NumericalData/ACE/CRIS/L2/P1D.xml#L67

```
         <AccessURL>
            <Name>CDAWeb</Name>
            <URL>https://cdaweb.gsfc.nasa.gov/cgi-bin/eval2.cgi?dataset=AC_H3_CRIS&amp;index=sp_phys</URL>
            <ProductKey>AC_H3_CRIS</ProductKey>
            <Description>ACE/CRIS L2 hourly data with subset, plot, list functionalities from CDAWeb</Description>
         </AccessURL>
         <Format>CDF</Format>
```

https://github.com/hpde/NASA/blob/master/NumericalData/ACE/CRIS/L2/PT1H.xml#L69

```
         <AccessURL>
            <Name>CDAWeb</Name>
            <URL>https://cdaweb.gsfc.nasa.gov/cgi-bin/eval2.cgi?dataset=AC_H2_CRIS&amp;index=sp_phys</URL>
            <ProductKey>AC_H2_CRIS</ProductKey>
            <Description>ACE/CRIS L2 hourly data with subset, plot, list functionalities from CDAWeb</Description>
         </AccessURL>
         <Format>Text</Format>
```



There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using different non-CDF master metadata. My understanding is that CDAWeb does not correct errors in non-master CDFs, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master (many do). To see the occurrences, search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `double`.

When attempting to compute sampleStart/Stops from data returned from the CDAS REST server, I noticed that a dataset has a time variable that is not monotonic. Nand's server has this too:

https://hapi-server.org/servers/#server=CDAWeb&dataset=AMPTECCE_H0_MEPA@3&parameters=Time&start=1989-01-10T23:36:42Z&stop=1989-01-10T23:36:43Z&return=data&format=csv&style=noheader

Should time always be monotonic?

## 8 Allowing queries of content of hpde.io repository

At least four developers have written code to ingest the contents of the [`hpde.io` git repository](https://github.com/hpde/hpde.io/) to extract information. The contents of the `hpde.io` represent a database and as such should be stored in one (a source code repository is not a database). We suggest that [hpde.io](https://hpde.io/) provide a query interface so that, e.g.,

> https:/hpde.io/?q={"Version": 2.4.1}

would be allowed, using, for example [MongoDB](https://mongodb.com/), [eXist-db](https://exist-db.org/), or [PostgreSQL](https://www.postgresql.org/). Note that Bernie Harris already runs an eXist database containing SPASE, which could be adapted for this purpose:

> ... it searches the spase documents for ones with //AccessInformation/AccessURL[name = ‘CDAWeb’ and ProductKey = ‘whatever’]. "Sufficiently described" meant that the cdaweb information was in the spase documents. At the time that code was written, there were many cdaweb datasets that didn't have spase descriptions or the spase descriptions didn't contain the cdaweb access information. Even now, spase is usually missing the most recent cdaweb datasets but it's not too far behind.
>
> This https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html describes a resolver service. To get all datasets at once, you might want to use https://heliophysicsdata.gsfc.nasa.gov/queries/CDAWeb_SPASE.html. Also note that https://cdaweb.gsfc.nasa.gov/WebServices/REST/#Get_Datasets returns the spase ResourceID.  For example,
>
>$ curl -s -H "Accept: application/json" "https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets?idPattern=AC_H0_MFI" | jq -jr '.DatasetDescription[]|(.Id,", ",.SpaseResourceId,"\n")'

Note that not all existing content in `hpde.io` is yet used by the automated process. For example, some SPASE records have additional details about CDAWeb variables that the automated process does not use. For example, `Qualifier`, `RenderingHints`, `CoordinateSystem`, `SupportQuantity`, and `Particle`, `Field`, etc. This could also be addressed by a table that has this information. However, there should be a discussion of this; there are over ~100,000 CDAWeb variables, and the search use case for much of this information is not clear; not having this information should not prevent the 15-year effort to create correct and up-to-date SPASE `NumericalData` records. That is, if only 10% of SPASE records have a given attribute, a search on it will not be useful. Also, some of the not-yet used metadata is not useful for search, such as `Valid{Min,Max}`, `FillValue`, and `RenderingHints`. This information would be useful if the SPASE record was being used for automated extraction and plotting. However, much more information is needed to enable automated extraction and even then (as we found with attempts to use SPASE for HAPI), given the issues described above, this may not be possible or too time-consuming. If it were possible, an application that uses it and could test the results should be identified first. As noted above, metadata that is not used by an application is likely to have problems.