There are many instances where Nand's server has a type of `double`, but I get `integer` based on CDF master metadata. The reason may be that Nand is using different non-CDF master metadata. My understanding is that CDAWeb does not correct errors in non-master CDFs, but I could see it causing problems with people who read CDFs posted at CDAWeb directly without using the master (many do). To see the occurrences, search [the logfile](http://mag.gmu.edu/git-data/cdawmeta/data/hapi/compare.log) for `double`.

When attempting to compute sampleStart/Stops from data returned from the CDAS REST server, I noticed that a dataset has a time variable that is not monotonic. Nand's server has this too:

https://hapi-server.org/servers/#server=CDAWeb&dataset=AMPTECCE_H0_MEPA@3&parameters=Time&start=1989-01-10T23:36:42Z&stop=1989-01-10T23:36:43Z&return=data&format=csv&style=noheader

Should time always be monotonic?