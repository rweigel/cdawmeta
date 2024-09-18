https://sourceforge.net/p/autoplot/code/HEAD/tree/autoplot/trunk/CdfJavaDataSource/src/org/autoplot/cdf/CdfVirtualVars.java

https://github.com/rweigel/CDAWlib/blob/952a28b08658413081e75714bd3b9bd3ba9167b9/virtual_funcs.pro

https://cdaweb.gsfc.nasa.gov/registry/hdp/hapi/hapiHtml.html#url=https://cdaweb.gsfc.nasa.gov/hapi&id=VOYAGER1_10S_MAG@0,VOYAGER1_10S_MAG@1

How is https://www.w3.org/TR/vocab-dcat/ related to https://github.com/ESIPFed/science-on-schema.org/?

https://heliophysicsdata.gsfc.nasa.gov/websearch/dispatcher?action=TEXT_SEARCH_PANE_ACTION&inputString=VOYAGER1_10S_MAG

# From Bernie

That hapi implementation is running inside the HDP database so it searches the spase documents for ones with //AccessInformation/AccessURL[name = ‘CDAWeb’ and ProductKey = ‘whatever’].  “Sufficiently described” meant that the cdaweb information was in the spase documents.  At the time that code was written, there were many cdaweb datasets that didn’t have spase descriptions or the spase descriptions didn’t contain the cdaweb access information.  Even now, spase is usually missing the most recent cdaweb datasets but it’s not too far behind.

This https://heliophysicsdata.gsfc.nasa.gov/queries/index_resolver.html describes a resolver service.  To get all datasets at once, you might want to use https://heliophysicsdata.gsfc.nasa.gov/queries/CDAWeb_SPASE.html.  Also note that https://cdaweb.gsfc.nasa.gov/WebServices/REST/#Get_Datasets returns the spase ResourceID.  For example,

$ curl -s -H "Accept: application/json" “https://cdaweb.gsfc.nasa.gov/WS/cdasr/1/dataviews/sp_phys/datasets?idPattern=AC_H0_MFI” |jq -jr '.DatasetDescription[]|(.Id,", ",.SpaseResourceId,"\n")'

curl -s -H "Accept: application/xml" "https://heliophysicsdata.gsfc.nasa.gov/WS/hdp/1/Spase?ResourceID=spase://NASA/NumericalData/ACE/Ephemeris/PT12M"
