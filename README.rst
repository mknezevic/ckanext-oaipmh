OAI-PMH Plugin for CKAN
=======================
This plugin provides two things: a harvester which can be configured to harvest
datasets from a OAI-PMH data source and a fully compatible interface for OAI-PMH
which can list all datasets and resources in CKAN for OAI-PMH.

Harvester
---------

The steps to install harvester, add the extension name 'oaipmh_harvester'
to the configuration option 'ckan.plugins' of the CKAN ini file in use.

After this restart CKAN. Then navigate to http://ckan-url/harvest/new and add a harvesting source.
For this source do:
  * Fill in URL to a OAI-PMH repository.
  * Select 'Source Type' to be 'OAI-PMH'.
  * In configuration, you may specify selected sets that should be imported 
    and additional metadata formats (metadata in oai_dc will always be harvested).
    Configuration should be specified in JSON format, e.g.
    {"set": ["set1","set2"], "metadata_formats": ["ead"]}
    If configuration is left empty, metadata records (only) in oai_dc format from all sets will be harvested 
  * Click save

To see the list of harvesting sources go to http://ckan-url/harvest

You may need to configure your fetch and gather consumer to be run as daemons or
via a the paster commands.

This is clearly documented in ckanext-harvest extension, see it here:

 https://github.com/okfn/ckanext-harvest/blob/master/README.rst
 


Interface
---------

The interface is simple to install, add the extension name 'oaipmh' to the
configuration option 'ckan.plugins' of the CKAN ini file in use.
