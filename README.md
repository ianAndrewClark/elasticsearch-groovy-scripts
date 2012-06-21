elasticsearch-groovy-scripts
============================

A set of utility scripts to administer elasticsearch - backup, restore, and more.

Implemented in groovy, so you will need java and groovy installed. 

Using groovy makes these a little more powerful than simple curl or shell 
scripts, the groovy process actually connects to the elasticsearch cluster and 
communicates directly with the nodes (a TransportClient).