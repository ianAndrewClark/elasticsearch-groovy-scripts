elasticsearch-groovy-scripts
============================

A set of utility scripts to administer elasticsearch - backup, restore, and more.

Implemented in groovy, so you will need java and groovy installed. 

Using groovy makes these a little more powerful than simple curl or shell 
scripts, the groovy process actually connects to the elasticsearch cluster and 
communicates directly with the nodes (a TransportClient).

Backup example
--------------

    ./backup.groovy -host localhost -index yourindex -cluster yourcluster -output /tmp/somewhere

The output will be a settings file, and mappings file in json, along with all data in a smile json gzip'd file.
Works using a scroll query so it is a snapshot in time, you will probably want to flush and stop indexing to the index before running.

NB:- requires the source field to be enabled!


Restore example
--------------

    ./restore.groovy -host localhost -index yourindex -cluster yourcluster -input /tmp/somewhere

NB:- currently has a hardcoded bulk index request size, this might need tuning depending on your documents size.
