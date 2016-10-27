# redisnio
A NIO2 plugin that allows the EPICS Archiver Appliance to store data in Redis.

This should let the PlainPBStoragePlugin store PB chunks in Redis. 
There should be enough functionality to support both the ETLSource and ETLDest interfaces; which should enable you to use Redis at any stage in your sequence of data stores. 
Use a URI with a prefix like `redis://localhost:port/archiver` as the rootFolder of the PlainPBStoragePlugin.
For example, `export ARCHAPPL_SHORT_TERM_FOLDER=redis://localhost:6379/quickstart` lets you use Redis as your short term store when you use the default policy file in a quickstart install.

This works best with Redis versions >= redis-3.2.5.
