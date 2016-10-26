# redisnio
A NIO2 plugin that allows the EPICS Archiver Appliance to store data in Redis.

This has support for an ETLSource and a ETLDest; so this should let you use Redis at any stage in you data stores. 
Use a "redis://localhost:port" prefix to store data in redis.
For example, export ARCHAPPL_SHORT_TERM_FOLDER=redis://localhost:6379/quickstart lets you use Redis as your short term store when you use the default policy file.
