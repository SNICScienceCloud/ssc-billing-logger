Dependencies
============
Python 3.x
arrow
pymaybe
requests
PySocks

Setup
=====
Create a configuration file from the template. 
Fields for `password`, `keystone_url`, `ceilometer_url` should be grabbed from the local OpenStack installation.
Remember to customize the site/region with centre name in allcaps. If no proxy is desired, remove the `socks_proxy_url` field completely.

Create the data directory and its subdirectories:

    mkdir -p $datadir/{logger_state,records}

Put a `costs.json` file in the `logger_state` directory, an example file can be seen in `samples/sample_costs.json`.
Compute instances are billed by instance flavour per hour. Block storage is billed per gigabyte and hour.

Cron jobs
=========

    10 * * * * /usr/bin/python3 /usr/local/share/ssc-billing-logger/fetch-deleted-volumes.sh > /var/spool/sgas-cr/logger-state/deleted-volumes.tsv
    15 * * * * /usr/bin/python3 /usr/local/share/ssc-billing-logger/ssc-billing-logger.py -c /usr/local/share/ssc-billing-logger/ssc-billing-logger.conf
    0  * * * * /usr/local/share/sgas-cr-registrant/bin/sgas-cr-registrant -c /usr/local/share/sgas-cr-registrant/etc/sgas-cr-registrant.conf

Usage
=====
* -c config.conf -- override the configuration file location
* -s -- run for a single hour instead of the full period since last reporting period.
