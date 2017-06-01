#Dependencies
Python 3.x
arrow
pymaybe
requests
PySocks

#Setup
Create a configuration file from the template. 
Fields for `password`, `keystone_url`, `ceilometer_url` should be grabbed from the local OpenStack installation.
Remember to customize the site/region.

{
	"username": "admin",
	"password": "0123456789abcdef",
	"project": "admin",
	"domain": "default",
	"keystone_url": "http://1.2.3.4:35357/v3",
	"ceilometer_url": "http://1.2.3.4:8777/v2",
	"socks_proxy_url": "socks5://localhost:9998",
	"site": "HPC2N",
	"region": "HPC2N",
	"datadir": "/opt/ssc-billing-logger/sgas-cr"
}

Create the data directory and its subdirectories:

    mkdir -p $datadir/{records,state}

Put a `costs.json` file in the `state` directory, an example file can be seen in `samples/costs.json`.
