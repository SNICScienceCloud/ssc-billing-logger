Build prerequisites
===================
Rust toolchain

Runtime requirements
====================
* OpenSSL libraries
* Network API access to OpenStack admin endpoint
* radosgw-admin command line tool

Building
========
Install a Rust toolchain, typically via the `rustup` tool from https://rustup.rs/ .

For example, for a personal installation somewhere else than your home directory:

    export RUSTUP_HOME=/scratch/you/rustup
    export CARGO_HOME=/scratch/you/cargo
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- --no-modify-path
    source $CARGO_HOME/env

Clone the respository or download a release tarball and build it:

    git clone https://github.com/SNICScienceCloud/ssc-billing-logger.git
    cd ssc-billing-logger/
    cargo build --release

The resulting `ssc-billing-logger` executable will be in the `./target/release/` directory which can be deployed to the billing container.

It depends only on the C runtime and OpenSSL libraries, so as long as the build system and deployment systems are similar enough, you can build on a separate machine.

Setup
=====
Create a configuration file from the template. The format is the same as for the previous Ceilometer-using implementation.

Fields for `password` and `keystone_url` should be taken from the local OpenStack installation.
Remember to customize the site/region with centre name in allcaps. If no proxy is desired, remove the `socks_proxy_url` field completely.

In version 0.3.0 the previous `resource` field is replaced with a `resources` dictionary, mapping from domain to the resource to report as in order to support things like local rounds.

Create the data directory and its subdirectories:

    mkdir -p $datadir/{logger-state,records}

Put a `costs.json` file in the `logger-state` directory, an example file can be seen in `samples/sample_costs.json`.
Compute instances are billed by instance flavour per hour. Storage is billed per gigabyte and hour. There is a discount on volumes if they're used by a compute instance.

In version 0.3.0 there is an additional level of nesting beneath the region with one set of costs for each resource in order to support different costs for local rounds.

Any domains that do not have an associated resource will not be reported.

Cron jobs
=========

    # run the logger once per hour
    50 * * * * /opt/ssc-billing-logger/ssc-billing-logger -c /opt/ssc-billing-logger/ssc-billing-logger.conf
    # send results upstream to SGAS
    0  * * * * /opt/sgas-cr-registrant/bin/sgas-cr-registrant -c /opt/sgas-cr-registrant/etc/sgas-cr-registrant.conf

Usage
=====
* `-c config.conf` -- override the configuration file location
* `--dry-run` -- do not emit any XML or state information
* `--force` -- generate XML regardless of if the current hour has been processed already
* `--save-snapshot snap.json` -- Save a snapshot of cloud state for testing
* `--load-snapshot snap.json` -- Run on snapshot data instead of live data

Notes
=====
`fetch-deleted-volumes.sh` is no longer needed as we always query for live information from the system instead of using Ceilometer data which could contain deleted volumes.
