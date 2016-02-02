# MANTA-ADM 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-adm - administer a Manta deployment

## SYNOPSIS

`manta-adm cn [-l LOG_FILE] [-H] [-o FIELD...] [-n] [-s] CN_FILTER`

`manta-adm genconfig "lab" | "coal"`

`manta-adm show [-l LOG_FILE] [-a] [-c] [-H] [-o FIELD...] [-s] SERVICE`

`manta-adm show [-l LOG_FILE] [-js] SERVICE`

`manta-adm update [-l LOG_FILE] [-n] [-y] [--no-reprovision] FILE [SERVICE]`

`manta-adm zk list [-l LOG_FILE] [-H] [-o FIELD...]`

`manta-adm zk fixup [-l LOG_FILE] [-n] [-y]`


## DESCRIPTION

The `manta-adm` command is used to administer various aspects of a Manta
deployment.  This command only operates on zones within the same datacenter.
The command may need to be repeated in other datacenters in order to execute it
across an entire Manta deployment.

`manta-adm cn`
  Show information about Manta servers in this DC.

`manta-adm genconfig`
  Generate a configuration for a COAL or lab deployment.

`manta-adm show`
  Show information about deployed services.

`manta-adm update`
  Update deployment to match a JSON configuration.

`manta-adm zk`
  View and modify ZooKeeper servers configuration.

With the exception of agents (which are not currently managed by this tool),
Manta components are deployed as SDC **zones** (also called **instances**).
Each zone is part of a **service**, which identifies its role in the system.

Services that are part of the Manta application include:

**authcache**
  Stores user identity information

**electric-moray**
  Moray proxy that handles sharding using consistent hashing

**jobpuller**
  Manages the archival of completed user compute jobs

**jobsupervisor**
  Manages the execution of user compute jobs

**loadbalancer**
  Handles SSL termination and loadbalancing for "webapi"

**madtom**
  Operational dashboard for component health

**marlin-dashboard**
  Operational dashboard for job activity

**marlin**
  Zones used to execute end user compute tasks

**medusa**
  Manages end user interactive shell sessions

**moray**
  Key-value store used to access PostgreSQL

**nameservice**
  internal DNS nameservers and ZooKeeper nodes

**ops**
  Manages asynchronous operations like garbage collection, metering, and auditing

**postgres**
  PostgreSQL databases used for storing object and job metadata

**storage**
  Stores actual object data

**webapi**
  Handles end user API requests

These services are described in much more detail in the Manta Operator's Guide.

The SDC SAPI service stores configuration about the "manta" application, each of
the above services, and each instance of the above service.  The information
reported by this tool is derived from SDC's internal APIs, including SAPI (for
service configuration), CNAPI (for compute node information), VMAPI (for zone
information), NAPI (for network information), and IMGAPI (for image information)
services.

Many subcommands produce tabular output, with a header row, one data record per
line, and columns separated by whitespace.  With any of these commands, you can
use options:

`-H, --omit-header`
  Do not print the header row.

`-o, --columns FIELD[,FIELD...]`
  Only print columns named `FIELD`.  You can specify this option multiple times
  or use comma-separated field names (or both) to select multiple fields.  The
  available field names vary by command and are described in the corresponding
  command section above.  In general, the default set of field names for each
  command is subject to change at any time.

Many commands also accept:

`-l, --log_file LOGFILE`
  Emit verbose log to LOGFILE.  The special string "stdout" causes output to be
  emitted to the program's stdout.


## SUBCOMMANDS


### "cn" subcommand

`manta-adm cn [-l LOG_FILE] [-H] [-o FIELD...] [-n] [-s] [CN_FILTER]`

The `manta-adm cn` subcommand is used to list SDC compute nodes being used in
the current Manta deployment in the current datacenter.  The default output is a
table with one row per compute node.  See above for information on the `-l`,
`-H`, and `-o` options.

`-n, --oneachnode`
  Instead of printing a table, emit a comma-separated list of matching
  hostnames, suitable for use with sdc-oneachnode(1)'s `-n` option.  See also
  manta-oneach(1).

`-s, --storage-only`
  Show only compute nodes with "storage" zones on them.

The optional `CN_FILTER` string can be used to provide any substring of a
compute node's hostname, server uuid, administrative IP address, compute id, or
storage ids.  All matching compute nodes will be reported.

Available fields for the `-o/--columns` option include "server\_uuid", "host",
"dc" (the datacenter name), "admin\_ip", "ram", "compute\_id", "storage\_ids",
and "kind" (which is either "storage" or "other").

Example: list basic info about all Manta CNs in this DC:

    # manta-adm cn

Example: list info about Manta CN with server uuid matching 7432ffc8:

    # manta-adm cn 7432ffc8

Example: list only storage nodes:

    # manta-adm cn -s

Example: list only the hostnames (and omit the header):

    # manta-adm cn -H -o host

Example: list hostnames in form suitable for "sdc-oneachnode -n":

    # manta-adm cn -n


### "genconfig" subcommand

`manta-adm genconfig "lab" | "coal"`

The `manta-adm genconfig` subcommand generates a JSON configuration file
suitable for use with `manta-adm update` that deploys an appropriate set of
services for a single-system Manta deployment.

The sole argument, `"coal"` or `"lab"`, determines the broad class of
deployment.  `"coal"` produces a configuration suitable for a small,
VM-in-a-laptop deployment, while `"lab"` produces a configuration suitable for
a larger server install.  The images used for each service are the images
configured in SAPI, which are generally the last images downloaded by
manta-init(1), so this command is sometimes used as a shortcut for identifying
the latest images that have been fetched for each service.


### "show" subcommand

`manta-adm show [-l LOG_FILE] [-a] [-c] [-H] [-o FIELD...] [-s] SERVICE`

`manta-adm show [-l LOG_FILE] [-js] SERVICE`

The `manta-adm show` subcommand reports information about deployed Manta
component zones.  The default output is a table with one row per deployed zone.
See above for information on the `-l`, `-H`, and `-o` options.

`-a, --all`
  Show zones deployed in all datacenters associated with this Manta deployment.
  By default, only zones deployed in the current datacenter are shown.  Many
  fields for zones deployed in other datacenters will not be available.

`-c, --bycn`
  Instead of showing tabular output with one row per zone sorted by service,
  group zones by the compute node on which each zone is deployed.  With
  `-a/--all`, all compute zones in other datacenters are grouped together, since
  compute node information is not available for remote datacenters.

`-s, --summary`
  Instead of showing tabular output with one row per zone, show tabular output
  with one row per group of zones having the same "service", "image", and
  "shard" properties (or just "image", for zones to which "shard" does not
  logically apply).  The count for each group is also reported.  With
  `-j/--json`, the same information is presented in JSON form.

`-j, --json`
  Instead of the default text-based output, emit a JSON representation of the
  summary information reported with the `-s/--summary` command.  This format is 
  suitable for use with `manta-adm update`.  This option cannot be combined with
  `-c/--bycn`, `-a/--all`, `-H/--omit-header`, or `-o/--columns`, and it _must_
  be combined with `-s/--summary`.  (Future versions of this command may support
  a different JSON-based report when `-j/--json` is used without
  `-s/--summary`.)  For details on the JSON format, see `manta-adm update`.

If `SERVICE` is specified, then only zones whose service name is `SERVICE` will
be reported.

Available fields for the `-o/--columns` option include:

* `datacenter`: the name of the datacenter in which this zone is deployed
* `image`: the uuid of the zone's image
* `primary_ip`: the primary IP address for this zone
* `service`: the name of the service this zone is part of
* `shard`: the metadata shard number for this zone.  This is only meaningful
  for "moray" and "postgres" zones.
* `storage_id`: the internal storage id for this zone.  This is only present
  for "storage" zones.
* `zonename`: the full unique identifier for this zone
* `zoneabbr`: the first 8 characters of "zonename"
* `gz_host`: the hostname of the CN on which this zone is deployed
* `gz_admin_ip`: the primary IP address for the CN on which this zone is
  deployed
* `count` (summary mode only): the number of zones having the same "service",
  "image", and "shard" fields (where meaningful)

Note that the "count" field is only meaningful when `-s/--summarize` is
specified.  The only other fields that are meaningful when `-s/--sumarize` is
specified are "service", "image", and "shard".

Example: list all Manta zones in the current DC

    # manta-adm show

Example: list zones in the current DC by compute node

    # manta-adm show -c

Example: summarize Manta zones in the current DC

    # manta-adm show -s

Example: list all Manta zones in all datacenters (no IP info available)

    # manta-adm show -a

Example: show only postgres zones in the current datacenter

    # manta-adm show postgres


### "update" subcommand

`manta-adm update [-l LOG_FILE] [-n] [-y] [--no-reprovision] FILE [SERVICE]`

The `manta-adm update` command updates a Manta deployment to match the JSON
configuration stored at path `FILE`.  The JSON configuration describes the
precise number of instances that should be running for each version (image) of
each type of service on each server.  The update process will involve some
number of zone deployments, undeployments, and reprovisions.  For example, if
there are 3 "webapi" instances deployed of version "X" on a given server and the
configuration specifies that there should be 1 "webapi" instance at version "Y",
then one of the existing "webapi" instances will be reprovisioned to version "Y"
and the others will be removed.

The command automatically manages the sequence and concurrency of updates to
minimize impact to a running system.  Because running the command always
compares the current deployment to the one provided in the configuration file,
it is idempotent.  If there are any failures, you can re-run `manta-adm update`
as needed to bring the system to the desired configuration.

**This command is primarily intended for use with stateless services.  Extreme
care should be taken when using it with stateful services like "postgres" or
"storage".  See the Manta Operator's Guide for the appropriate procedures for
upgrading all components.**

Options:

`-n, --dryrun`
  Print what changes would be made without actually making them.

`-y, --confirm`
  Bypass all confirmation prompts.

`--no-reprovision`
  When upgrading a zone, always provision a new zone and deprovision the
  previous one, rather than reprovisioning the existing one.

See above for information about the `-l/--log_file` option.

If `SERVICE` is specified, then only instances of the named service are
changed.

The JSON configuration format consists of an object with several levels of
properties:

1. Top-level properties are server uuids.  Everything below a given server uuid
   describes instances deployed on that server.
2. The next-level properties are service names.
3. For services that use shards ("postgres" and "moray"), the next-level
   property names are shard numbers.
4. The next-level property names are image uuids, which describe the specific
   image (version) of zones should be deployed.
5. The values at the leafs are integers describing the number of zones for that
   image uuid should be deployed for this service on this server.

Here's an example snippet:

    {
        "44454c4c-5700-1047-8051-b3c04f585131": {
            "nameservice": {
                "59ef6322-6968-11e5-987a-0bd10a3d6e65": 3
            },
            "postgres": {
                "1": {
                    "0a8692f6-6968-11e5-a997-3334c877b2f3": 3
                },
                "2": {
                    "0a8692f6-6968-11e5-a997-3334c877b2f3": 3
                }
            },
            ...
        }
    }

This configuration denotes that on the server with uuid
"44454c4c-5700-1047-8051-b3c04f585131", there should be:

* three "nameservice" instances using image
  "59ef6322-6968-11e5-987a-0bd10a3d6e65",
* three "postgres" instances in shard 1 using image
  "0a8692f6-6968-11e5-a997-3334c877b2f3", and
* three "postgres" instances in shard 2 using image
  "0a8692f6-6968-11e5-a997-3334c877b2f3".

The starting point for an update operation is usually the output of `manta-adm
show -sj`.  From that configuration, you can:

* scale up or down the number of any component by increasing or decreasing the
  counts,
* upgrade all instances of a component by changing the image uuid for it, and
* perform rolling upgrades by adding a second image uuid for a service with
  count "1", then updating repeatedly with more instances of the second image
  and fewer instances of the first image.

subject to the caveats described above for stateful services.

This tool does not provide an interface for undeploying or upgrading specific
zones by zonename.

Example: update the current deployment to the configuration in `newconfig.json`:

    # manta-adm update newconfig.json

Example: update only "moray" instances to the configuration in `newconfig.json`:

    # manta-adm update newconfig.json moray


### "zk" subcommand

`manta-adm zk list [-l LOG_FILE] [-H] [-o FIELD...]`

`manta-adm zk fixup [-l LOG_FILE] [-n] [-y]`

The `manta-adm zk` subcommand provides subcommands for viewing and repairing the
list of ZooKeeper peers.  The `manta-adm zk list` command reports a tabular view
of the ZooKeeper servers used for the current Manta deployment.  The `manta-adm
zk fixup` command compares the ZooKeeper configuration (defined by the
`ZK_SERVERS` and `ZK_ID` SAPI metadata properties) to the list of deployed
nameservice zones, reports any discrepancies or other issues, and optionally
repairs certain kinds of issues.  If repairs are made, only metadata is changed.
This tool is intended for cases where a ZK server has been undeployed and the
configuration needs to be updated, or where deployment failed and left stale
configuration, or other unusual cases where the configuration does not match the
list of deployed nameservers.

See above for information about the `-l`, `-H`, and `-o` options for
`manta-adm zk list`.  Fields available for use with `-o` include "ord" (the
ordinal number of each server), "datacenter", "zoneabbr", "zonename", "ip", and
"port".

The `manta-adm zk fixup` command supports options:

`-n, --dryrun`
  Print what changes would be made without actually making them.

`-y, --confirm`
  Bypass all confirmation prompts.

It also supports the `-l/--log_file` option described above.


## EXIT STATUS

`0`
  Success

`1`
  Generic failure.

`2`
  The command-line options were not valid.


## COPYRIGHT

Copyright (c) 2016 Joyent Inc.

## SEE ALSO

json(1), Manta Operator's Guide
