# MANTA-ADM 1 "2019" Manta "Manta Operator Commands"

## NAME

manta-adm - administer a Manta deployment

## SYNOPSIS

`manta-adm accel-gc SUBCOMMAND... [OPTIONS...]`

`manta-adm alarm SUBCOMMAND... [OPTIONS...]`

`manta-adm cn [-l LOG_FILE] [-H] [-o FIELD...] [-n] [-s] CN_FILTER`

`manta-adm genconfig "lab" | "coal"`

`manta-adm genconfig [--directory=DIR] --from-file=FILE`

`manta-adm show [-l LOG_FILE] [-a] [-c] [-H] [-o FIELD...] [-s] SERVICE`

`manta-adm show [-l LOG_FILE] [-js] SERVICE`

`manta-adm update [-l LOG_FILE] [-n] [-y] [--no-reprovision] [--skip-verify-channel] FILE [SERVICE]`

`manta-adm zk list [-l LOG_FILE] [-H] [-o FIELD...]`

`manta-adm zk fixup [-l LOG_FILE] [-n] [-y]`


## DESCRIPTION

The `manta-adm` command is used to administer various aspects of a Manta
deployment.  This command only operates on zones within the same datacenter.
The command may need to be repeated in other datacenters in order to execute it
across an entire Manta deployment.

`manta-adm accel-gc`
  Administer garbage-collector zones and accelerated gc-enabled accounts.

`manta-adm alarm`
  List and configure amon-based alarms for Manta.

`manta-adm cn`
  Show information about Manta servers in this DC.

`manta-adm genconfig`
  Generate a configuration for a COAL, lab, or multi-server deployment.

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

**boray**
  PostgreSQL interface for manta buckets system

**buckets-postgres**
  PostgreSQL databases used for storing object metadata for the buckets system

**buckets-api**
  Handles end user buckets API requests

**electric-boray**
  Boray proxy for buckets that handles sharding using consistent hashing

**electric-moray**
  Moray proxy that handles sharding using consistent hashing

**garbage-collector**
  Processes delete records in specialized Postgres tables

**loadbalancer**
  Handles SSL termination and loadbalancing for "webapi"

**madtom**
  Operational dashboard for component health

**moray**
  Key-value store used to access PostgreSQL

**nameservice**
  internal DNS nameservers and ZooKeeper nodes

**ops**
  Manages asynchronous operations like garbage collection, metering, and auditing

**postgres**
  PostgreSQL databases used for storing object metadata

**pgstatsmon**
  Monitoring system for Postgres

**prometheus**
  Time-series database that aggregates Manta and Triton metrics

**reshard**
  Automated resharding system for Manta

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

Commands that make changes support:

`-n, --dryrun`
  Print what changes would be made without actually making them.

`-y, --confirm`
  Bypass all confirmation prompts.

**Important note for programmatic users:** Except as noted below, the output
format for this command is subject to change at any time. The only subcommands
whose output is considered committed are:

* `manta-adm cn`, only when used with the "-o" option
* `manta-adm show`, only when used with either the "-o" or "-j" option
* `manta-adm zk list`, only when used with the "-o" option

The output for any other commands may change at any time.  The `manta-adm alarm`
subcommand is still considered an experimental interface.  All other documented
subcommands, options, and arguments are committed, and you can use the exit
status of the program to determine success or failure.


## SUBCOMMANDS

### "accel-gc" subcommand

`manta-adm accel-gc show [-j]`

`manta-adm accel-gc update CONFIG_FILE`

`manta-adm accel-gc gen-shard-assignment`

`manta-adm accel-gc genconfig [-m MAX_CNS] [-a SERVICE...] [-i] IMAGE_UUID
NCOLLECTORS`

`manta-adm accel-gc enable ACCOUNT_LOGIN`

`manta-adm accel-gc disable ACCOUNT_LOGIN`

`manta-adm accel-gc accounts [-H] [-o FIELD...]`

Accelerated garbage-collection is a low-latency alternative to the backup-based
garbage-collection pipeline in the Mola software consolidation. The system
allows users to trade Manta's snaplink functionality for faster storage space
reclamation.

Accounts that do not have any snaplinks can be marked as snaplink-disabled,
indicating that all of the objects owned by the account have a single reference
in the metadata tier. This constrained setting allows garbage-collector zones,
each of which is responsible for processing deleted objects on some subset of
the index shards, to coordinate object file cleanup as soon as a front-door
delete request completes.

The `manta-adm accel-gc` subcommand provides tools that allow operators:

* manage the assignment of metadata shards to garbage-collector instances
* toggle accelerated garbage-collection for individual accounts

`manta-adm accel-gc show -j`

Dump a mapping that shows which shards are assigned to which garbage-collectors.
The output of this command can be re-purposed as input to `manta-adm
accel-gc update`. The interaction between these commands is similar to that
between `manta-adm show` and `manta-adm update`.

`manta-adm accel-gc update CONFIG_FILE`

Update the mapping from shards to garbage-collectors. This will require restarting
the garbage-collectors so that they pick up the new assigned shards. CONFIG_FILE
here should have the same format as the output of `manta-adm accel-gc show -j`.

`manta-adm accel-gc genconfig [-m MAX_CNS] [-a SERVICE...] [-i] IMAGE_UUID
NCOLLECTORS`

Generate a service deployment layout (interpretable by manta-adm update) by
layering NCOLLECTORS garbage-collector zones onto the existing deployment
layout in a minimally disruptive fashion. By default, this means the command
will:

* avoid colocating garbage-collector zones with loadbalancer or nameservice
  zones.
* add garbage-collectors to at most MAX_CNS CNs meeting the previous criterion
  if specified, otherwise use as many CNs meeting the above criterion as are
  available.
* distribute garbage-collectors as evenly as possible amongest the CNs between
  the above criteria.

In some deployments these criteria cannot be met. To generate a layout that
does not meet the criteria pass the -i flag. The flag should not be used in
production deployments.

To change the list of services to avoid, pass multiple SERVICEs in a
comma-separated list or with repeated -a flags.

`manta-adm accel-gc gen-shard-assignment`

Generate a mapping from shards to garbage-collectors based on SAPI metadata that
distributes index shards to garbage-collectors as evenly as possible.

`manta-adm accel-gc enable ACCOUNT_LOGIN`

Enable accelerated garbage-collection for the account. No snaplinks to data
owned by this account will be allowed after this command completes. Note that
this command does not get rid of any existing snaplinks to objects owned by the
account. It is not safe to use accelerated garbage-collection for an account
that may own snaplinked data.

`manta-adm accel-gc disable ACCOUNT_LOGIN`

Disable accelerated garbage-collection for the account. Objects owned by the
account which are deleted while the account has accelerated garbage-collection
disabled will have to be garbage-collected with the offline garbage-collection
defined in the Mola software consolidation.

`manta-adm accel-gc accounts [-H] [-o FIELD]`

List accounts for which accelerated garbage-collection is enabled in a
human-readable format.

### "alarm" subcommand

`manta-adm alarm close ALARM_ID...`

`manta-adm alarm config probegroup list [-H] [-o FIELD...]`

`manta-adm alarm config show`

`manta-adm alarm config update [-n] [-y] [--unconfigure]`

`manta-adm alarm config verify [--unconfigure]`

`manta-adm alarm details ALARM_ID...`

`manta-adm alarm faults ALARM_ID...`

`manta-adm alarm list [-H] [-o FIELD...] [--state=STATE]`

`manta-adm alarm maint create CREATE_OPTIONS`

`manta-adm alarm maint delete WIN_ID...`

`manta-adm alarm maint list [-H] [-o FIELD...]`

`manta-adm alarm maint show`

`manta-adm alarm metadata events`

`manta-adm alarm metadata ka [EVENT_NAME...]`

`manta-adm alarm notify on|off ALARM_ID...`

`manta-adm alarm show`

The `manta-adm alarm` subcommand provides several tools that allow operators to:

* view and configure amon probes and probe groups (`config` subcommand)
* view open alarms (`show`, `list`, `details`, and `faults` subcommands)
* configure notifications for open alarms (`notify` subcommand)
* view local metadata about alarms and probes (`metadata` subcommand)
* view and configure amon maintenance windows (`maint` subcommand)

The primary commands for working with alarms are:

* `manta-adm alarm config update`: typically used during initial deployment and
  after other deployment operations to ensure that the right set of probes and
  probe groups are configured for the deployed components
* `manta-adm alarm show`: summarize open alarms
* `manta-adm alarm details ALARM_ID...`: report detailed information (including
  suggested actions) for the specified alarms
* `manta-adm alarm close ALARM_ID...`: close open alarms, indicating that they
  no longer represent issues

For background about Amon itself, probes, probegroups, and alarms, see the
Triton Amon reference documentation.

As with other subcommands, this command only operates on the current Triton
datacenter.  In multi-datacenter deployments, alarms are managed separately in
each datacenter.

Some of the following subcommands can operate on many alarms.  These subcommands
exit failure if they fail for any of the specified alarms, but the operation may
have completed successfully for other alarms.  For example, closing 3 alarms is
not atomic.  If the operation fails, then 1, 2, or 3 alarms may still be open.

`manta-adm alarm close ALARM_ID...`

Close the specified alarms.  These alarms will no longer show up in the
`manta-adm alarm list` or `manta-adm alarm show` output.  Amon purges closed
alarms completely after some period of time.

If the underlying issue that caused an alarm is not actually resolved, then a
new alarm may be opened for the same issue.  In some cases, that can happen
almost immediately.  In other cases, it may take many hours for the problem to
resurface.  In the case of transient issues, a new alarm may not open again
until the issue occurs again, which could be days, weeks, or months later.  That
does not mean the underlying issue was actually resolved.

As mentioned above, this command attempts to separately close each of the
specified alarms.  It's possible for some of the specified alarms to be closed
even if others were not.


`manta-adm alarm config probegroup list [-H] [-o FIELD...]`

List configured probe groups in tabular form.  This is primarily useful in
debugging unexpected behavior from the alarms themselves.  The `manta-adm alarm
config show` command provides a more useful summary of the probe groups that are
configured.

`manta-adm alarm config show`

Shows summary information about the probes and probe groups that are configured.
This is not generally necessary but it can be useful to verify that probes are
configured as expected.

`manta-adm alarm config update [-n] [-y] [--unconfigure]`

Examines the Manta components that are deployed and the alarm configuration
(specifically, the probes and probe groups deployed to monitor those components)
and compares them with the expected configuration.  If these do not match,
prints out a summary of proposed changes to the configuration and optionally
applies those changes.

If `--unconfigure` is specified, then the tool removes all probes and probe
groups.

This is the primary tool for updating the set of deployed probes and probe
groups.  Operators would typically use this command:

- during initial deployment to deploy probes and probe groups
- after deploying (or undeploying) any Manta components to deploy (or remove)
  probes related to the affected components
- after updating the `manta-adm` tool itself, which bundles the probe
  definitions, to deploy any new or updated probes
- at any time to verify that the configuration matches what's expected

This operation is idempotent.

This command supports the `-n/--dryrun` and `-y/--confirm` options described
above.

`manta-adm alarm config verify [--unconfigure]`

Behaves exactly like `manta-adm alarm config update --dryrun`.

`manta-adm alarm details ALARM_ID...`

Prints detailed information about any number of alarms.  The detailed
information includes the time the alarm was opened, the last time an event was
associated with this alarm, the total number of events associated with the
alarm, the affected components, and information about the severity, automated
response, and suggested actions for this issue.

`manta-adm alarm faults ALARM_ID...`

Prints detailed information about the faults associated with any number of
alarms.  Each fault represents a particular probe failure.  The specific
information provided depends on the alarm.  If the alarm related to a failed
health check command, then the exit status, terminating signal, stdout, and
stderr of the command are provided.  If the alarm relates to an error log entry,
the contents of the log entry are provided.  There can be many faults associated
with a single alarm.

`manta-adm alarm list [-H] [-o FIELD...] [--state=STATE]`

Lists alarms in tabular form.  `STATE` controls which alarms are listed, which
may be any of "open", "closed", "all", or "recent".  The default is "open".

See also the `manta-adm alarm show` command.

`manta-adm alarm maint create CREATE_OPTIONS`

Creates (schedules) an Amon maintenance window, which is a period of time and a
scope for which alarm notifications are suspended.  Maintenance windows have a
start time, an end time, and an operator-provided notes field (typically used to
reference a ticket number in some other system).  By default, maintenance
windows affect all notifications for an account (and so Manta maintenance
windows affect all Manta-related notifications), but they can be scoped to a
specific set of probes, probe groups, or machines.

During maintenance windows, Amon continues to execute all probe checks and it
continues to open new alarms for failing probe checks.  However, faults created
during a maintenance window that are within the scope of that window are
reported as "maintenance faults", and such faults do not trigger notifications.

As an example, suppose an operator creates a maintenance window for the period
today between 0200Z and 0400Z scoped to machine "lb7".  At 0214Z, Amon detects a
failure for a "log-scan" probe on machine "lb7" that would normally open a new
alarm and send notifications.  The alarm is opened as usual.  Because the event
happened within the maintenance window's time period and within its scope
(namely, machine "lb7"), a new maintenance fault is created, not a regular
fault, and no notifications are sent out.  But the alarm remains open until an
operator closes it.  A probe check failure for "lb7" after 0400Z would result in
a normal fault being created for the same alarm, and notifications would be
sent.  Similarly, a probe check failure at 0300Z for a different machine would
result in notifications being sent, even if the resulting fault would be
attached to the same alarm (e.g., because the "lb7" probe and this new probe are
in the same probe group).

The following three option-arguments are always required:

`--start START_TIME`
  Specifies the start time of the maintenance window.  `START_TIME` should be an
  ISO 8601 timestamp, or else the special string `now`, which means that the
  window should begin immediately.

`--end END_TIME`
  Specifies the end time of the maintenance window.  `END_TIME` should be an ISO
  8601 timestamp, and it must be later than the specified start time.

`--notes NOTES`
  Provides arbitrary notes to be recorded with the window.  This is intended for
  operators to reference tickets or other identifiers in other systems.  The
  system ignores the contents of this field except to report it back via the
  other subcommands.

You may also specify:

`--machine MACHINE_UUID, --probe PROBE_UUID, --probegroup GROUP_UUID`
  Limits the scope of the maintenance window so that it only affects the
  specified machines, probes, or probe groups.  You can specify any one of these
  options multiple times (e.g., to specify multiple machines), but you cannot
  mix these options together.  The values are only validated for basic syntax.
  They are not validated against the set of deployed machines, probes, or probe
  groups.

Note that Amon automatically deletes maintenance windows whose end time has
passed.  This tool does not allow you to create maintenance windows whose end
time is in the past.

Example: create an alarm for the period between 0200Z and 0400Z on July 17,
2017 associated with ticket `CM-123`

    # manta-adm alarm maint create --start=2017-07-17T02:00:00Z \
        --end=2017-07-17T04:00:00Z --notes "CM-123"

`manta-adm alarm maint delete WIN_ID...`

Deletes (cancels) the maintenance windows with identifiers `WIN_ID...`.  The
windows will no longer show up in the `manta-adm alarm maint list` or `manta-adm
alarm maint show` output, and Amon will resume sending notifications for events
that would have fallen within the window's time period and scope.

`WIN_ID` is Amon's integer identifier for the window.  You can retrieve this
from the `manta-adm alarm maint list` or `manta-adm alarm maint show` commands.

This command attempts to separately delete each of the specified windows.  If it
fails to delete any of them (e.g., because they're not valid window identifiers
or because of a transient problem with Amon), it may still have deleted others.

`manta-adm alarm maint list [-H] [-o FIELD...]`

Lists basic information about outstanding maintenance windows.  This command is
intended when you want tabular output or specific fields.  See the `manta-adm
alarm maint show` command for a more useful human-readable summary.

`manta-adm alarm maint show`

Summarizes each outstanding maintenance window.  This is intended for human
operators, not programmatic tools.  The output format may change in future
versions.

`manta-adm alarm metadata events`

List the names for all of the events known to this version of `manta-adm`.  Each
event corresponds to a distinct kind of problem.  For details about each one,
see `manta-adm alarm metadata ka`.  The list of events comes from metadata
bundled with the `manta-adm` tool.

`manta-adm alarm metadata ka [EVENT_NAME...]`

Print out knowledge articles about each of the specified events.  This
information comes from metadata bundled with the `manta-adm` tool.  If no events
are specified, prints out knowledge articles about all events.

Knowledge articles include information about the severity of the problem, the
impact, the automated response, and the suggested action.

`manta-adm alarm notify on|off ALARM_ID...`

Enable or disable notifications for the specified alarms.  Notifications are
generally configured through Amon, which supports both email and XMPP
notification for new alarms and new events on existing, open alarms.  This
command controls whether notifications are enabled for the specified alarms.

`manta-adm alarm show`

Summarize open alarms.  For each alarm, use the `manta-adm alarm details`
subcommand to view more information about it.


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

Example: list storage CNs with their associated storage id (used in object
metadata) and compute ids (vestigial):

    # manta-adm cn -o host,admin_ip,compute_id,storage_ids storage

### "genconfig" subcommand

`manta-adm genconfig "lab" | "coal"`

`manta-adm genconfig [--directory=DIR] --from-file=FILE`

The `manta-adm genconfig` subcommand generates a JSON configuration file
suitable for use with `manta-adm update`.  The images used for each service are
the images configured in SAPI, which are generally the last images downloaded by
manta-init(1), so this command is sometimes used as a shortcut for identifying
the latest images that have been fetched for each service.

When the first argument is `"coal"`, the command produces a configuration
suitable for a small VM-in-a-laptop deployment.  The configuration is always
emitted to stdout.

When the first argument is `"lab"`, the command produces a configuration
suitable for a larger single-server install.  The configuration is always
emitted to stdout.

The `--from-file=FILE` form can be used to generate a configuration suitable for
a much larger, production-style deployment.  `FILE` is a JSON file in the format
specified below that describes the parameters of the deployment, including the
number of metadata shards and the set of availability zones, racks, and servers.
This form attempts to create a deployment that will survive failures of any
component, server, rack, or availability zone as long as sufficient servers,
racks, and availability zones are included in the input file.  Availability zone
and rack information can be omitted from the file, in which case the tool will
generate a configuration ignoring rack-level and AZ-level considerations.  This
tool uses a number of heuristics, and the output should be verified.

By default, the generated configuration is emitted to stdout.  With the
`--directory` option, the configuration will be written to files in the
specified directory named by availability zone.  This option must be used if the
servers in `FILE` span more than one availability zone.

The input JSON file `FILE` should contain a single object with properties:

`nshards` (positive integer)
  the number of database shards to create.

`servers` (array of objects)
  the list of servers available for deployment

Each element of `servers` is an object with properties:

`type` (string: either `"metadata"` or `"storage"`)
  identifies this server as a target for metadata services or storage services.
  It's not strictly required that Manta services be partitioned in this way, but
  this tool requires that because most production deployments use two classes of
  hardware for these purposes.

`uuid` (string)
  the SDC compute node uuid for this server.  This must be unique within the
  entire region.

`memory` (positive integer)
  gigabytes of memory available on this server.  This is currently only used for
  storage servers to determine the appropriate number of compute zones.

`az` (string)
  (optional) availability zone.  If the value is omitted from any server, that
  server is placed into a default availablity zone.

`rack` (string)
  (optional) arbitrary identifier for the rack this server is part of.  Racks
  often represent fault domains, so the tool uses this information to attempt to
  distribute services across racks.  If the value is omitted from any server,
  that server is placed into a default rack.

See the Manta Operator's Guide for a more complete discussion of sizing and
laying out Manta services.


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
* `version`: the version of the zone's image
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
specified are "service", "image", "version", and "shard".

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

`manta-adm update [-l LOG_FILE] [-n] [-y] [--no-reprovision] [--skip-verify-channel] FILE [SERVICE]`

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

This command supports the `-l/--log_file`, `-n/--dryrun`, and `-y/--confirm`
options described above, plus:

`--no-reprovision`
  When upgrading a zone, always provision a new zone and deprovision the
  previous one, rather than reprovisioning the existing one.

`--skip-verify-channel`
  When upgrading, do not verify that the images being provisioned or
  reprovisioned are present on the "remote" (usually https://updates.joyent.com)
  imgapi channel that was set on the headnode using the `sdcadm channel`
  command.

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

The `manta-adm zk fixup` command supports the `-l/--log_file`, `-n/--dryrun`,
and `-y/--confirm` options described above.


## EXIT STATUS

`0`
  Success

`1`
  Generic failure.

`2`
  The command-line options were not valid.


## COPYRIGHT

Copyright 2019 Joyent, Inc.

## SEE ALSO

json(1), Manta Operator's Guide
