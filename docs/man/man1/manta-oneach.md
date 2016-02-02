# MANTA-ONEACH 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-oneach - execute commands and file transfers on Manta components

## SYNOPSIS

`manta-oneach OPTIONS SCOPE_ARGUMENTS COMMAND`

`manta-oneach OPTIONS SCOPE_ARGUMENTS -d|--dir DIRECTORY -g|--get FILE`

`manta-oneach OPTIONS SCOPE_ARGUMENTS -d|--dir DIRECTORY -p|--put FILE`

## DESCRIPTION

The `manta-oneach` command is used to execute commands and file transfers on an
arbitrary set of Manta components.  Manta components include:

* **non-global zones**, which make up most Manta components.  Each zone is
  associated with a service like "webapi", "jobsupervisor", "loadbalancer",
  and the like.  See manta-adm(1).
* **global zones**, which represent the operating system context on each compute
  node.  Manta has agent components that run in global zones.  Operating on
  global zones can also be useful for fetching system-wide metrics or
  manipulating system-wide configuration.

When you use this command, you specify a **scope** (which identifies the
components to operate on) and a **command** (which either represents a shell
script to execute or else describes a file to transfer).


### Selecting a scope

This command only operates on zones within the same datacenter.  The command
may need to be repeated in other datacenters in order to execute it across an
entire Manta deployment.

The following options must be specified to select a group of zones within the
current datacenter:

`-a, --all-zones`
  Select all non-global zones (e.g., all Manta components, but not operating
  system global zones).

`-s, --service SERVICE[,SERVICE...]`
  Select zones for the specified services.  You can specify this option multiple
  times or specify a comma-separated list of service names.  See manta-adm(1)
  for a list of service names.

`-S, --compute-node HOSTNAME | UUID[,HOSTNAME | UUID...]`
  Select zones running on the specified compute nodes (servers), identified by
  their hostname or server uuid.  You can specify this option multiple times or
  specify a comma-separated list of hostnames or server uuids.

`-z, --zonename ZONENAME[,ZONENAME...]`
  Select specific zones by zonename (uuid).  You can specify this option
  multiple times or specify a comma-separated list of zonenames.

At least one of the above options must be specified.  You can specify any
combination of `--service`, `--compute-node`, and `--zonename`, options, in
which case only zones matching all of these criteria will be selected.

You may also specify:

`-G, --global-zones`
  Execute the operation (either the shell command or the file transfer) in the
  global zones of whatever zones are matched by the other flags.  For example,
  `--global-zones --all-zones` executes the command once in each global zone.
  `--global-zones --service webapi` executes the command once for each global
  zone where a "webapi" zone is running.


### Executing commands

To execute a shell command in each of the selected zones, simply pass a single
`COMMAND` argument.  This argument must be a single string, so it usually needs
to be quoted on the command-line.  The string is executed as a script with
bash(1) in a login shell.  It may contain any valid bash input, including
redirects, pipes, and other special characters (`>`, `<`, `|`, `&&`, and the
like).


### Transferring files

To transfer files between the current system and each of the selected zones, you
must specify exactly one of the following options:

`-p, --put FILENAME`
  For each of the selected components, transfers file `FILENAME` from that
  component into the local directory specified by the `-d/--dir` option.  The
  target directory will contain one file for each of the selected components,
  named by the unique identifier for that component (zonenames for non-global
  zones, and server uuids for global zones).

`-g, --get FILENAME`
  For each of the selected components, transfers file `FILENAME` from the local
  system to the directory on the remote system specified by the `-d/--dir`
  option.

You must also specify:

`-d, --dir DIRECTORY`
  When `-p/--put` is used, `DIRECTORY` is the directory on the current system
  into which files from each of the remote components will be transferred.  When
  `-g/--get` is used, `DIRECTORY` is the directory on the remote system into
  which the local file will be transferred.

and you may also specify:

`-X, --clobber`
  If specified, allow a `-g/--get` operation to overwrite an existing file.
  Otherwise, the operation may fail if the destination file already exists.

**NOTE:** This tool is intended for operators and only supports zones that are
expected to be under the full control of trusted operators.  The mechanism used
for per-zone file transfers is not safe when untrusted users have access to the
selected zones.


## OTHER OPTIONS

Besides the scope options and file transfer options described above, the
following options may be specified:

`-c, --concurrency N`
  Specifies that no more than `N` remote commands or file transfers may be
  outstanding at any given time.

`-I, --immediate`
  Causes results to be emitted immediately as they are received rather than only
  after all results are available.  When this option is not used, then
  `manta-oneach` waits for all results before printing any, it sorts them by
  component so that they appear in a consistent order, and it formats them
  either for single-line (tabular) or multi-line display depending on the actual
  result output.  (See the `-N/--oneline` option for details.)  If this flag is
  specified, the results are printed as they arrive, in arbitrary order, and
  with the multi-line output mode unless `-N/--oneline` was also specified.

`-J, --jsonstream`
  Report results as newline-separated JSON rather than human-readable text.  See
  "Streaming JSON format" below.

`-n, --dry-run`
  Report what would be executed without actually doing anything.

`-N, --oneline`
  Forces tabular, single-line-per-result output mode.  Normally, if
  `-I/--immediate` is not used, then `manta-oneach` waits for all results before
  printing any.  If all results contain only one line of output, then the
  results are reported using a single line with leading columns that describe
  which component reported that result.  If any result contains more than one
  line of output, then all results are reported verbatim with a single-line
  header before each result and a blank line after each result.  This option
  forces the single-line-per-result output.

`-T, --exectimeout SECONDS`
  If a command takes longer than `SECONDS` seconds or a file transfer takes more
  than `SECONDS` seconds to start, then the operation is abandoned on that
  component and an error is reported.  Note that the command is not killed when
  this happens, so the operator is responsible for any necessary cleanup.  The
  default timeout is 60 seconds.

`--amqp-host HOST, --amqp-port TCP_PORT,`

`--amqp-login LOGIN, --amqp-password PASSWORD,`

`--amqp-timeout NSECONDS`
  AMQP connection parameters.  This tool uses the SDC Ur facility through the
  SDC AMQP broker.  By default, the host, port, login, and password are
  automatically configured based on the current SDC installation.  The default
  connect timeout is 5 seconds.


### Streaming JSON format

If the `-J/--jsonstream` option is used, then the output consists of one line
per result, with each line containing a complete JSON object describing the
result of the operation.  This format is a subset of the format reported by
`sdc-oneachnode(1)`'s JSON output format.  Specifically, objects contain
properties:

* `uuid` (string): the server uuid where the command was executed
* `hostname` (string): the server hostname where the command was executed
* `zonename` (string): the zonename where the command was executed (only present
  when `-G/--global-zones` was not specified)
* `service` (string): the service name to which zone `zonename` belongs (only
  present when `-G/--global-zones` was not specified)
* `result` (object) Ur result, which contains an integer property `exit_status`
  describing the exit status of the command.  The `result` property is only
  present when `error` is not present.  For command execution, the properties
  `stdout` and `stderr` are also attached the `result`, and they contain the
  stdout and stderr from the command executed.
* `error` (object): describes a failure to execute the command or transfer the
  file.  The presence of this property indicates that the command failed to
  execute at all or it was abandoned because of the execution timeout (see
  `-T/--exectimeout`) or the system failed to determine the result.  If the
  command was executed but it failed, that will not produce an error.  Callers
  should use `exit_status` to identify that case.


## EXAMPLES

**Running commands in all zones for a given service**: Use the svcs(1) command
to check the status of the `minnow` service inside each storage zone:

    $ manta-oneach --service storage 'svcs minnow'
    === Output from 3211f5ed-f70d-481d-93ae-755e4c84837d on headnode (storage):
    STATE          STIME    FMRI
    online         Jan_20   svc:/manta/application/minnow:default

    === Output from 4ecda097-2556-49ca-908a-b8fea6a923c4 on headnode (storage):
    STATE          STIME    FMRI
    online         Jan_20   svc:/manta/application/minnow:default

    === Output from b150f995-a91b-4fa5-8fe4-7cb84621e553 on headnode (storage):
    STATE          STIME    FMRI
    online         Jan_20   svc:/manta/application/minnow:default

Or, use svcs(1) options to limit the output to a single line for a more concise
display:

    $ manta-oneach --service storage 'svcs -H -o state minnow'
    SERVICE          ZONE     OUTPUT
    storage          3211f5ed online
    storage          4ecda097 online
    storage          b150f995 online

**Running commands in global zones**: report disk usage from zpool(1M) on each
of the compute nodes running at least one "postgres" instance:

    $ manta-oneach --global-zones --service postgres 'zpool list'
    === Output from 44454c4c-5700-1047-8051-b3c04f585131 (headnode):
    NAME    SIZE  ALLOC   FREE  EXPANDSZ   FRAG    CAP  DEDUP  HEALTH  ALTROOT
    zones  1.62T   565G  1.07T         -    42%    33%  1.00x  ONLINE  -

Similarly, the zpool(1M) command can provide more concise output:

    $ manta-oneach -G -s postgres 'zpool list -H -o cap'
    HOSTNAME              OUTPUT
    headnode              33%

**Complex filtering:** use svcs(1) to report the state of "haproxy" processes
in "moray" and "webapi" zones on compute node RA14872:

    # manta-oneach --compute-node RA14872 --service moray,webapi 'svcs haproxy'
    === Output from 69790d4a-d500-4e56-99ac-967024765805 on RA14872 (moray):
    STATE          STIME    FMRI
    online         Jan_15   svc:/manta/haproxy:default

    === Output from b3c6c144-7500-4904-a0b5-91997a71f75d on RA14872 (moray):
    STATE          STIME    FMRI
    online         Jan_15   svc:/manta/haproxy:default

    === Output from b68396db-d49f-487a-8379-a36234ac9993 on RA14872 (moray):
    STATE          STIME    FMRI
    online         Jan_15   svc:/manta/haproxy:default

    === Output from 380920d9-ed44-4bcd-b61c-4b99f49c1329 on RA14872 (webapi):
    STATE          STIME    FMRI
    online         Jan_15   svc:/manta/haproxy:default

**File transfer from remote components**: transfer a copy of `/etc/resolv.conf`
from all "jobsupervisor" components into /var/tmp on the current system

    $ manta-oneach --service jobsupervisor --dir /var/tmp --put /etc/resolv.conf
    SERVICE          ZONE     OUTPUT
    jobsupervisor    2b1d10be ok

After that, /var/tmp will contain one file for each jobsupervisor zone, each
named according to that zone's zonename.

**File transfer to remote components:** transfer a copy file "pgsqlstat" in the
current directory to `/root` in each "postgres" zone:

    $ manta-oneach --service postgres --dir /root --get ./pgsqlstat
    SERVICE          ZONE     OUTPUT
    postgres         70d44638 ok
    postgres         a5223321 ok
    postgres         ef318383 ok


## EXIT STATUS

`0`
  Success

`1`
  Generic failure.

`2`
  The command-line options were not valid.

Programs that want to determine the precise result of each operation should use
the `-J/--jsonstream` output format.


## ENVIRONMENT

`LOG_LEVEL`
  If present, this must be a valid node-bunyan log level name (e.g., "warn").
  The internal logger will use this log level and emit output to `stderr`.  This
  option is subject to change at any time.


## COPYRIGHT

Copyright (c) 2016 Joyent Inc.

## SEE ALSO

bash(1), json(1), manta-adm(1), manta-login(1), sdc-oneachnode(1), Manta
Operator's Guide
