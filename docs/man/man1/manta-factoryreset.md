# MANTA-FACTORYRESET 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-factoryreset - completely removes a Manta deployment


## SYNOPSIS

`manta-factoryreset [-l | --log_file LOG_FILE] [-y | --skip_confirmation]`


## DESCRIPTION

The `manta-factoryreset` command completely removes a Manta deployment.  **This
command is extremely destructive.  All data stored in this Manta deployment will
be destroyed.**  This is primarily intended for developers and others who may
need to routinely set up and tear down Manta deployments.

This command does the following:

* disables all Marlin agents and removes their configuration
* destroys and deletes all Manta component zones
* deletes all SAPI services for the "manta" application (including 
  configuration)
* deletes the SAPI "manta" application (including configuration)
* removes the "poseidon" user and its ssh keys
* removes hash ring images used for metadata sharding

This command does not:

* remove the "manta" zone itself (which is used for deployment)
* remove images used for Manta zones
* remove networks created for Manta

because the assumption is that an operator may want to deploy Manta again
without having to set up the "manta" zone, download all the zone images, and
setup networks again.

This command does not work in Manta deployments that were initialized with size
"production" using manta-init(1).


## OPTIONS

`-l, --log_file LOGFILE`
  Specifies where the verbose activity log should be written.  The default path
  is /var/log/manta-init.log.  The special string "stdout" is used to dump to
  stdout.

`-y, --skip_confirmation`
  Skips the confirmation prompt.  Be sure this is what you want to do before
  using this option.


## EXIT STATUS

`0`
  Success

Non-zero
  Generic failure.


## COPYRIGHT

Copyright (c) 2016 Joyent Inc.

## SEE ALSO

manta-init(1), Manta Operator's Guide
