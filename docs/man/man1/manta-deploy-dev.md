# MANTA-DEPLOY-DEV 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-deploy-dev - deploy Manta in a development environment

manta-deploy-lab - deploy Manta on a lab machine

manta-deploy-coal - deploy Manta in a laptop VM


## SYNOPSIS

`manta-deploy-dev [-y] "lab" | "coal"`

`manta-deploy-lab [-y]`

`manta-deploy-coal`


## DESCRIPTION

The `manta-deploy-dev` command automates much of the Manta setup process for a
single-system Manta deployment.  You must select either a "lab" or "coal"
deployment.  This parameter determines how many metadata shards to create, how
many virtual nodes to create in the metadata tier, and how many of each kind of
component to deploy.

The `manta-deploy-lab` command is equivalent to `manta-deploy-dev lab`.

The `manta-deploy-coal` command is equivalent to `manta-deploy-dev -y coal`.

See the Manta Operator's Guide for information on using these commands.


## OPTIONS

`-y`
  Skips confirmations intended to improve reliability of the deployment process.


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

manta-init(1), Manta Operator's Guide
