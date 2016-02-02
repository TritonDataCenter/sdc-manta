# MANTA-INIT 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-init - initialize or update a Manta deployment


## SYNOPSIS

`manta-init [OPTIONS] -e|--email EMAIL`


## DESCRIPTION

The `manta-init` command is used during initial Manta deployment to download
Manta component images and configure the local Manta SAPI application.

`manta-init` is also used when updating an existing Manta deployment to download
newer Manta component images.

Both of these use cases are described in the Manta Operator's Guide.  **Do not
run this command except as documented in the Manta Operator's Guide.**


## OPTIONS

`-c, --concurrent_downloads N`
  Specifies that no more than `N` zone images should be downloaded
  concurrently.

`-e, --email EMAIL_ADDRESS`
  Specifies that amon alarm notifications should be sent to `EMAIL_ADDRESS`.
  This option is required, even after the initial setup.

`-l, --log_file FILE`
  Specifies where the verbose activity log should be written.  The default path
  is /var/log/manta-init.log.  The special string "stdout" is used to dump to
  stdout.

`-m, --marlin_image IMAGE_UUID`
  Use image `IMAGE_UUID` for Marlin ("compute") zones.  This is not supported
  and should only be used by Manta developers.

`-n, --no_download`
  Do not download any new zone images.

`-s, --size SIZE`
  Specifies that this Manta deployment as one of the predefined sizes "coal",
  "lab", and "production".  The size controls parameters like the memory, CPU,
  and disk resources assigned to deployed components.  "coal" is used for very
  small deployments (e.g., a VM in a laptop).  "lab" is used for larger
  single-system deployments.  "production" is used for standard server
  deployments.  Some additional constraints are imposed based on size, like the
  fact that `manta-factoryreset` will refuse to operate on "production"
  deployments.


## EXAMPLES

See the Manta Operator's Guide.


## EXIT STATUS

`0`
  Success

Non-zero
  Generic failure.


## COPYRIGHT

Copyright (c) 2016 Joyent Inc.

## SEE ALSO

Manta Operator's Guide
