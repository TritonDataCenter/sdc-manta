# MANTA-LOGIN 1 "2016" Manta "Manta Operator Commands"

## NAME

manta-login - start interactive shell session on remote Manta zones

## SYNOPSIS

`manta-login SERVICE [CHOICE]`

`manta-login ZONENAME [CHOICE]`

`manta-login ZONENAME_PREFIX [CHOICE]`

`manta-login SUBSTR [CHOICE]`

## DESCRIPTION

The `manta-login` command is used to start an interactive shell session on
remote Manta zones.  It operates by using ssh(1) to open a connection to the
compute node where the designated zone is running and then using zlogin(1) to
start a shell in that zone.  As a result, it requires that the current user has
privileges to `ssh` to the target compute node.  This operation may result in a
password prompt.  Operators can avoid this by deploying ssh "authorized\_keys"
files to compute nodes.

To select the zone to log into, you may specify a service name, a zonename, or
any substring of a service name or zonename.  If more than one zone matches the
criteria, you will be prompted to select from a list of zones.  You may specify
CHOICE to bypass this prompt and select that option.

## OPTIONS

This command does not support any options.


## EXAMPLES

Log into a "webapi" zone, specified by service name.  Since there's more than
one in this datacenter, there will be a prompt.  Select option "3" (the fourth
entry):

    # manta-login webapi
    0:   webapi            1 02d02889-cd80-4ac1-bc0c-4775b86661e4 10.10.0.37      
    1:   webapi            1 39adec6c-bded-4a14-9d80-5a8bfc1121f9 10.10.0.41      
    2:   webapi            1 562a0e29-5024-4482-8f6c-26e0d95c5a36 10.10.0.40      
    3:   webapi            1 56564894-a7c2-470e-a218-3d859e7e1687 10.10.0.42      
    Choose a number: 3
    [Connected to zone '56564894-a7c2-470e-a218-3d859e7e1687' pts/4]
    [root@56564894 (webapi) ~]$ 

If you know which entry you want because you've logged into this zone before,
you can specify the choice directly:

    # manta-login webapi 3
    3
    [Connected to zone '56564894-a7c2-470e-a218-3d859e7e1687' pts/4]
    [root@56564894 (webapi) ~]$

This is especially useful when you don't care which zone you get as long as
it's a zone for the correct service.  You can just specify choice "0".

If you want to log into a specific instance and you have the zonename, you can
specify that and skip the prompt and the choice:

    # manta-login 56564894-a7c2-470e-a218-3d859e7e1687
    [Connected to zone '56564894-a7c2-470e-a218-3d859e7e1687' pts/4]
    [root@56564894 (webapi) ~]$ 


## COPYRIGHT

Copyright (c) 2016 Joyent Inc.

## SEE ALSO

ssh(1), manta-adm(1), manta-oneach(1), Manta Operator's Guide
