<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
-->

# sdc-manta

This repository is part of the Joyent Triton and Manta projects.
For contribution guidelines, issues, and general documentation, visit the main
[Triton](http://github.com/joyent/triton) and
[Manta](http://github.com/joyent/manta) project pages.

This repository contains all the deployment tools necessary to manage a Manta
within a Triton deployment.


## Active Branches

There are currently two active branches of this repository, for the two
active major versions of Manta. See the [mantav2 overview
document](https://github.com/joyent/manta/blob/master/docs/mantav2.md) for
details on major Manta versions.

- [`master`](../../tree/master/) - For development of mantav2, the latest
  version of Manta.
- [`mantav1`](../../tree/mantav1/) - For development of mantav1, the long
  term support maintenance version of Manta.


# Overview

This repository contains the manta deployment tools.  The documentation for
using these tools is part of the [Manta Operator's
Guide](https://joyent.github.io/manta).

These tools use SAPI (see http://github.com/joyent/sdc-sapi) to deploy and
configure the manta object storage service.  For details on the SAPI object
model, see the SAPI documentation.

**Note:** historically, this repository included scripts used to set up various
Manta component zones.  This code (and its documentation) now lives in the
[manta-scripts](https://github.com/joyent/manta-scripts) repo.


# Repository

See [eng.git](https://github.com/joyent/eng) for common directories.
Directories specific to this repo include:

    config/         Configuration which describes the manta deployment.
    manifests/      Service-wide configuration manifests.
    networking/     Scripts and configuration for Manta networking.
    sapi_manifests/ SAPI manifests for zone configuration.
    scripts/        The user-script for all manta instances.


# Development

Before committing and pushing a change, run:

    make prepush

`make prepush` will run the test suite, which requires (a) being on SmartOS
and (b) having a filled in "etc/config.json" (manually created from
"sapi\_manifests/manta/template" or copied from a deployed manta0 zone).

An alternative to run the test suite is to:

1. run `make prepush` on your Mac (ignoring the test failures); and

2. sync your local changes to a deployed `manta0` zone (e.g. in COAL) and test
   there. This can be done as follows:

        ./tools/rsync-to $HEADNODE   # e.g. ./tools/rsync-to root@10.99.99.7
        ssh $HEADNODE
        sdc-login -l manta
        cd /opt/smartdc/manta-deployment
        pkgin in -y make
        make test


## Adding a new Manta service

### Prerequisites

When adding a new service to manta, there are several prerequisites:

 * Your software must be delivered in an SDC image.  This process requires
   changes to the mountain-gorilla.git Makefile and targets.json.  In addition,
   your repository's Makefile will likely need "release" and "publish" targets.

 * To deploy the new service, the image must be available in the
   updates.joyent.com IMGAPI.  It should be named "manta-SERVICE", as in:

        [root@headnode (bh1-kvm6) ~]# updates-imgadm list name=manta-postgres | tail -3 | awk '{print $1, $2, $3 }'
        a54c49d4-68c8-41b8-906e-0eb0d84fe3f7 manta-postgres master-20130621T213801Z-g94b316a
        a579d45b-6703-4d18-8673-c35202490d37 manta-postgres master-20130621T224758Z-g94b316a
        5d9524cf-67e4-43c5-a77d-72a1b43cfa9f manta-postgres master-20130624T162632Z-g1f6c2f7
        [root@headnode (bh1-kvm6) ~]#

   If you implement Makefile rules similarly to other repositories, then the
   image upload will happen as part of the standard "make publish" target.  See
   Mountain Gorilla for details on how this works.

 * Add any SAPI manifests required for your service.  Traditionally these are
   delivered in /opt/smartdc/$SERVICE/sapi\_manifests.  There are many examples
   of services following this pattern: see mako.git, muskie.git, manatee.git,
   and others.  That manifest defines the config.json file which will be used
   for configuring your service.

 * Your image should provide a script in /opt/smartdc/boot/configure.sh.  That
   script will be run every time an instance boots, and that script should
   configure your instance appropriately (enable config-agent, enable other
   services, update ~/.profile, etc.).  This will likely use the common
   manta-scripts files.  Check out the manta-scripts repo (including the
   documentation there), and be sure to update the script in that repo that
   updates all dependent repos.

### Local Changes

Once your image has been loaded into the updates.joyent.com IMGAPI (check with
`updates-imgadm list name=manta-SERVICE`), you're ready to add your service to
the sdc-manta repository.  There are several steps involved there:

 * Add a manta-deployment.git/config/services/SERVICE.json file for your
   service.  This JSON file defines the SAPI service, which includes your zone
   parameters and metadata used in deployment.  Look through the existing files
   to see which options are available, and consult the SAPI documentation for a
   full explanation.

 * You probably want to update lib/adm.js, lib/services.js, and lib/layout.js to
   deploy your service in a standard deployment.  You'll definitely want to
   update mSvcNames so that manta-adm knows about this service and will update
   it when requested.  The service should almost certainly become part of the
   default lab system configuration later in the file so that people are testing
   its deployment frequently.  If the service can reasonably be deployed in COAL
   (or needs to be, in order for Manta to work), then add it to the default COAL
   configuration as well.

# Everything else

For information on how to deploy manta, add a new service, or learn about
manta's configuration, see the documentation in the
[manta](http://github.com/joyent/manta) repository.
