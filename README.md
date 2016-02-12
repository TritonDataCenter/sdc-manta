<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2015, Joyent, Inc.
-->

# sdc-manta

This repository is part of the Joyent SmartDataCenter project (SDC).  For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

This repository contains all the deployment tools necessary to manage a Manta
within an SDC deployment.


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
    skate/          A prototype for a mini-Manta in SDC.


# Development

Before committing and pushing a change, run:

    make prepush

and if warranted, get a code review.

`make prepush` will run the test suite.  For this to work, you'll need to have a
configuration file in etc/config.json.  The easiest way to create one is to copy
the template in sapi\_manifests/manta/template and fill in the details for your
environment, or else copy the configuration file directly out of a deployed
sdc-manta zone in your environment.


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
