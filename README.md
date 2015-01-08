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


# Everything else

For information on how to deploy manta, add a new service, or learn about
manta's configuration, see the documentation in the
[manta](http://github.com/joyent/manta) repository.
