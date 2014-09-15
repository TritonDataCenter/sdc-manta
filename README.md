<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# sdc-manta

This repository is part of the Joyent SmartDataCenter project (SDC).  For
contribution guidelines, issues, and general documentation, visit the main
[SDC](http://github.com/joyent/sdc) project page.

This repository contains all the deployment tools necessary to manage a Manta
within an SDC deployment.

# Overview

This repository contains the manta deployment tools.  These tools use SAPI (see
http://github.com/joyent/sdc-sapi) to deploy and configure the manta object
storage service.  This document assumes familiarity with the SAPI object model,
so go read the SAPI docs if you haven't already.


# Repository

    bin/            Commands available in $PATH.
    boot/           Configuration scripts on zone setup.
    cmd/            Top-level commands.
    config/         Configuration which describes the manta deployment.
    deps/           Git submodules and/or committed 3rd-party deps should go
                    here. See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    manifests/      Service-wide configuration manifests.
    networking/     Scripts and configuration for SDC networking.
    node_modules/   Node.js deps, either populated at build time or committed.
    sapi_manifests/ SAPI manifests for zone configuration.
    scripts/        The user-script for all manta instances.
    skate/          A prototype for a mini-Manta in SDC.
    test/           Tests.
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    Makefile
    package.json    npm module info
    README.md


# Development

Before committing and pushing a change, run:

    make prepush

and if warranted, get a code review.


# Everything else

For information on how to deploy manta, add a new service, or learn about
manta's configuration, see the documentation in the
[manta](http://github.com/joyent/manta) repository.
