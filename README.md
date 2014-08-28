<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright (c) 2014, Joyent, Inc.
-->

# Manta Deployment Framework

Repository: <git@git.joyent.com:manta-deployment.git>
Browsing: <https://mo.joyent.com/manta-deployment>
Who: Bill Pijewski
Docs: <https://mo.joyent.com/docs/manta-deployment>
Tickets/bugs: <https://devhub.joyent.com/jira/browse/MANTA>


# Overview

This repository contains the manta deployment tools.  These tools use SAPI (see
https://mo.joyent.com/docs/sapi/master) to deploy and configure the manta object
storage service.  This document assumes familiarity with the SAPI object model,
so go read the SAPI docs if you haven't already.


# Repository

    amon/           Amon probes to add for each service
    bin/            Commands available in $PATH.
    cmd/            Top-level commands.
    config/         Configuration which describes the manta deployment.
    deps/           Git submodules and/or committed 3rd-party deps should go
                    here. See "node_modules/" for node.js deps.
    docs/           Project docs (restdown)
    lib/            Source files.
    manifests/      Configuration manifests
    networking/     Scripts and configuration for SDC networking.
    node_modules/   Node.js deps, either populated at build time or committed.
    scripts/        The user-script for all manta instances
    tools/          Miscellaneous dev/upgrade/deployment tools and data.
    ufds/           LDIF for engineer test accounts
    Makefile
    package.json    npm module info
    README.md


# Development

Before committing and pushing a change, run:

    make prepush

and if warranted, get a code review.


# Status

This is now the repository of record for deploying manta.  The manta.git
repository has been deprecated and will not be modified further.


# Everything else

For information on how to deploy manta, add a new service, or learn about
manta's configuration, see the mo docs (https://mo.joyent.com/docs/manta-deployment/master).
