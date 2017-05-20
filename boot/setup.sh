#!/usr/bin/bash
# -*- mode: shell-script; fill-column: 80; -*-
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o xtrace

PATH=/opt/local/bin:/opt/local/sbin:/usr/bin:/usr/sbin

role=manta
app_name=$role

CONFIG_AGENT_LOCAL_MANIFESTS_DIRS=/opt/smartdc/manta-deployment

# Include common utility functions (then run the boilerplate)
source /opt/smartdc/boot/lib/util.sh
sdc_common_setup

# Cookie to identify this as a SmartDC zone and its role
mkdir -p /var/smartdc/manta-deployment

# Install deployment tools
mkdir -p /opt/smartdc/manta-deployment
chown -R nobody:nobody /opt/smartdc/manta-deployment

# Add build/node/bin and node_modules/.bin to PATH
echo "" >>/root/.profile
echo "export PATH=\$PATH:/opt/smartdc/manta-deployment/build/node/bin:/opt/smartdc/manta-deployment/bin:/opt/smartdc/manta-deployment/node_modules/.bin:/opt/smartdc/sapi/node_modules/.bin" >>/root/.profile
echo 'export MANTA_DATACENTER=$(mdata-get "sdc:datacenter_name")' >> /root/.profile

# All done, run boilerplate end-of-setup
sdc_setup_complete

exit 0
