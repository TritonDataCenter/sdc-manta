#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

#
# update_manta_zone.sh: This script updates the manta deployment tools on a
#     server.
#


set -o xtrace
set -o errexit

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <machine>"
    exit 1
fi

NODE=$1

# Allow callers to pass additional flags to ssh and scp
[[ -n ${SSH} ]] || SSH=ssh
[[ -n ${SCP} ]] || SCP=scp

UUID=$(${SSH} ${NODE} "vmadm lookup alias=~manta")

rsync -avz \
    amon \
    bin \
    build \
    cmd \
    config \
    lib \
    manifests \
    networking \
    node_modules \
    package.json \
    scripts \
    tools \
    ufds \
    ${NODE}:/zones/${UUID}/root/opt/smartdc/manta-deployment
