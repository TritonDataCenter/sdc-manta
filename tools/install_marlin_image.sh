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
# install_marlin_image.sh: This script downloads, caches, and installs the
#     latest marlin compute image.
#


set -o xtrace
set -o errexit

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <machine>"
    exit 1
fi

WRKDIR=/var/tmp/install_marlin_image
IMGADM=$WRKDIR/sdc-imgapi-cli/bin/updates-imgadm
JSON=$WRKDIR/json/bin/json
NODE=$1
MARLIN_UUID=bb9264e2-f134-11e3-9ec7-478da02d1a13
MARLIN_MANIFEST=${MARLIN_UUID}.imgmanifest

# Allow callers to pass additional flags to ssh and scp
[[ -n ${SSH} ]] || SSH=ssh
[[ -n ${SCP} ]] || SCP=scp

ADMIN_UUID=$(${SSH} $NODE 'bash /lib/sdc/config.sh -json | json ufds_admin_uuid' 2>/dev/null)

set +o errexit
${SSH} ${NODE} "/opt/smartdc/bin/sdc-imgadm get ${MARLIN_UUID}"
if [[ $? -eq 0 ]]; then
    echo "Marlin image ${MARLIN_UUID} already installed."
    exit 0
fi
set -o errexit

# Setup
if [[ ! -d $WRKDIR/sdc-imgapi-cli ]]; then
    mkdir -p $WRKDIR
    cd $WRKDIR
    git clone git@github.com:joyent/sdc-imgapi-cli.git
    cd $WRKDIR/sdc-imgapi-cli
    make all
fi

if [[ ! -d $WRKDIR/json ]]; then
    mkdir -p $WRKDIR
    cd $WRKDIR
    git clone git://github.com/trentm/json.git
fi

function download_and_install_image_files {
    local UUID=$1
    local MANIFEST=${UUID}.imgmanifest

    [[ -d $WRKDIR/images ]] || mkdir -p $WRKDIR/images
    cd $WRKDIR/images
    [[ -f ${MANIFEST} ]] || ${IMGADM} get ${UUID} > ${MANIFEST}
    [[ -f "$(ls ${UUID}-file.*)" ]] || ${IMGADM} get-file ${UUID} -O

    # Check for the base image
    ORIGIN=$(json -f ${MANIFEST} origin)
    if [[ -n ${ORIGIN} ]]; then
        download_and_install_image_files ${ORIGIN}
    fi

    # Use the target SDC's admin uuid as the marlin image owner.
    json -f $MANIFEST -e "this.owner = '$ADMIN_UUID'" > $MANIFEST.tmp
    mv $MANIFEST.tmp $MANIFEST

    # Copy the files over and import into IMGAPI.
    ${SCP} ${MANIFEST} ${NODE}:/var/tmp/
    ${SCP} ${UUID}-file.* ${NODE}:/var/tmp/
    ${SSH} ${NODE} \
        "/opt/smartdc/bin/sdc-imgadm import \
            -m /var/tmp/${MANIFEST} \
            -f /var/tmp/${UUID}-file.* \
        && rm /var/tmp/${MANIFEST} /var/tmp/${UUID}-file.*"
}

download_and_install_image_files ${MARLIN_UUID}
