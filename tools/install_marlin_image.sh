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
MARLIN_UUID=1757ab74-b3ed-11e2-b40f-c7adac046f18

IMGADM=$WRKDIR/imgapi-cli/bin/updates-imgadm
JSON=$WRKDIR/json/bin/json
MANIFEST=${MARLIN_UUID}.imgmanifest
NODE=$1

# Allow callers to pass additional flags to ssh and scp
[[ -n ${SSH} ]] || SSH=ssh
[[ -n ${SCP} ]] || SCP=scp

set +o errexit
${SSH} ${NODE} "/opt/smartdc/bin/sdc-imgadm get ${MARLIN_UUID}"
if [[ $? -eq 0 ]]; then
    echo "Marlin image ${MARLIN_UUID} already installed."
    exit 0
fi
set -o errexit

if [[ ! -d $WRKDIR/imgapi-cli ]]; then
    mkdir -p $WRKDIR
    cd $WRKDIR
    git clone git@git.joyent.com:imgapi-cli.git
    cd $WRKDIR/imgapi-cli
    make all
fi
if [[ ! -d $WRKDIR/json ]]; then
    mkdir -p $WRKDIR
    cd $WRKDIR
    git clone git://github.com/trentm/json.git
fi

# Download the marlin manifest and image, if necessary.
[[ -d $WRKDIR/images ]] || mkdir -p $WRKDIR/images
cd $WRKDIR/images
[[ -f ${MANIFEST} ]] || ${IMGADM} get ${MARLIN_UUID} > ${MANIFEST}
[[ -f "$(ls $MARLIN_UUID-file.*)" ]] || ${IMGADM} get-file ${MARLIN_UUID} -O

# Use the target SDC's admin uuid as the marlin image owner.
ADMIN_UUID=$(${SSH} $NODE 'bash /lib/sdc/config.sh -json | json ufds_admin_uuid' 2>/dev/null)
json -f $MANIFEST -e "this.owner = '$ADMIN_UUID'" > $MANIFEST.tmp
mv $MANIFEST.tmp $MANIFEST

# Copy the files over and import into IMGAPI.
${SCP} ${MANIFEST} ${NODE}:/var/tmp/
${SCP} ${MARLIN_UUID}-file.* ${NODE}:/var/tmp/
${SSH} ${NODE} \
    "/opt/smartdc/bin/sdc-imgadm import \
        -m /var/tmp/${MANIFEST} \
        -f /var/tmp/${MARLIN_UUID}-file.* \
    && rm /var/tmp/${MANIFEST} /var/tmp/${MARLIN_UUID}-file.*"
