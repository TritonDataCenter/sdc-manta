#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace

MD=/var/tmp/metadata.json
SAPI=http://$(json -f $MD SAPI_SERVICE)
CNAPI=http://$(json -f $MD CNAPI_SERVICE)
CURL="curl -s -H accept:application/json -H content-type:application/json"
SERVER_UUID=$($CURL $CNAPI/servers | json -Ha uuid)

SKATE_APP=$($CURL $SAPI/applications?name=skate | json -Ha uuid)
if [[ -z $SKATE_APP ]]; then
    exit 0
fi

# Disable and remove the marlin agent
CMD="svcadm disable marlin-agent; svcadm delete marlin-agent; \
    rm -rf /opt/smartdc/marlin/etc"
$CURL $CNAPI/servers/$SERVER_UUID/execute -X POST \
    -d "{ \"script\": \"$CMD\" }"

$CURL $SAPI/services?application_uuid=$SKATE_APP | json -Ha uuid | while read l; do
    # Destroy all instances for service
    $CURL $SAPI/instances?service_uuid=$l | json -Ha uuid | while read k; do
        $CURL $SAPI/instances/$k -X DELETE
    done

    # Destroy all services
    $CURL $SAPI/services/$l -X DELETE
done

# Destroy the Skate application
$CURL $SAPI/applications/$SKATE_APP -X DELETE
