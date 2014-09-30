#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

echo ""
export PS4='[\D{%FT%TZ}] ${BASH_SOURCE}:${LINENO}: ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

set -o errexit
set -o pipefail
set -o xtrace

function fatal {
    echo "$(basename $0): fatal error: $*" >&2
    exit 1
}

DIR=$(dirname $(dirname $0))
VNODES=
PORT=
PNODES=
MANIFEST_PNODES=
RING_IMAGE="/var/tmp/$(uuid).ring.tar.gz"
RING_IMAGE_MANIFEST="/var/tmp/$(uuid).ring.manifest.json"
SAPI_URL=$(cat $DIR/etc/config.json | json sapi.url)
[[ -n $SAPI_URL ]] || fatal "no SAPI_URL"
CURRENT_RING_IMAGE=$(curl -s "$SAPI_URL/applications?name=manta&include_master=true"\
    | json -Ha metadata.HASH_RING_IMAGE)
POSEIDON_UUID=$(curl -s "$SAPI_URL/applications?name=manta&include_master=true"\
    | json -Ha owner_uuid)
[[ -n $POSEIDON_UUID ]] || fatal "no POSEIDON_UUID found"
export SDC_IMGADM_URL=$(cat /opt/smartdc/manta-deployment/etc/config.json | json imgapi.url)
[[ -n $SDC_IMGADM_URL ]] || fatal "no SDC_IMGAPI_URL found"
RING_LOCATION_PREFIX=/var/tmp/$(uuid)/hash_ring
RING_LOCATION=$RING_LOCATION_PREFIX/hash_ring
mkdir -p $RING_LOCATION
MANTA_APPLICATION=$(curl -s \
    "$SAPI_URL/applications?name=manta&include_master=true" | json -Ha uuid)
[[ -n $MANTA_APPLICATION ]] || fatal "no MANTA_APPLICATION found"
IMAGE_UUID=$(uuid -v4)
FORCE=0

function usage() {
    if [[ -n "$1" ]]
    then
        echo "error: $1"
        echo ""
    fi
    echo "Usage:"
    echo "  $0 -v <vnodes> -p <moray port>"
    echo "Creates a consistent hash ring used by electric-moray. The ring is"
    echo "created and uploaded to imgapi. The resulting img UUID is persisted"
    echo "in SAPI on the Manta application as metadata.HASH_RING_IMAGE"
    echo ""
    echo "WARNING: Run this command with care, improper use such as generating "
    echo "a bad ring or a different ring in production will result in the "
    echo "corruption of Manta metadata."
    exit 2
}

function fatal {
    echo "$(basename $0): error: $1"
    exit 1
}

function ring_exists {
    echo "ring exists with uuid $CURRENT_RING_IMAGE"
    exit 3
}

[[ -n $1 ]] || usage

while getopts "hv:p:f" c; do
    case "$c" in
    h)
        usage
        ;;
    f)
        FORCE=1
        ;;
    v)
        VNODES=$OPTARG
        ;;
    p)
        PORT=$OPTARG
        ;;
    *)
        usage "illegal option -- $OPTARG"
        ;;
    esac
done
shift $((OPTIND - 1))

#
# Mainline
#

# make sure no previous topology image exists
if [[ $FORCE -eq 0 ]]
then
    if [[ -n $CURRENT_RING_IMAGE ]]
    then
        ring_exists
    fi
fi

# get the index nodes, put them all on one line and prepend tcp:// to each node
PNODES=$(for i in $( manta-shardadm list | \
    grep Index | awk '{print $2}'); do echo tcp://$i:$PORT; done | tr -s '\n' ' ')
[[ -n $PNODES ]] || fatal "no index shards found"
MANIFEST_PNODES=$(echo $PNODES | tr ' ' '-'| tr -d ':' | tr -d '/')

fash create -v $VNODES -l $RING_LOCATION -b leveldb -p "$PNODES"
/usr/bin/tar -czf $RING_IMAGE -C $RING_LOCATION_PREFIX hash_ring
cat <<HERE > $RING_IMAGE_MANIFEST
{
    "v": 2,
    "uuid": "$IMAGE_UUID",
    "owner": "$POSEIDON_UUID",
    "name": "manta-hash-ring",
    "version": "$(date +%Y%m%dT%H%m%SZ)",
    "state": "active",
    "public": false,
    "published_at": "$(node -e 'console.log(new Date().toISOString())')",
    "type": "other",
    "os": "other",
    "files": [
    {
        "sha1": "$(sha1sum $RING_IMAGE | tr -s ' '| cut -d ' ' -f1)",
        "size": $(stat -c %s $RING_IMAGE),
        "compression": "gzip"
    }
    ],
    "description": "Manta Hash Ring"
}
HERE

sdc-imgadm import -m $RING_IMAGE_MANIFEST -f $RING_IMAGE

curl --connect-timeout 10 -fsS -i -H accept:application/json \
    -H content-type:application/json\
    --url "$SAPI_URL/applications/$MANTA_APPLICATION" \
    -X PUT -d \
    "{ \"action\": \"update\", \"metadata\": { \"HASH_RING_IMAGE\": \"$IMAGE_UUID\", \"HASH_RING_IMGAPI_SERVICE\": \"$SDC_IMGADM_URL\" } }"
