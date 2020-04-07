#!/bin/bash
#
# Copyright 2020 Joyent, Inc.
#
#
# This program runs against a Manta postgres VM
#
#   snaplink-sherlock.sh <manatee VM UUID>
#
# and:
#
#  * creates a temporary (surrogate) VM
#  * creates a new manatee (zfs) snapshot and clones it
#  * attaches the cloned dataset as the delegated dataset for the surrogate VM
#  * installs a user-script which runs on startup and:
#      * starts postgresql
#      * does a pg_dump of the 'manta' table
#      * feeds the stream through a program that pulls out only SnapLinks
#      * writes a JSON line for every snaplink to a dump file (gzip compressed)
#      * reports the total number of lines, objects and SnapLinks seen
#  * waits for completion of user-script
#  * outputs a summary of the results
#  * copies the result files to the $PWD:
#       {shard}_sherlock.tsv.gz
#
# Once the program is complete, the resulting dump file can be collected and the
# both the zone and snapshot should be deleted (otherwise they will be holding a
# reference to the manatee's zfs filesystem).
#
# On any unexpected error it will exit prematurely.
#

set -o errexit
set -o pipefail

SURROGATE_CPU_CAP=400
SURROGATE_QUOTA=100
TARGET=$1

function fatal {
    echo "$*" 2>&1
    exit 1
}

[[ -n ${TARGET} ]] || fatal "Usage: $0 <vm UUID>"

xtrace_log="/var/tmp/snaplink-sherlock.$(date -u +%Y%m%dT%H%M%S).xtrace.log"
echo "Writing xtrace output to: ${xtrace_log}" >&2
exec 4>>$xtrace_log
BASH_XTRACEFD=4
set -o xtrace

# Parse the full moray shard out of the postgres zone alias.
# E.g.:
#       1.postgres.coalregion.joyent.us-f8bd09a5
#       {shardnum}.postgres.{region}.{dns_domain}-{instance_prefix}
# from which (assuming always that shard="{shardnum}.moray"):
#       shard=1.moray.{region}.{dns_domain}
targetAlias=$(vmadm get ${TARGET} | json -H alias)
[[ ${targetAlias} =~ ^[0-9]+\.postgres\.[a-z0-9\.-]+-[0-9a-f]+ ]] \
    || fatal "VM '${targetAlias}' does not look like a Manta postgres zone."
shard=${targetAlias%-*}             # drop the trailing "-{instance_prefix}"
shard=${shard/.postgres./.moray.}

echo "Target is ${TARGET} (alias=${targetAlias}, shard=${shard})"

shortId=$(cut -d'-' -f1 <<<${TARGET})
vmJson=$(vmadm get ${TARGET})
imageUuid=$(json image_uuid <<<${vmJson})
newVmUuid=$(uuid -v4)

maxLocked=$(json max_locked_memory <<<${vmJson})
maxSwap=$(json max_swap <<<${vmJson})
maxPhys=$(json max_physical_memory <<<${vmJson})

# TODO: do we really need to match the target's parameters for anything else?

TMP_USERSCRIPT=/tmp/snaplink-sherlock.$$.sh

echo "Creating surrogate VM..."

cat > ${TMP_USERSCRIPT} <<'EOF'
#!/bin/bash

set -o xtrace
set -o errexit
set -o pipefail

export PATH=/usr/local/sbin:/usr/local/bin:/opt/local/sbin:/opt/local/bin:/usr/sbin:/usr/bin:/sbin

svccfg delete svc:/network/physical:default
hostname $(mdata-get sdc:alias)

morayShard=$(mdata-get morayShard)
dataDir=/zones/$(zonename)/data
pgVersion=$(json current < ${dataDir}/manatee-config.json)
groupadd -g 907 postgres && useradd -u 907 -g postgres postgres
mkdir -p /var/pg
chown -R postgres:postgres /var/pg
sudo -u postgres -g postgres /opt/postgresql/${pgVersion}/bin/postgres -D ${dataDir}/data &

# XXX forever?!
while ! /opt/postgresql/${pgVersion}/bin/psql -U postgres -c 'SELECT now();' moray; do
    sleep 5
done

cat > /var/tmp/filter.$$.js <<'EOFILTER'
var assert = require('assert');
var readline = require('readline');
var util = require('util');

var lineReader = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: false
});

var inCopy = false;
var numLines = 0;
var numSnapLinks = 0;
var numObjects = 0;

lineReader.on('line', function _onLine(rawLine) {
    var _value;
    var fields;
    var line = rawLine.trim();

    numLines++;

    if (!inCopy && line.match(/^COPY manta \(_id, _txn_snap, _key, _value, _etag, _mtime, _vnode, dirname, name, owner, objectid, type, _idx\) FROM stdin;$/)) {
        inCopy = true;
    } else if (!inCopy && line.match(/^COPY manta \(_id, _txn_snap, _key, _value, _etag, _mtime, _vnode, dirname, name, owner, objectid, type\) FROM stdin;$/)) {
        inCopy = true;
    }

    if (inCopy) {
       if (line.match(/^\\.$/)) {
           inCopy = false;
       } else if (line.match(/"type":"object"/)) {
           numObjects++;
           if (line.match(/"createdFrom":"/)) {
               numSnapLinks++;
               fields = line.split('\t');
               if (fields.length !== 12) {
                   assert.equal(fields.length, 13, 'Must have 12 or 13 fields');
               }
               _value = JSON.parse(fields[3]);

               console.log(JSON.stringify({
                   storageIds: _value.sharks.map(function _m(obj) { return (obj.manta_storage_id); }),
                   key: _value.key,
                   creatorId: _value.creator,
                   objectId: _value.objectId,
                   size: _value.contentLength
               }));
           }
       }
    }
}).on('close', function() {
    console.error(util.format('Lines: %d, SnapLinks: %d, Objects: %d', numLines, numSnapLinks, numObjects));
    process.exit(0);
});
EOFILTER

(time /opt/postgresql/${pgVersion}/bin/pg_dump -Fp -a -U postgres -t manta moray \
    | /usr/node/bin/node /var/tmp/filter.$$.js \
    | gzip > /root/${morayShard}.manta_dump.gz) 2>/root/manta_dump.stderr

echo "DONE" > /root/manta_dump.done

(sleep 2; halt -q) &

exit 0

EOF

(/usr/vm/sbin/add-userscript ${TMP_USERSCRIPT} | vmadm create) <<EOF
{
    "alias": "snaplink-sherlock-${shortId}",
    "autoboot": false,
    "billing_id": "00000000-0000-0000-0000-000000000000",
    "brand": "joyent-minimal",
    "cpu_cap": ${SURROGATE_CPU_CAP},
    "customer_metadata": {
        "morayShard": "${shard}"
    },
    "delegate_dataset": true,
    "do_not_inventory": true,
    "firewall_enabled": false,
    "image_uuid": "${imageUuid}",
    "max_locked_memory": ${maxLocked},
    "max_lwps": 256000,
    "max_physical_memory": ${maxPhys},
    "max_swap": ${maxSwap},
    "mdata_exec_timeout": 0,
    "owner_uuid": "00000000-0000-0000-0000-000000000000",
    "quota": ${SURROGATE_QUOTA},
    "tmpfs": ${maxPhys},
    "uuid": "${newVmUuid}",
    "zfs_io_priority": 100
}
EOF

rm -f ${TMP_USERSCRIPT}

echo "Surrogate VM is ${newVmUuid}"

zfs destroy zones/${newVmUuid}/data

newSnapshotName="zones/${TARGET}/data/manatee@sherlock-$$"
echo "Creating snapshot ${newSnapshotName}..."
zfs snapshot zones/${TARGET}/data/manatee@sherlock-$$

newSnapshotTime=$(zfs get -Hpo value creation ${newSnapshotName})
newSnapshotTimeIso=$(/usr/node/bin/node -e "console.log(new Date(${newSnapshotTime}000).toISOString());")
currentTimeIso=$(/usr/node/bin/node -e "console.log(new Date().toISOString());")

echo "Current timestamp is -- ${currentTimeIso}."
echo "Attaching snapshot from ${newSnapshotTimeIso}..."

zfs clone ${newSnapshotName} zones/${newVmUuid}/data

echo "Starting Surrogate VM..."
vmadm start ${newVmUuid}

zoneRoot="/zones/${newVmUuid}/root"

echo "Waiting for user-script to complete..."
while [[ ! -f ${zoneRoot}/root/manta_dump.done ]]; do
    sleep 1
done
echo "DONE (${zoneRoot}/root/manta_dump.done)"

echo ""
echo "-- Results:"
cat ${zoneRoot}/root/manta_dump.stderr
ls -l ${zoneRoot}/root/
echo "--"

echo ""
echo "Copying snaplink listing to PWD ($PWD):"
echo " - ${PWD}/${shard}_sherlock.tsv.gz"
cp $zoneRoot/root/${shard}.manta_dump.gz ${shard}_sherlock.tsv.gz

echo ""
echo "* * *"
echo "Successfully completed snaplink discovery."

echo ""
echo "Sherlock leftovers the will be removed later in Step 3.7:"
echo " - vmadm delete ${newVmUuid}"
echo " - zfs destroy ${newSnapshotName}"

echo ""
echo "You should now copy '${shard}_sherlock.tsv.gz' back to '/var/db/snaplink-cleanup/discovery/' on the driver DC."
