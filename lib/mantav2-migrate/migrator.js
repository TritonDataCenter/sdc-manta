/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * A mantav2 "Migrator" object that holds the business logic, config, utilities
 * for doing migration steps.
 */

var format = require('util').format;
var fs = require('fs');
var http = require('http');
var path = require('path');
var zlib = require('zlib');
var uuidv4 = require('uuid/v4');

var assert = require('assert-plus');
var netconfig = require('triton-netconfig');
var sdcClients = require('sdc-clients');
var vasync = require('vasync');
var VError = require('verror').VError;

// ---- globals/constants

const STORDELINK_TEMPLATE = `#!/bin/bash

set -o errexit
set -o pipefail
xtrace_log="/var/tmp/stordelink.$(date -u +%Y%m%dT%H%M%S).xtrace.log"
echo "Writing xtrace output to: \${xtrace_log}" >&2
exec 4>>$xtrace_log
BASH_XTRACEFD=4
set -o xtrace

function fatal {
    echo "$0: fatal error: $*"
    exit 1
}

# Ensure this is run on the correct storage node.
targ_storage_id={{storage_id}}
curr_storage_id=$(json -f /opt/smartdc/mako/etc/gc_config.json manta_storage_id)
if [[ $curr_storage_id != $targ_storage_id ]]; then
    fatal "this stordelink script must run on '$targ_storage_id': this is '$curr_storage_id'"
fi

# Exit early if already have a ".success" file.
TOP=$(cd $(dirname $0) 2>/dev/null >/dev/null; pwd)
success_file="$TOP/{{storage_id}}_stordelink.success"
if [[ -f "$success_file" ]]; then
    echo "Already completed stordelink successfully (\${success_file} exists)."
    exit 0
fi

{{cmds}}

echo "[$(date -u "+%Y%m%dT%H%M%SZ")] Completed stordelink successfully." >"\${success_file}"
echo "Completed stordelink successfully (created \${success_file})."
`;

const MORAYDELINK_TEMPLATE = `#!/bin/bash

set -o errexit
set -o pipefail
xtrace_log="/var/tmp/moraydelink.$(date -u +%Y%m%dT%H%M%S).xtrace.log"
echo "Writing xtrace output to: \${xtrace_log}" >&2
exec 4>>$xtrace_log
BASH_XTRACEFD=4
set -o xtrace

function fatal {
    echo "$0: fatal error: $*"
    exit 1
}

# Ensure this is run on the correct shard.
targ_shard="{{shardHost}}"
curr_shard=$(json -f /opt/smartdc/moray/etc/config.json service_name)
if [[ $curr_shard != $targ_shard ]]; then
    fatal "this moraydelink script must run on a moray for shard '$targ_shard': this is '$curr_shard'"
fi

# Exit early if already have a ".success" file.
TOP=$(cd $(dirname $0) 2>/dev/null >/dev/null; pwd)
success_file="$TOP/{{shardHost}}_moraydelink.success"
if [[ -f "$success_file" ]]; then
    echo "Already completed moraydelink successfully (\${success_file} exists)."
    exit 0
fi

{{cmds}}

echo "[$(date -u "+%Y%m%dT%H%M%SZ")] Completed moraydelink successfully." >"\${success_file}"
echo "Completed moraydelink successfully."
`;

// ---- Migrator

function Migrator(opts) {
    assert.object(opts.log, 'opts.log');
    assert.uuid(opts.runId, 'opts.runId');
    assert.string(opts.userAgent, 'opts.userAgent');

    this.log = opts.log;
    this.runId = opts.runId;
    this.config = this._loadConfigSync();
    this.userAgent = opts.userAgent;
}

Migrator.prototype.close = function close() {
    if (this._sapi) {
        this._sapi.close();
    }
    if (this._vmapi) {
        this._vmapi.close();
    }
};

Migrator.prototype._loadConfigSync = function _loadConfigSync() {
    var configFile = path.resolve(__dirname, '../../etc/config.json');
    var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
    return config;
};

Migrator.prototype.getSapiClient = function getSapiClient() {
    if (!this._sapi) {
        this._sapi = new sdcClients.SAPI({
            url: this.config.sapi.url,
            version: '~2',
            userAgent: this.userAgent,
            log: this.log,
            headers: {
                'x-request-id': this.runId
            }
        });
    }

    return this._sapi;
};

Migrator.prototype.getVmapiClient = function getVmapiClient() {
    if (!this._vmapi) {
        this._vmapi = new sdcClients.VMAPI({
            url: this.config.vmapi.url,
            userAgent: this.userAgent,
            log: this.log,
            headers: {
                'x-request-id': this.runId
            }
        });
    }

    return this._vmapi;
};

/*
 * Find the 'manta' SAPI application.
 *
 * @param {Function} cb - `function (err, mantaApp)`. If there is no "manta"
 *      application in SAPI, this calls back `cb(null, null)`.
 */
Migrator.prototype.getMantaApp = function getMantaApp(cb) {
    var self = this;

    if (this._mantaApp !== undefined) {
        cb(null, this._mantaApp);
        return;
    }

    var sapi = self.getSapiClient();

    sapi.listApplications(
        {
            name: 'manta',
            include_master: true
        },
        function onApps(err, apps) {
            if (err) {
                cb(err);
                return;
            }

            if (!apps || apps.length < 1) {
                self._mantaApp = null;
            } else {
                assert(
                    apps.length === 1,
                    'there are multiple "manta" apps in SAPI: ' +
                        JSON.stringify(apps)
                );
                self._mantaApp = apps[0];
            }
            cb(null, self._mantaApp);
        }
    );
};

/*
 * List all the DC names in this region on which there are Manta service
 * instances deployed.
 *
 * Manta SAPI instances have a metadata.DATACENTER that names the DC to which
 * they are deployed.
 */
Migrator.prototype.listDcs = function listDcs(cb) {
    let dcs = [];
    let sapi = this.getSapiClient();
    let self = this;

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function theMantaApp(ctx, next) {
                    self.getMantaApp(function onApp(err, app) {
                        ctx.app = app;
                        next(err);
                    });
                },

                function findDcsFromAllMantaInsts(ctx, next) {
                    // Dev Note: `sapi.getApplicationObjects` is a heavyweight
                    // function that performs poorly in a very large DC for mantav1.
                    // If this proves unreliable, then we can switch to interating
                    // through all the services.
                    sapi.getApplicationObjects(
                        ctx.app.uuid,
                        {include_master: true},
                        function(err, objs) {
                            if (err) {
                                next(err);
                                return;
                            }

                            let hasInstFromDc = {};
                            let instsSansDc = [];
                            for (let svcUuid of Object.keys(objs.instances)) {
                                for (let inst of objs.instances[svcUuid]) {
                                    let dc = inst.metadata.DATACENTER;
                                    if (!dc) {
                                        instsSansDc.push(inst);
                                    } else {
                                        hasInstFromDc[dc] = true;
                                    }
                                }
                            }
                            dcs = Object.keys(hasInstFromDc).sort();

                            if (instsSansDc.length > 0) {
                                let summary = instsSansDc.map(function(inst) {
                                    return format(
                                        '%s (%s)',
                                        inst.uuid,
                                        inst.params.alias
                                    );
                                });
                                next(
                                    new VError(
                                        '%d manta instances do not have metadata.DATACENTER set:\n%s',
                                        instsSansDc.length,
                                        summary.join('\n    ')
                                    )
                                );
                            } else {
                                next();
                            }
                        }
                    );
                }
            ]
        },
        function finish(err) {
            cb(err, dcs);
        }
    );
};

Migrator.prototype.setMantaMetadata = function setMantaMetadata(metadata, cb) {
    let sapi = this.getSapiClient();

    this.getMantaApp(function onApp(appErr, app) {
        if (appErr) {
            cb(appErr);
            return;
        }
        sapi.updateApplication(app.uuid, {metadata: metadata}, function(err) {
            cb(err);
        });
    });
};

Migrator.prototype.deleteMantaMetadata = function deleteMantaMetadata(
    names,
    cb
) {
    assert.arrayOfString(names, 'names');

    let sapi = this.getSapiClient();

    this.getMantaApp(function onApp(appErr, app) {
        if (appErr) {
            cb(appErr);
            return;
        }

        var update = {
            action: 'delete',
            metadata: {}
        };
        for (let name of names) {
            update.metadata[name] = null;
        }

        sapi.updateApplication(app.uuid, update, function(err) {
            cb(err);
        });
    });
};

/*
 * Return an array of objects, one for each webapi instance in this datacenter.
 *      [
 *          {
 *              uuid: <vm/instance uuid>,
 *              alias: <vm alias>,
 *              state: <vm state>,
 *              adminIp: <ip of VM on the admin network>,
 *              isV2: <true or false>,
 *              err: <a error instance if there was an error determining isV2>
 *          }
 *      ]
 */
Migrator.prototype.getWebapiV2Info = function getWebapiV2Info(cb) {
    let log = this.log;
    let sapi = this.getSapiClient();
    let self = this;
    let thisDcName = this.config.datacenter_name;
    let vmapi = this.getVmapiClient();
    let wInfo;

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function theMantaApp(ctx, next) {
                    self.getMantaApp(function onApp(err, app) {
                        ctx.app = app;
                        next(err);
                    });
                },
                function theWebapiSvc(ctx, next) {
                    sapi.listServices(
                        {
                            name: 'webapi',
                            application_uuid: ctx.app.uuid,
                            include_master: true
                        },
                        function(err, svcs) {
                            if (err) {
                                next(err);
                            } else {
                                assert(
                                    svcs.length === 1,
                                    format(
                                        'there is not exactly one "webapi" service on the "manta" app (%s): %s',
                                        ctx.app.uuid,
                                        svcs
                                    )
                                );
                                ctx.svc = svcs[0];
                                next();
                            }
                        }
                    );
                },
                function theInsts(ctx, next) {
                    sapi.listInstances(
                        {
                            service_uuid: ctx.svc.uuid,
                            include_master: true
                        },
                        function(err, insts) {
                            if (err) {
                                next(err);
                            } else {
                                ctx.insts = insts.filter(function onInst(inst) {
                                    return (
                                        inst.metadata.DATACENTER === thisDcName
                                    );
                                });
                                next();
                            }
                        }
                    );
                },
                function theVms(ctx, next) {
                    let uuids = ctx.insts
                        .map(function onInst(inst) {
                            return inst.uuid;
                        })
                        .join(',');

                    vmapi.listVms({uuids: uuids}, function(err, vms) {
                        if (err) {
                            next(err);
                            return;
                        }

                        // We use the webapi *admin* network IP rather than the
                        // *manta* network IP because (a) it is listening on both,
                        // and (b) the headnode doesn't generally have a NIC on
                        // the manta network.
                        wInfo = vms.map(function onVm(vm) {
                            let adminIp = vm.nics
                                .filter(nic => netconfig.isNicAdmin(nic))
                                .map(nic => nic.ip)[0];
                            assert(
                                adminIp,
                                format(
                                    'webapi VM %s (%s) does not have an "admin" nic',
                                    vm.uuid,
                                    vm.alias
                                )
                            );
                            return {
                                uuid: vm.uuid,
                                alias: vm.alias,
                                state: vm.state,
                                adminIp: adminIp
                            };
                        });
                        log.debug({wInfo: wInfo}, 'webapi inst V2 info');
                        next();
                    });
                },
                function determineIsV2(_, next) {
                    vasync.forEachPipeline(
                        {
                            inputs: wInfo,
                            func: function checkOneWebapi(webapi, nextWebapi) {
                                var req = http.request(
                                    {
                                        method: 'GET',
                                        host: webapi.adminIp,
                                        path: '/',
                                        timeout: 5000,
                                        agent: false
                                    },
                                    function onRes(res) {
                                        res.on('data', function(_chunk) {});
                                        res.on('end', function onEnd() {
                                            var server = res.headers['server'];
                                            switch (server) {
                                                case 'Manta':
                                                    webapi.isV2 = false;
                                                    break;
                                                case 'Manta/2':
                                                    webapi.isV2 = true;
                                                    break;
                                                default:
                                                    webapi.isV2 = false;
                                                    webapi.err = new VError(
                                                        'unexpected Server header value %j from webapi instance %s (%s)',
                                                        server,
                                                        webapi.uuid,
                                                        webapi.alias
                                                    );
                                                    break;
                                            }
                                            nextWebapi();
                                        });
                                    }
                                );

                                req.on('error', function onErr(err) {
                                    webapi.isV2 = false;
                                    webapi.err = new VError(
                                        err,
                                        'could not "GET %s/" to check webapi instance %s (%s)',
                                        webapi.adminIp,
                                        webapi.uuid,
                                        webapi.alias
                                    );
                                    nextWebapi();
                                });

                                req.end();
                            }
                        },
                        function finish(err) {
                            next(err);
                        }
                    );
                }
            ]
        },
        function finish(err) {
            cb(err, wInfo);
        }
    );
};

/*
 * If necessary, read in
 *      /var/db/snaplink-cleanup/discovery/{region}_{shard}_sherlock.tsv.gz
 * files for each shard and write out "delink" scripts to be run on
 * (a) each storage node and, then (b) on each shard's moray to "delink"
 * each snaplink.
 *
 * "Delinking" a snaplink is:
 *  - generate a new object id (uuid) for the delinked object,
 *  - [on storage node] create a hardlink from the old object id file to the new
 *    object id (fast, takes no extra space so we need not worry about disk
 *    space on storage nodes for this), and
 *  - [moray] update the metadata entry to have the new object id
 *
 * Generate files are:
 *      /var/db/snaplink-cleanup/delink/
 *          {region}_{storage_id}_stordelink.sh
 *          {region}_{shard}_moraydelink.sh
 *
 * This calls back `function (err, scripts, generated)` where, on success,
 * `scripts` is:
 *      {
 *          stordelink: [
 *              '{region}_{storage_id}_stordelink.sh',
 *              ...
 *          ],
 *          moraydelink: [
 *              '{region}_{shard}_moraydelink.sh',
 *              ...
 *          ]
 *      }
 * and `generated` is a boolean indicating if the delink scripts were newly
 * generated in this call.
 *
 * If delink scripts already exist under "/var/db/snaplink-cleanup/delink/"
 * then, this just returns the listing of them and ensures the full expected
 * set is there.
 */
Migrator.prototype.ensureDelinkScripts = function ensureDelinkScripts(
    opts,
    cb
) {
    assert.string(opts.dbDir, 'opts.dbDir');
    assert.arrayOfObject(opts.indexShards, 'opts.indexShards');

    let delinkDir = path.join(opts.dbDir, 'delink');
    let filenames;
    let log = this.log;
    let metadataCmdsFromShard = {};
    let numZeroByteObjs = 0;
    let scripts = {
        stordelink: [],
        moraydelink: []
    };
    let shardHosts;
    let storCmdsFromStorId = {};
    let storIds;

    // If already have generated delink scripts, then just validate and return
    // those.
    try {
        filenames = fs.readdirSync(delinkDir);
    } catch (readdirErr) {
        cb(readdirErr);
        return;
    }
    scripts.stordelink = filenames
        .filter(f => /_stordelink.sh$/.test(f))
        .map(f => path.join(delinkDir, f));
    scripts.moraydelink = filenames
        .filter(f => /_moraydelink.sh$/.test(f))
        .map(f => path.join(delinkDir, f));
    if (scripts.moraydelink.length > 0) {
        let missingMorayScripts = [];
        opts.indexShards.forEach(function(shard) {
            var expected = path.join(delinkDir, `${shard.host}_moraydelink.sh`);
            if (scripts.moraydelink.indexOf(expected) === -1) {
                missingMorayScripts.push(expected);
            }
        });
        if (missingMorayScripts.length > 0) {
            cb(
                new VError(
                    'existing delink scripts are missing the following expected ' +
                        'moray delink scripts: %s',
                    missingMorayScripts.join(', ')
                )
            );
        } else {
            cb(null, scripts, false);
        }
        return;
    }

    // Work through each discovery file and generate the delink info.
    for (let shardIdx = 0; shardIdx < opts.indexShards.length; shardIdx++) {
        let shard = opts.indexShards[shardIdx];
        metadataCmdsFromShard[shard.host] = [];

        let content;
        let records;
        try {
            content = fs.readFileSync(shard.discoveryFile);
        } catch (readErr) {
            cb(readErr);
            return;
        }
        try {
            records = zlib
                .gunzipSync(content)
                .toString('utf8')
                .trim()
                .split(/\n/g)
                .filter(line => line)
                .map(line => JSON.parse(line));
        } catch (parseErr) {
            cb(new VError(parseErr, 'could not load %s', shard.discoveryFile));
            return;
        }
        log.debug(
            {discoveryFile: shard.discoveryFile, numRecords: records.length},
            'loaded discovery file'
        );

        for (let i = 0; i < records.length; i++) {
            let obj = records[i];
            let newUuid = uuidv4();

            if (obj.size === 0) {
                numZeroByteObjs++;
            } else {
                for (let s = 0; s < obj.storageIds.length; s++) {
                    let storId = obj.storageIds[s];
                    if (!storCmdsFromStorId.hasOwnProperty(storId)) {
                        storCmdsFromStorId[storId] = [];
                    }

                    // Typically a sherlock snaplink record has a 'creatorId'.
                    // This is set (by snaplink-sherlock.sh) from the
                    // "manta" table `creator` field. In very old Manta,
                    // before MANTA-1848 added support for cross-account
                    // snaplinks, there was no `creator` field. In those
                    // cases we can assume the creator is the owner of the
                    // object path (available in the "key" field if the
                    // snaplink record):
                    //      "key":"/1b315468-c6be-46dc-b99b-9c1f59224693/..."
                    let creatorId;
                    if (obj.hasOwnProperty('creatorId')) {
                        creatorId = obj.creatorId;
                    } else {
                        creatorId = obj.key.split('/')[1];
                        assert.uuid(
                            creatorId,
                            'creatorId from obj.key=' + obj.key
                        );
                    }

                    // Add a `ln <old> <new>` for each storage zone that has
                    // this object. To make this re-runnable, we skip that if
                    // the two exist and refer to the same file (hardlink or
                    // symlink).
                    //
                    //      if [[ ! $old -ef $new ]]; then
                    //          ln $old $new
                    //      fi
                    let oldPath = `/manta/${creatorId}/${obj.objectId}`;
                    let newPath = `/manta/${creatorId}/${newUuid}`;
                    storCmdsFromStorId[storId].push(
                        `if [[ ! ${oldPath} -ef ${newPath} ]]; then`
                    );
                    storCmdsFromStorId[storId].push(
                        `    ln ${oldPath} ${newPath}`
                    );
                    storCmdsFromStorId[storId].push(`fi`);
                }
            }

            // Our metadata command cannot handle some special chars in
            // the object key.
            if (obj.key.indexOf('"') !== -1 || obj.key.indexOf('\\') !== -1) {
                cb(
                    new VError(
                        'object key cannot have `"` or `\\` characters: %s',
                        obj.key
                    )
                );
                return;
            }

            // Add a command to update the metadata entry to the new object
            // id. Basically we just need to rewrite each object, so we're
            // just going to do a getobject, transform and then putobject.
            //
            // E.g.:
            //      putobject -d $(getobject manta "/771e0f61-f938-4678-87d1-33381702ed6f/stor/hello1.txt" \
            //          | json -o json-0 -e "this.value.objectId='6e09bf84-ce29-602d-d58c-92765e72124a'" value) \
            //          manta "/771e0f61-f938-4678-87d1-33381702ed6f/stor/hello1.txt"
            metadataCmdsFromShard[shard.host].push(
                [
                    'putobject -d "$(getobject manta "',
                    obj.key,
                    '" | json -o json-0 -e "this.value.objectId=\'',
                    newUuid,
                    '\'" value)" manta "',
                    obj.key,
                    '"'
                ].join('')
            );
        }
    }

    storIds = Object.keys(storCmdsFromStorId);
    shardHosts = Object.keys(metadataCmdsFromShard);
    log.debug(
        {
            numZeroByteObjs: numZeroByteObjs,
            numStorIds: storIds.length,
            numShards: shardHosts.length
        },
        'generated delink commands'
    );

    // Write delink scripts.
    for (let i = 0; i < storIds.length; i++) {
        let storId = storIds[i];
        let cmds = storCmdsFromStorId[storId];
        let filename = path.join(delinkDir, `${storId}_stordelink.sh`);
        let script = STORDELINK_TEMPLATE.replace(
            '{{cmds}}',
            cmds.join('\n')
        ).replace(/{{storage_id}}/g, storId);
        try {
            fs.writeFileSync(filename, script, {encoding: 'utf8'});
        } catch (writeErr) {
            cb(writeErr);
            return;
        }
        scripts.stordelink.push(filename);
        log.debug({filename: filename}, 'wrote stordelink script');
    }

    for (let i = 0; i < shardHosts.length; i++) {
        let shardHost = shardHosts[i];
        let cmds = metadataCmdsFromShard[shardHost];
        let filename = path.join(delinkDir, `${shardHost}_moraydelink.sh`);
        let script = MORAYDELINK_TEMPLATE.replace(
            '{{cmds}}',
            cmds.join('\n')
        ).replace(/{{shardHost}}/g, shardHost);
        try {
            fs.writeFileSync(filename, script, {encoding: 'utf8'});
        } catch (writeErr) {
            cb(writeErr);
            return;
        }
        scripts.moraydelink.push(filename);
        log.debug({filename: filename}, 'wrote moraydelink script');
    }

    cb(null, scripts, true);
};

// ---- exports

module.exports = {
    Migrator: Migrator
};
