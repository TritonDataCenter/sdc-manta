/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 * Copyright 2022 MNX Cloud, Inc.
 */

//
// A "Hotpatcher" object that holds the business logic, config, utilities
// for doing rebalancer-agent hotpatching.
//

var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var sdcClients = require('sdc-clients');
var strsplit = require('strsplit');
var vasync = require('vasync');
var VError = require('verror').VError;

var oneach = require('../oneach/oneach');
var sdc = require('../sdc');

// ---- Hotpatcher

function Hotpatcher(opts) {
    assert.object(opts.log, 'opts.log');
    assert.uuid(opts.runId, 'opts.runId');
    assert.string(opts.userAgent, 'opts.userAgent');

    this.log = opts.log;
    this.runId = opts.runId;
    this.config = this._loadConfigSync();
    this.userAgent = opts.userAgent;
}

Hotpatcher.prototype.close = function close() {
    if (this._sapi) {
        this._sapi.close();
    }
    if (this._imgapi) {
        this._imgapi.close();
    }
    if (this._vmapi) {
        this._vmapi.close();
    }
    if (this._updatesJo) {
        this._updatesJo.close();
    }
};

Hotpatcher.prototype._loadConfigSync = function _loadConfigSync() {
    var configFile = path.resolve(__dirname, '../../etc/config.json');
    var config = JSON.parse(fs.readFileSync(configFile, 'utf8'));
    return config;
};

Hotpatcher.prototype.getSapiClient = function getSapiClient() {
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

Hotpatcher.prototype.getImgapiClient = function getImgapiClient() {
    if (!this._imgapi) {
        this._imgapi = new sdcClients.IMGAPI({
            url: this.config.imgapi.url,
            userAgent: this.userAgent,
            log: this.log,
            headers: {
                'x-request-id': this.runId
            }
        });
    }

    return this._imgapi;
};

Hotpatcher.prototype.getVmapiClient = function getVmapiClient() {
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

Hotpatcher.prototype.getUpdatesJoClient = function getUpdatesJoClient() {
    if (!this._updatesJo) {
        this._updatesJo = new sdcClients.IMGAPI({
            url: 'https://updates.tritondatacenter.com',
            userAgent: this.userAgent,
            log: this.log,
            headers: {
                'x-request-id': this.runId
            }
        });
    }

    return this._updatesJo;
};

// Find the 'manta' SAPI application.
//
// @param {Function} cb - `function (err, mantaApp)`. If there is no "manta"
//      application in SAPI, this calls back `cb(null, null)`.
Hotpatcher.prototype.getMantaApp = function getMantaApp(cb) {
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

// Get the named SAPI service in the given SAPI application.
//
// @param {Object} app - The SAPI application (typically the Manta app).
// @param {String} svcName - The service name.
// @param {Function} cb - `function (err, svc)`. If there is no such service
//      this calls back `cb(null, null)`.
Hotpatcher.prototype.getSvc = function getSvc(app, svcName, cb) {
    assert.uuid(app.uuid, 'app.uuid');
    assert.string(svcName, 'svcName');

    let sapi = this.getSapiClient();
    let svc;

    sapi.listServices(
        {
            application_uuid: app.uuid,
            name: svcName,
            include_master: true
        },
        function onSvcs(err, svcs) {
            if (err) {
                cb(err);
                return;
            }

            if (!svcs || svcs.length < 1) {
                svc = null;
            } else {
                assert(
                    svcs.length === 1,
                    'multiple "' +
                        svcName +
                        '" services were found: ' +
                        JSON.stringify(svcs)
                );
                svc = svcs[0];
            }
            cb(null, svc);
        }
    );
};

//
// List all the Manta "storage" instances in this DC.
//
Hotpatcher.prototype.listStorInsts = function listStorInsts(cb) {
    assert.string(this.config.datacenter_name, 'this.config.datacenter_name');

    let sapi = this.getSapiClient();
    let self = this;
    let storInsts = [];

    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function theMantaApp(ctx, next) {
                    self.getMantaApp(function onApp(err, app) {
                        if (err) {
                            next(err);
                        } else if (!app) {
                            next(true); // early abort
                        } else {
                            ctx.app = app;
                            next();
                        }
                    });
                },
                function theStorSvc(ctx, next) {
                    self.getSvc(ctx.app, 'storage', function onSvc(err, svc) {
                        if (err) {
                            next(err);
                        } else if (!svc) {
                            next(true); // early abort
                        } else {
                            ctx.svc = svc;
                            next();
                        }
                    });
                },

                function theInsts(ctx, next) {
                    sapi.listInstances(
                        {
                            service_uuid: ctx.svc.uuid,
                            include_master: true
                        },
                        function onInsts(err, insts) {
                            if (err) {
                                next(err);
                            } else {
                                // `insts` is the set of storage instances in
                                // *all* DCs in this region. We need to pare
                                // down to just those in this DC. We'll rely
                                // on `metadata.DATACENTER` for each SAPI obj.
                                storInsts = insts.filter(function inThisDc(
                                    inst
                                ) {
                                    assert.string(
                                        inst.metadata.DATACENTER,
                                        'inst.metadata.DATACENTER'
                                    );
                                    return (
                                        inst.metadata.DATACENTER ===
                                        self.config.datacenter_name
                                    );
                                });
                                next();
                            }
                        }
                    );
                }
            ]
        },
        function finish(err) {
            if (err === true) {
                // Early abort signal.
                err = null;
            }

            cb(err, storInsts);
        }
    );
};

//
// Fetch the image manifest (if it exists), for each of the given storage
// instances.
//
// @param {Array} storInsts - An array of SAPI instance objects (as from
//      <Hotpatcher>.listStorInsts). The `params.image_uuid` field is used.
// @param {Function} cb - `function (err)`. On success, `vm` and `image` fields
//      are (somewhat messily) added to each storInsts entry. These fields are
//      from VMAPI and IMGAPI respectively. The `image` field can be null if
//      IMGAPI no longer has the image.
//
Hotpatcher.prototype.fetchStorInstImages = function fetchStorInstImages(
    storInsts,
    cb
) {
    assert.arrayOfObject(storInsts, 'storInsts');

    let imgapi = this.getImgapiClient();
    let vmapi = this.getVmapiClient();

    vasync.pipeline(
        {
            funcs: [
                // First get the VM for this instance, from which we can (usually)
                // trust the "image_uuid". The "params.image_uuid" on SAPI instance
                // objects is often inaccurate.
                function fetchVms(_, next) {
                    vasync.forEachPipeline(
                        {
                            inputs: storInsts,
                            func: function fetchOneVm(storInst, nextVm) {
                                vmapi.getVm(
                                    {
                                        uuid: storInst.uuid
                                    },
                                    function(err, vm) {
                                        storInst.vm = vm;
                                        nextVm(err);
                                    }
                                );
                            }
                        },
                        next
                    );
                },

                // Then get the image manifest from IMGAPI for this `vm.image_uuid`.
                function fetchImages(_, next) {
                    let imageUuid;
                    let imageFromUuid = {};
                    let storInst;

                    // Uniq the set of image UUIDs to fetch from IMGAPI.
                    for (storInst of storInsts) {
                        imageUuid = storInst.vm.image_uuid;
                        if (imageUuid) {
                            imageFromUuid[imageUuid] = null;
                        }
                    }

                    // Fetch all the image manifests.
                    vasync.forEachParallel(
                        {
                            inputs: Object.keys(imageFromUuid),
                            func: function getImage(uuid, nextImage) {
                                imgapi.getImage(uuid, function(err, image) {
                                    if (
                                        err &&
                                        err.name !== 'ResourceNotFoundError'
                                    ) {
                                        nextImage(err);
                                    } else {
                                        if (image) {
                                            imageFromUuid[uuid] = image;
                                        }
                                        nextImage();
                                    }
                                });
                            }
                        },
                        function(err) {
                            if (err) {
                                next(err);
                                return;
                            }

                            // Add the `.image` field to each of the storInst objects.
                            for (storInst of storInsts) {
                                imageUuid = storInst.vm.image_uuid;
                                if (imageUuid) {
                                    storInst.image = imageFromUuid[imageUuid];
                                }
                            }

                            next();
                        }
                    );
                }
            ]
        },
        function(err) {
            cb(err);
        }
    );
};

//
// Fetch rebalancer-agent details for the rebalancer-agent on each of the given
// Manta storage instances in this DC.
//
// @param {Array} storInsts - An array of objects representing the storage
//      instances (in this DC) on which to fetch rebalancer-agent details.
//      Minimally each object must have a "uuid" field. Typically these are
//      raw SAPI instance objects, e.g. from <Hotpatcher>.listStorInsts.
// @param {Function} cb - `function (err, rebalAgents)`. On success
//      `rebalAgents` is an array of objects with the following fields:
//          - storInst - the storage instance item in the given `storInsts`
//          - storImageUuid
//          - hotpatched - Boolean. Whether rebalancer-agent is hotpatched on
//            this instance.
//          - version - String. The rebalancer-agent version.
//
Hotpatcher.prototype.listRebalAgents = function listRebalAgents(storInsts, cb) {
    assert.arrayOfObject(storInsts, 'storInsts');

    let cmd = `
set -o errexit
set -o pipefail

INSTALL_DIR=/opt/smartdc/rebalancer-agent
REBAL_AGENT=$INSTALL_DIR/bin/rebalancer-agent

# Line 1: storage instance image UUID.
mdata-get sdc:image_uuid

# Line 2: is there a hotpatched rebalancer-agent installed?
if [[ ! -a $INSTALL_DIR ]]; then
    echo "not-hotpatched"    # no rebalancer-agent at all
elif [[ -h $INSTALL_DIR ]]; then
    echo "hotpatched"
elif [[ -d $INSTALL_DIR ]]; then
    echo "not-hotpatched"
else
    echo "error: unknown state of $INSTALL_DIR"
fi

# Line 3: rebalancer-agent version.
if [[ ! -a $INSTALL_DIR ]]; then
    echo "null"    # no version if not installed
elif [[ -x "$REBAL_AGENT" ]]; then
    $REBAL_AGENT --version
else
    echo "error: $REBAL_AGENT is not executable"
fi

exit 0
`;

    let execErrs = [];
    let log = this.log;
    let rebalAgents = [];
    let self = this;
    let storInstFromUuid = {};

    storInsts.forEach(function(inst) {
        storInstFromUuid[inst.uuid] = inst;
    });

    let exec = new oneach.mzCommandExecutor({
        scopeZones: storInsts.map(inst => inst.uuid),
        execMode: oneach.MZ_EM_COMMAND,
        execCommand: cmd,

        // No particular reason to give such a large timeout. It should only
        // be reduced if we expose a CLI option to set it.
        execTimeout: 60000,
        // TODO: Should expose option to override concurrency.
        concurrency: 100,
        log: self.log,

        // -- The graveyard of unused arguments
        // `mzCommandExecutor`'s draconian interface blows up if one does not
        // very carefully specify almost (!) each and every argument.
        scopeGlobalZones: false,
        scopeAllZones: false,
        scopeServices: null,
        scopeComputeNodes: null,
        execDirectory: null,
        execFile: null,
        execClobber: null,
        // `streamStatus` is "required", but only used if `dryRun=true`, which
        // is not clearly documented. We emphatically do not want this writing
        // to stderr, so we stub it out.
        streamStatus: {write: function() {}},
        dryRun: false,
        // Hardcode the same AMQP connection defaults that ../oneach/cli.js does
        // (and doesn't export).
        amqpHost: null,
        amqpPort: 5672,
        amqpLogin: 'guest',
        amqpPassword: 'guest',
        amqpTimeout: 5000,
        sdcMantaConfigFile: sdc.sdcMantaConfigPathDefault
    });

    exec.on('error', function onError(err) {
        cb(err);
    });

    exec.on('data', function onData(res) {
        log.debug({oneachResult: res}, 'listRebalAgents result');

        if (res.result.exit_status !== 0) {
            execErrs.push(
                new VError(
                    {info: {res: res}},
                    'error gathering rebalancer-agent info on storage inst ' +
                        res.zonename
                )
            );
        } else {
            let lines = res.result.stdout.split(/\n/g);
            let storImageUuid = lines[0];
            assert.uuid(storImageUuid, 'storImageUuid');
            let hotpatchedStr = lines[1];
            let versionStr = lines[2];

            if (hotpatchedStr.startsWith('error:')) {
                execErrs.push(
                    new VError(
                        {info: {res: res, hotpatchedStr: hotpatchedStr}},
                        'error getting rebalancer-agent install status on storage inst ' +
                            res.zonename
                    )
                );
            } else if (versionStr.startsWith('error:')) {
                execErrs.push(
                    new VError(
                        {info: {res: res, versionStr: versionStr}},
                        'error getting rebalancer-agent versionStr on storage inst ' +
                            res.zonename
                    )
                );
            } else {
                rebalAgents.push({
                    storInst: storInstFromUuid[res.zonename],
                    storImageUuid: storImageUuid,
                    hotpatched: hotpatchedStr === 'hotpatched',
                    version:
                        versionStr === 'null'
                            ? null
                            : // Strip off first token from, e.g.:
                              //   rebalancer-agent 0.1.0 (master-202...19Z-g0c96e91)
                              strsplit(versionStr, ' ', 2)[1]
                });
            }
        }
    });

    exec.on('end', function onEnd() {
        if (execErrs.length > 0) {
            cb(VError.errorFromList(execErrs));
        } else {
            cb(null, rebalAgents);
        }
    });
};

//
// Hotpatch rebalancer-agent on the given storage instances with the given
// image (already imported into the DC's local IMGAPI).
//
// @param {Array} args.storInsts - Required. An array of objects representing
//      the storage instances (in this DC) on which to fetch rebalancer-agent
//      details. Minimally each object must have a "uuid" field. Typically these
//      are raw SAPI instance objects, e.g. from <Hotpatcher>.listStorInsts.
// @param {UUID} args.imageUuid - Required. The UUID of a rebalancer-agent image
//      imported into the DC's local IMGAPI.
// @pararm {Object} args.ui - Required.
// @param {Number} args.concurrency - Optional.
// @param {Function} cb - `function (err)`.
//
Hotpatcher.prototype.hotpatchRebalAgents = function hotpatchRebalAgents(
    args,
    cb
) {
    assert.arrayOfObject(args.storInsts, 'args.storInsts');
    assert.uuid(args.imageUuid, 'args.imageUuid');
    assert.object(args.ui, 'args.ui');
    assert.optionalFinite(args.concurrency, 'args.concurrency');

    let cmd = `
set -o errexit
set -o pipefail

PATH=/opt/local/sbin:/opt/local/bin:/usr/sbin:/usr/bin:/bin

function fatal {
    echo "hotpatchRebalAgents.cmd: fatal error: $*" >&2
    exit 1
}

IMAGE_UUID=${args.imageUuid}
[[ -n "$IMAGE_UUID" ]] || fatal "missing IMAGE_UUID"
INSTALL_DIR=/opt/smartdc/rebalancer-agent
HOT_INSTALL_DIR=$INSTALL_DIR.hotpatch-$IMAGE_UUID

# Start xtrace log when it starts to get interesting.
export PS4='[\\D{%FT%TZ}] <cmd>:\${LINENO}: '
set -o xtrace

# Download the tarball from imgapi over the admin network and crack it.
imgapi_url=$(curl -sS $(mdata-get SAPI_URL)/applications?name=sdc | json -H 0.metadata.imgapi_domain)
rm -rf /var/tmp/rebalancer-agent-$IMAGE_UUID.tgz
curl -sS -o /var/tmp/rebalancer-agent-$IMAGE_UUID.tgz $imgapi_url/images/$IMAGE_UUID/file
rm -rf $HOT_INSTALL_DIR
mkdir $HOT_INSTALL_DIR
gtar -C $HOT_INSTALL_DIR --strip-components 4 -xf /var/tmp/rebalancer-agent-$IMAGE_UUID.tgz

# Stop the service, swap in the new.
svcadm disable -s rebalancer-agent
svccfg delete rebalancer-agent
if [[ -h $INSTALL_DIR ]]; then
    # If there is an old hotpatch symlink, remove it.
    rm $INSTALL_DIR
elif [[ -d $INSTALL_DIR ]]; then
    # If there is an original rebalancer-agent, move it safely to the side.
    mv $INSTALL_DIR $INSTALL_DIR.orig
fi
(cd $(dirname $HOT_INSTALL_DIR) && ln -s $(basename $HOT_INSTALL_DIR) $INSTALL_DIR)

# Force a config-agent run to regenerate any config files, then start.
svcadm disable -s config-agent && svcadm enable -s config-agent
svccfg import $INSTALL_DIR/smf/manifests/rebalancer-agent.xml

# Wait for rebalancer-agent to come online (or go to maint).
loops=30
status=
while [[ "$status" != "online" ]]; do
    status=$(svcs -Ho state rebalancer-agent)
    if [[ "$status" == "online" ]]; then
        break
    elif [[ "$status" == "maintenance" ]]; then
        fatal "rebalancer-agent went to maintenance"
    fi
    sleep 1
    loops=$((loops - 1))
done
if [[ "$status" != "online" ]]; then
    fatal "timeout waiting for rebalancer-agent to come online (last status: '$status')"
fi

exit 0
`;

    let concurrency = args.concurrency || 100;
    let execErrs = [];
    let log = this.log;
    let self = this;
    let ui = args.ui;

    // If more than one storage instance, start a progress bar.
    if (args.storInsts.length > 1) {
        ui.barStart({
            name: 'Hotpatching ' + args.storInsts.length + ' storage insts',
            size: args.storInsts.length
        });
    }

    let exec = new oneach.mzCommandExecutor({
        scopeZones: args.storInsts.map(inst => inst.uuid),
        execMode: oneach.MZ_EM_COMMAND,
        execCommand: cmd,

        // Allow 5 minutes, which should be way longer than necessary. If this
        // isn't sufficient, then an option should be exposed to override it.
        execTimeout: 300000,
        concurrency: concurrency,
        log: self.log,

        // -- The graveyard of unused arguments
        // `mzCommandExecutor`'s draconian interface blows up if one does not
        // very carefully specify almost (!) each and every argument.
        scopeGlobalZones: false,
        scopeAllZones: false,
        scopeServices: null,
        scopeComputeNodes: null,
        execDirectory: null,
        execFile: null,
        execClobber: null,
        // `streamStatus` is "required", but only used if `dryRun=true`, which
        // is not clearly documented. We emphatically do not want this writing
        // to stderr, so we stub it out.
        streamStatus: {write: function() {}},
        dryRun: false,
        // Hardcode the same AMQP connection defaults that ../oneach/cli.js does
        // (and doesn't export).
        amqpHost: null,
        amqpPort: 5672,
        amqpLogin: 'guest',
        amqpPassword: 'guest',
        amqpTimeout: 5000,
        sdcMantaConfigFile: sdc.sdcMantaConfigPathDefault
    });

    exec.on('error', function onError(err) {
        ui.barEnd();
        cb(err);
    });

    exec.on('data', function onData(res) {
        log.debug({oneachResult: res}, 'hotpatchRebalAgents result');
        ui.barAdvance(1);

        if (res.result.exit_status !== 0) {
            execErrs.push(
                new VError(
                    {info: {res: res}},
                    'error hotpatching rebalancer-agent on storage inst ' +
                        res.zonename
                )
            );
            ui.error('Error hotpatching storage instance %s', res.zonename);
        } else {
            ui.info('Hotpatched storage instance %s', res.zonename);
        }
    });

    exec.on('end', function onEnd() {
        ui.barEnd();
        if (execErrs.length > 0) {
            cb(VError.errorFromList(execErrs));
        } else {
            cb(null);
        }
    });
};

//
// Revert rebalancer-agent hotpatches on the given storage instances.
//
// @param {Array} args.storInsts - Required. An array of objects representing
//      the storage instances (in this DC) on which to fetch rebalancer-agent
//      details. Minimally each object must have a "uuid" field. Typically these
//      are raw SAPI instance objects, e.g. from <Hotpatcher>.listStorInsts.
// @pararm {Object} args.ui - Required.
// @param {Number} args.concurrency - Optional.
// @param {Function} cb - `function (err)`.
//
// @param {Function} cb - `function (err)`.
//
Hotpatcher.prototype.unhotpatchRebalAgents = function unhotpatchRebalAgents(
    args,
    cb
) {
    assert.arrayOfObject(args.storInsts, 'args.storInsts');
    assert.object(args.ui, 'args.ui');
    assert.optionalFinite(args.concurrency, 'args.concurrency');

    let cmd = `
set -o errexit
set -o pipefail

PATH=/opt/local/sbin:/opt/local/bin:/usr/sbin:/usr/bin:/bin

function fatal {
    echo "unhotpatchRebalAgents.cmd: fatal error: $*" >&2
    exit 1
}

INSTALL_DIR=/opt/smartdc/rebalancer-agent
ORIG_INSTALL_DIR=/opt/smartdc/rebalancer-agent.orig

# Start xtrace log when it starts to get interesting.
export PS4='[\\D{%FT%TZ}] <cmd>:\${LINENO}: '
set -o xtrace

if [[ -h $INSTALL_DIR ]]; then
    echo "hotpatched, rollbacking using $ORIG_INSTALL_DIR"
elif [[ -d $INSTALL_DIR ]]; then
    echo "not hotpatched, nothing to do"
    exit 0
elif [[ ! -d $ORIG_INSTALL_DIR ]]; then
    fatal "there is no original rebalance-agent dir from which to rollback: '$ORIG_INSTALL_DIR' does not exist"
fi

# Stop the service, swap in the new.
svcadm disable -s rebalancer-agent
svccfg delete rebalancer-agent
rm -f $INSTALL_DIR
mv $ORIG_INSTALL_DIR $INSTALL_DIR

# Force a config-agent run to regenerate any config files, then start.
svcadm disable -s config-agent && svcadm enable -s config-agent
svccfg import $INSTALL_DIR/smf/manifests/rebalancer-agent.xml

# Wait for rebalancer-agent to come online (or go to maint).
loops=30
status=
while [[ "$status" != "online" ]]; do
    status=$(svcs -Ho state rebalancer-agent)
    if [[ "$status" == "online" ]]; then
        break
    elif [[ "$status" == "maintenance" ]]; then
        fatal "rebalancer-agent went to maintenance"
    fi
    sleep 1
    loops=$((loops - 1))
done
if [[ "$status" != "online" ]]; then
    fatal "timeout waiting for rebalancer-agent to come online (last status: '$status')"
fi

# Clear out any hotpatch dirs.
rm -rf $INSTALL_DIR.hotpatch-*

exit 0
`;

    let concurrency = args.concurrency || 100;
    let execErrs = [];
    let log = this.log;
    let self = this;
    let ui = args.ui;

    // If more than one storage instance, start a progress bar.
    if (args.storInsts.length > 1) {
        ui.barStart({
            name: 'Unhotpatching ' + args.storInsts.length + ' storage insts',
            size: args.storInsts.length
        });
    }

    let exec = new oneach.mzCommandExecutor({
        scopeZones: args.storInsts.map(inst => inst.uuid),
        execMode: oneach.MZ_EM_COMMAND,
        execCommand: cmd,

        // Allow 5 minutes, which should be way longer than necessary. If this
        // isn't sufficient, then an option should be exposed to override it.
        execTimeout: 300000,
        concurrency: concurrency,
        log: self.log,

        // -- The graveyard of unused arguments
        // `mzCommandExecutor`'s draconian interface blows up if one does not
        // very carefully specify almost (!) each and every argument.
        scopeGlobalZones: false,
        scopeAllZones: false,
        scopeServices: null,
        scopeComputeNodes: null,
        execDirectory: null,
        execFile: null,
        execClobber: null,
        // `streamStatus` is "required", but only used if `dryRun=true`, which
        // is not clearly documented. We emphatically do not want this writing
        // to stderr, so we stub it out.
        streamStatus: {write: function() {}},
        dryRun: false,
        // Hardcode the same AMQP connection defaults that ../oneach/cli.js does
        // (and doesn't export).
        amqpHost: null,
        amqpPort: 5672,
        amqpLogin: 'guest',
        amqpPassword: 'guest',
        amqpTimeout: 5000,
        sdcMantaConfigFile: sdc.sdcMantaConfigPathDefault
    });

    exec.on('error', function onError(err) {
        ui.barEnd();
        cb(err);
    });

    exec.on('data', function onData(res) {
        log.debug({oneachResult: res}, 'unhotpatchRebalAgents result');
        ui.barAdvance(1);

        if (res.result.exit_status !== 0) {
            execErrs.push(
                new VError(
                    {info: {res: res}},
                    'error unhotpatching rebalancer-agent on storage inst ' +
                        res.zonename
                )
            );
            ui.error('Error unhotpatching storage instance %s', res.zonename);
        } else {
            ui.info('Unhotpatched storage instance %s', res.zonename);
        }
    });

    exec.on('end', function onEnd() {
        ui.barEnd();
        if (execErrs.length > 0) {
            cb(VError.errorFromList(execErrs));
        } else {
            cb(null);
        }
    });
};

//
// List all 'mantav2-rebalancer-agent' images after the given marker (a date
// or image UUID) in the 'dev' channel of updates.tritondatacenter.com.
//
Hotpatcher.prototype.listAvailImages = function listAvailImages(marker, cb) {
    assert.optionalString(marker, 'marker');

    let listOpts = {
        channel: 'dev',
        name: 'mantav2-rebalancer-agent'
    };
    let updatesJo = this.getUpdatesJoClient();

    if (marker) {
        listOpts.marker = marker;
    }

    updatesJo.listImages(listOpts, function onImages(err, availImages) {
        if (err) {
            cb(err);
        } else {
            cb(null, availImages);
        }
    });
};

Hotpatcher.prototype.getLocalImage = function getLocalImage(uuid, cb) {
    assert.uuid(uuid, 'uuid');
    let imgapi = this.getImgapiClient();
    imgapi.getImage(uuid, cb);
};

Hotpatcher.prototype.getRemoteImage = function getRemoteImage(uuid, cb) {
    assert.uuid(uuid, 'uuid');
    let updatesJo = this.getUpdatesJoClient();
    updatesJo.getImage(uuid, {channel: 'dev'}, cb);
};

Hotpatcher.prototype.importImage = function importImage(uuid, cb) {
    assert.uuid(uuid, 'uuid');
    let imgapi = this.getImgapiClient();
    imgapi.adminImportRemoteImageAndWait(
        uuid,
        'https://updates.tritondatacenter.com',
        cb
    );
};

// ---- exports

module.exports = {
    Hotpatcher: Hotpatcher
};
