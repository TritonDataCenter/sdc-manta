/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
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
    if (this._vmapi) {
        this._vmapi.close();
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
                                storInsts = insts;
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
//          -
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

// ---- exports

module.exports = {
    Hotpatcher: Hotpatcher
};
