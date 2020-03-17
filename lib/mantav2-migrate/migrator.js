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

var assert = require('assert-plus');
var sdcClients = require('sdc-clients');
var vasync = require('vasync');
var VError = require('verror').VError;

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
    var app;
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
    let dcs = [];
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
                                    return (inst.metadata.DATACENTER = thisDcName);
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
                                .filter(nic => nic.nic_tag === 'admin')
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
                function determineIsV2(ctx, next) {
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

// ---- exports

module.exports = {
    Migrator: Migrator
};
