/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * lib/deploy.js: common functions for deploying instances of Manta zones
 */

var assert = require('assert-plus');
var async = require('async');
var fs = require('fs');
var netconfig = require('triton-netconfig');
var uuidv4 = require('uuid/v4');
var path = require('path');
var util = require('util');
var vasync = require('vasync');
var sprintf = util.format;
var common = require('./common');
var services = require('./services');
var mod_ssl = require('./ssl');
var EventEmitter = require('events').EventEmitter;
var VError = require('verror').VError;

exports.deploy = deploy;
exports.createDeployer = createDeployer;

/*
 * Storage zone deployments cannot be done concurrently, so we funnel all
 * storage zone deployments through a vasync Queue with concurrency 1.  This is
 * global to the process.  Obviously, even that's not sufficient when there are
 * multiple processes involved, but it helps in the important case of using
 * manta-adm to deploy multiple storage zones.  See MANTA-2185 for details.
 */
var dStorageQueue = vasync.queue(function(func, callback) {
    func(callback);
}, 1);

/*
 * Deploy a new instance of a Manta service.  This is a one-shot method that
 * creates a Deployer and then deploys a zone.  If you're deploying more than
 * one zone, you're better off creating your own deployer and then calling
 * "deploy" as many times as you want.  Arguments:
 *
 *     options		an object with optional properties:
 *
 *         networks	array of network names (strings) that this zone should
 *         		be provisioned with
 *
 *         server_uuid	server uuid (string) on which to provision this zone
 *
 *     svcname		the friendly name of the service to be deployed
 *     			(e.g., "nameservice", "loadbalancer", "moray", etc.)
 *
 *     log		a bunyan logger
 *
 *     callback		invoked upon completion as callback([err])
 */
function deploy(options, svcname, ilog, callback) {
    var deployer = createDeployer(ilog);
    deployer.on('error', function(err) {
        callback(err);
    });
    deployer.on('ready', function() {
        deployer.deploy(options, svcname, callback);
    });
}

/*
 * Creates a new Deployer, which can be used to deploy several Manta zones.
 * This operation initializes connections to various SDC services and emits
 * "ready" when ready, or "error" if something goes wrong.
 */
function createDeployer(log) {
    return new Deployer(log);
}

/*
 * A single Deployer instance basically just keeps its own connections to
 * various SDC services and a cached copy of the "Manta" and "SDC" applications.
 * For consumers that want to deploy several zones, this is more efficient than
 * reinitializing those connections each time.
 */
function Deployer(ilog) {
    var self = this;
    self.log = ilog;

    EventEmitter.call(this);

    async.waterfall(
        [
            function initClients(cb) {
                var log = self.log;
                log.info('initing sdc clients');
                common.initSdcClients.call(self, cb);
            },

            function getPoseidon(cb) {
                var log = self.log;
                log.info('getting poseidon user');
                getUser(self, 'poseidon', function(err, user) {
                    self.poseidon = user;
                    return cb(err);
                });
            },

            function loadSdcApplication(cb) {
                var sapi = self.SAPI;
                var log = self.log;
                var search_opts = {name: 'sdc'};
                log.info('finding "sdc" application');
                sapi.listApplications(search_opts, function(err, apps) {
                    if (err) {
                        log.error(err, 'failed to list applications');
                        return cb(err);
                    }

                    if (apps.length === 0) {
                        var msg = 'application "sdc" not found';
                        log.error(msg);
                        return cb(new Error(msg));
                    }

                    self.sdc_app = apps[0];
                    return cb(null);
                });
            },

            function getMantaApplication(cb) {
                var log = self.log;
                log.info('finding "manta" application');
                common.getMantaApplication.call(
                    self,
                    self.poseidon.uuid,
                    function(err, app) {
                        if (err) {
                            return cb(err);
                        }

                        if (!app) {
                            var msg = 'application "manta" not found';
                            log.error(msg);
                            return cb(new Error(msg));
                        }

                        self.manta_app = app;
                        return cb();
                    }
                );
            },

            function getMantaServices(cb) {
                var log, params;
                log = self.log;
                params = {
                    include_master: true,
                    application_uuid: self.manta_app['uuid']
                };
                log.info(params, 'fetching "manta" application services');
                self.SAPI.listServices(params, function(err, svcs) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    self.services = svcs;
                    cb();
                });
            },

            function checkShardConfigs(cb) {
                var log = self.log;
                var app = self.manta_app;
                var md = app.metadata;
                var missing = [];
                var message, err;

                log.info('checking shard configuration parameters');

                if (
                    typeof md[common.STORAGE_SHARD] !== 'string' ||
                    md[common.STORAGE_SHARD].length === 0
                ) {
                    missing.push(common.STORAGE_SHARD);
                }

                if (
                    !Array.isArray(md[common.INDEX_SHARDS]) ||
                    md[common.INDEX_SHARDS].length === 0
                ) {
                    missing.push(common.INDEX_SHARDS);
                }

                if (missing.length === 0) {
                    setImmediate(cb);
                    return;
                }

                message =
                    'cannot deploy zones before shards have ' +
                    'been configured (see manta-shardadm)\n';
                message +=
                    'details: metadata properties missing or ' +
                    'not valid: ' +
                    missing.join(', ');
                err = new Error(message);
                log.error(err);
                setImmediate(cb, err);
            },

            function checkHashRingConfig(cb) {
                var log = self.log;
                var app = self.manta_app;
                var md = app.metadata;
                var message, err;

                log.info('checking shard configuration parameters');

                if (
                    typeof md[common.HASH_RING_IMAGE] !== 'string' ||
                    md[common.HASH_RING_IMAGE].length === 0 ||
                    typeof md[common.HASH_RING_IMGAPI_SERVICE] !== 'string' ||
                    md[common.HASH_RING_IMGAPI_SERVICE].length === 0
                ) {
                    message =
                        'cannot deploy zones before hash ' +
                        'ring topology has been created ' +
                        '(see `manta-adm create-topology`)';
                    err = new Error(message);
                    log.error(err);
                    setImmediate(cb, err);
                } else {
                    setImmediate(cb);
                }
            }
        ],
        function(err) {
            if (err) {
                self.emit('error', err);
            } else {
                self.emit('ready');
            }
        }
    );
}

util.inherits(Deployer, EventEmitter);

Deployer.prototype.close = function(cb) {
    common.finiSdcClients.call(this, cb);
};

/*
 * Actually deploy a Manta service zone for service "svcname".  For argument
 * details, see deploy() above.
 */
Deployer.prototype.deploy = function(options, svcname, callback) {
    var allservices;
    var deployer;
    var self = this;

    // Dev Note: *Cloning* `this`s fields to `deployer` and using that below
    // is odd. The code below uses `deployer` both (a) to call its methods *and*
    // as a holder of instance-specific *state*, e.g. `deployer.zkId`.
    // This is gross is should be cleaned up: separating inst state and passing
    // that through to Deployer methods.
    deployer = {};
    for (var k in this) {
        deployer[k] = this[k];
    }

    deployer.options = options;
    deployer.zone_uuid = uuidv4();
    deployer.svcname = svcname;
    allservices = this.services;

    async.waterfall(
        [
            function getMantaService(cb) {
                var log = deployer.log;
                var svcs = allservices.filter(function(s) {
                    return s['name'] === svcname;
                });
                if (svcs.length < 1) {
                    var t =
                        'Service "%s" not found.  ' +
                        'Did you run manta-init?  If so, ' +
                        'is it a valid service?';
                    var message = sprintf(t, deployer.svcname);
                    var e = new Error(message);
                    e.message = message;
                    log.error(message);
                    return cb(e);
                }

                deployer.service = svcs[0];
                log.debug(
                    {svc: deployer.service},
                    'found %s service',
                    deployer.svcname
                );
                return cb(null);
            },

            function ensureZk(cb) {
                var app = deployer.manta_app;
                var log = deployer.log;

                if (deployer.svcname === 'nameservice') {
                    return cb(null);
                }

                log.info('ensuring ZK servers have been deployed');

                if (
                    !app.metadata ||
                    !app.metadata['ZK_SERVERS'] ||
                    app.metadata['ZK_SERVERS'].length < 1
                ) {
                    var message =
                        'zk servers missing or empty ' +
                        'in the manta application.  Has the ' +
                        'nameservice been deployed yet?';
                    log.error(
                        {
                            zkServers: app.metadata['ZK_SERVERS']
                        },
                        message
                    );
                    var e = new Error(message);
                    e.message = message;
                    return cb(e);
                }
                return cb(null);
            },

            function generateSSLCertificate(cb) {
                var log = deployer.log;
                var sapi = deployer.SAPI;
                var app = deployer.manta_app;
                var svc = deployer.service;

                if (svc.name !== 'loadbalancer') {
                    log.info(
                        'service "%s" doesn\'t need an SSL certificate',
                        svc.name
                    );
                    cb(null);
                    return;
                }

                if (svc.metadata['SSL_CERTIFICATE']) {
                    log.info('SSL certificate already present');
                    cb(null);
                    return;
                }

                log.info('generating an ssl certificate');

                var file = sprintf('/tmp/cert.%d', process.pid);
                var svc_name = app.metadata['MANTA_SERVICE'];

                async.waterfall(
                    [
                        function(subcb) {
                            mod_ssl.generateCertificate.call(
                                deployer,
                                file,
                                svc_name,
                                subcb
                            );
                        },
                        function(subcb) {
                            fs.readFile(file, 'ascii', function(err, contents) {
                                if (err) {
                                    log.error(err, 'failed to read SSL cert');
                                } else {
                                    log.debug('read SSL cert');
                                }

                                fs.unlink(file, function(_unlinkErr) {
                                    subcb(err, contents);
                                });
                            });
                        },
                        function(cert, subcb) {
                            assert.string(cert, 'cert');
                            assert.func(subcb, 'subcb');

                            var opts = {};
                            opts.metadata = {};
                            opts.metadata['SSL_CERTIFICATE'] = cert;

                            sapi.updateService(svc.uuid, opts, function(err) {
                                if (err) {
                                    log.error(err, 'failed to save SSL cert');
                                    subcb(err);
                                    return;
                                }

                                log.debug('saved SSL cert');
                                subcb(null);
                            });
                        }
                    ],
                    cb
                );
            },

            /*
             * Make sure deployer.options.server_uuid is set for the rest of
             * this waterfall.
             */
            function getServerUuid(cb) {
                if (deployer.options.server_uuid) {
                    cb(null);
                    return;
                }
                common.findServerUuid.call(deployer, function(err, id) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    deployer.options.server_uuid = id;
                    cb(null);
                });
            },

            function reserveIP(cb) {
                if (deployer.svcname !== 'nameservice') {
                    cb(null, {});
                    return;
                }

                // XXX I can really do this after it's deployed, no need
                // to reserve before provisioning.
                var log = deployer.log;
                log.info('reserving nic');
                reserveAndGetNic(
                    deployer,
                    'manta',
                    deployer.zone_uuid,
                    deployer.poseidon.uuid,
                    function(err, nic) {
                        deployer.nic = nic;
                        cb(err, nic);
                    }
                );
            },

            function updateZKServers(nic, cb) {
                var sapi = deployer.SAPI;
                var log = deployer.log;
                var zkServers = {};

                if (deployer.svcname !== 'nameservice') {
                    cb(null);
                    return;
                }

                assert.object(nic, 'nic');
                assert.string(nic.ip, 'nic.ip');
                assert.object(
                    deployer.manta_app.metadata,
                    'deployer.manta_app.metadata'
                );

                if (deployer.manta_app.metadata.ZK_SERVERS) {
                    zkServers = deployer.manta_app.metadata.ZK_SERVERS.slice();
                } else {
                    zkServers = [];
                }

                var zkId = pickNextZkId(zkServers);
                if (zkId instanceof Error) {
                    log.error(zkId);
                    cb(zkId);
                    return;
                }
                deployer.zkId = zkId;

                zkServers.push({
                    host: nic.ip,
                    port: 2181,
                    num: zkId
                });

                var len = zkServers.length;
                for (var ii = 0; ii < len - 1; ii++) {
                    delete zkServers[ii].last;
                }
                zkServers[len - 1].last = true;

                sapi.updateApplication(
                    deployer.manta_app.uuid,
                    {
                        metadata: {
                            ZK_SERVERS: zkServers
                        }
                    },
                    function(err, app) {
                        if (err) {
                            cb(
                                new VError(
                                    err,
                                    'could not update ZK_SERVERS on SAPI manta app'
                                )
                            );
                        } else {
                            log.info(
                                {ZK_SERVERS: zkServers},
                                'updated ZK_SERVERS on the SAPI "manta" app'
                            );
                            deployer.manta_app = app;

                            // Ensure a possible subsequent nameservice instance
                            // provision *in this same manta-adm process* has
                            // the updated ZK_SERVERS.
                            self.manta_app.metadata.ZK_SERVERS = zkServers;

                            cb();
                        }
                    }
                );
            },

            function ensureComputeId(cb) {
                if (deployer.svcname !== 'storage') {
                    cb(null);
                    return;
                }

                var log = deployer.log;
                var serverUuid = deployer.options.server_uuid;

                log.debug('Ensuring that the server has a compute id');

                if (!serverUuid) {
                    cb(new Error('Error, missing server UUID'));
                    return;
                }

                log.debug(
                    {serverUuid: serverUuid},
                    'server uuid for looking up compute id'
                );

                var m = 'Error getting compute id';
                common.getOrCreateComputeId.call(deployer, serverUuid, function(
                    err,
                    cid
                ) {
                    if (err) {
                        cb(err);
                        return;
                    }

                    if (!cid) {
                        var e = new Error(m);
                        e.message = m;
                        cb(e);
                        return;
                    }

                    log.debug({computeId: cid}, 'found compute id');
                    cb(null);
                });
            },

            function deployMantaInstance(cb) {
                createInstance.call(
                    null,
                    deployer,
                    deployer.manta_app,
                    deployer.service,
                    function(err, inst) {
                        if (err) {
                            cb(err);
                            return;
                        }
                        deployer.instance = inst;
                        cb(null);
                    }
                );
            },

            function configureAllowTransfer(cb) {
                /*
                 * Adds the new instance's admin IP to the cns service's
                 * allow_transfer list. Currently, this is only
                 * necessary for prometheus instances.
                 */
                if (deployer.svcname !== 'prometheus') {
                    cb(null);
                    return;
                }
                deployer.addCnsAllowTransfer(cb);
            }
        ],
        function(err) {
            callback(err, deployer.zone_uuid);
        }
    );
};

/*
 * Undeploy a SAPI instance.
 */
Deployer.prototype.undeploy = function(instance, callback) {
    var self = this;

    async.waterfall(
        [
            function getInstanceType(cb) {
                self.log.info('fetching SAPI instance', instance);
                self.SAPI.getInstance(instance, function(err, inst) {
                    var svcs;

                    if (!err) {
                        svcs = self.services.filter(function(s) {
                            return s['uuid'] === inst['service_uuid'];
                        });

                        if (svcs.length === 0) {
                            err = new VError(
                                'zone "%s" has unexpected service "%s"',
                                instance,
                                inst['service_uuid']
                            );
                        }
                    }

                    cb(err);
                });
            },

            function sapiDelete(cb) {
                self.log.info('deleting SAPI instance', instance);
                self.SAPI.deleteInstance(instance, cb);
            }
        ],
        function(err) {
            self.log.info('undeploy complete', instance);
            callback(err);
        }
    );
};

/*
 * Reprovision a SAPI instance.
 */
Deployer.prototype.reprovision = function(instance, image_uuid, callback) {
    this.SAPI.reprovisionInstance(instance, image_uuid, callback);
};

/*
 * Adds a vm's admin IP to the CNS service's list of IPs that are allowed to
 * issue AXFR/IXFR requests, if the IP is not already in the list. Currently
 * only used for prometheus instances.
 */
Deployer.prototype.addCnsAllowTransfer = function addCnsAllowTransfer(cb) {
    var self = this;

    assert.object(self.SAPI, 'self.SAPI');
    assert.object(self.VMAPI, 'self.VMAPI');
    assert.object(self.log, 'self.log');
    assert.object(self.instance, 'self.instance');

    var sapi = self.SAPI;
    var vmapi = self.VMAPI;
    var log = self.log;
    var inst = self.instance;

    function getVmIp(ctx, next) {
        var params = {uuid: inst.uuid};
        vmapi.getVm(params, function(err, vm) {
            if (err) {
                next(
                    new VError(
                        err,
                        'failed to get instance ' +
                            '"%s" of service "%s" from VMAPI',
                        inst.uuid,
                        self.svcname
                    )
                );
                return;
            }
            /*
             * Get the admin IP of the vm
             */
            var ip;
            for (var i = 0; i < vm.nics.length; i++) {
                var nic = vm.nics[i];
                if (netconfig.isNicAdmin(nic)) {
                    ip = nic.ip;
                    break;
                }
            }
            if (ip === undefined) {
                next(
                    new VError(
                        'instance "%s" of service "%s" has no admin ip',
                        inst.uuid,
                        self.svcname
                    )
                );
                return;
            }
            ctx.ip = ip;
            next();
        });
    }

    function getCnsSvc(ctx, next) {
        sapi.listServices(
            {
                name: 'cns',
                application: self.sdc_app.uuid
            },
            function gotCnsSvc(err, svcs) {
                if (err) {
                    next(err);
                    return;
                }
                assert.equal(svcs.length, 1, 'svcs.length === 1');
                ctx.cnsSvc = svcs[0];
                next();
            }
        );
    }

    /*
     * Update the cns service with the vm's admin IP, if necessary.
     */
    function updateCnsSvc(ctx, next) {
        var allow_transfer = ctx.cnsSvc.metadata.allow_transfer;
        var existingIps = allow_transfer === undefined ? [] : allow_transfer;
        if (existingIps.indexOf(ctx.ip) > -1) {
            log.info(
                {
                    svcname: self.svcname,
                    inst: inst.uuid,
                    ip: ctx.ip
                },
                'IP already in CNS allow_transfer list; not adding'
            );
            next();
            return;
        }
        existingIps.push(ctx.ip);
        sapi.updateService(
            ctx.cnsSvc.uuid,
            {
                metadata: {
                    allow_transfer: existingIps
                }
            },
            function updatedCnsSvc(err) {
                if (err) {
                    next(err);
                    return;
                }
                log.info(
                    {
                        svcname: self.svcname,
                        inst: inst.uuid,
                        ip: ctx.ip
                    },
                    'Added admin IP to CNS allow_transfer list'
                );
                next();
            }
        );
    }

    vasync.pipeline(
        {
            arg: {}, // ctx
            funcs: [getVmIp, getCnsSvc, updateCnsSvc]
        },
        cb
    );
};

// -- User management

function getUser(deployer, login, cb) {
    var ufds = deployer.UFDS;
    var log = deployer.log;

    assert.string(login, 'login');

    ufds.getUser(login, function(err, ret) {
        if (err) {
            log.error(err, 'failed to get %s', login);
        }
        return cb(err, ret);
    });
}

// -- Network management

function reserveAndGetNic(deployer, name, zone_uuid, owner_uuid, cb) {
    var log = deployer.log;
    var napi = deployer.NAPI;

    assert.string(name, 'name');
    assert.string(zone_uuid, 'zone_uuid');
    assert.string(owner_uuid, 'owner_uuid');

    var opts = {
        belongs_to_uuid: zone_uuid,
        owner_uuid: owner_uuid,
        belongs_to_type: 'zone'
    };

    log.info({opts: opts}, 'provisioning NIC');

    async.waterfall(
        [
            function getNicTags(subcb) {
                common.getServerNicTags.call(deployer, function(err, tags) {
                    if (err) {
                        subcb(err);
                        return;
                    }
                    /*
                     * XXX: This case should never happen.  If it
                     * does, we should bomb out early.
                     */
                    if (!tags.online || tags.online.length < 1) {
                        var msg = 'No nic tags available';
                        log.error({tags: tags}, msg);
                        subcb(new Error(msg));
                        return;
                    }
                    subcb(null, tags.online);
                });
            },
            function checkNetworkPool(stags, subcb) {
                napi.listNetworkPools({name: name}, function(err, pools) {
                    if (err) {
                        subcb(err);
                        return;
                    }

                    if (!pools || pools.length < 1) {
                        subcb(null, null);
                        return;
                    }

                    if (pools && pools.length > 1) {
                        log.warn(
                            {pools: pools},
                            'Skipping network pool check for ' +
                                'NIC reservation, multiple pools ' +
                                'with the same name.'
                        );
                        subcb(null, null);
                        return;
                    }

                    /*
                     * We only look for the first matching nictag.
                     * If there were two matches that would be a
                     * configuration error.  Log it and move on.
                     */
                    var ptags = pools[0].nic_tags_present;
                    var final_tags = ptags.filter(function(t) {
                        return stags.indexOf(t) !== -1;
                    });

                    if (final_tags.length < 1) {
                        log.debug(
                            'No matching pool nictags ' +
                                'found for network name %s',
                            name
                        );
                        subcb(null, null);
                        return;
                    }

                    if (final_tags.length > 1) {
                        log.warn(
                            {tags: final_tags},
                            'More ' +
                                'than one matching nictag found. ' +
                                'using %s',
                            final_tags[0]
                        );
                    }

                    log.debug('Found matching pool nictag %s', final_tags[0]);

                    opts.nic_tag = final_tags[0];
                    subcb(null, pools[0].uuid);
                    return;
                });
            },
            function checkNetworks(network_uuid, subcb) {
                if (network_uuid) {
                    subcb(null, network_uuid);
                    return;
                }

                napi.listNetworks({name: name}, function(err, networks) {
                    if (err) {
                        log.error(err, 'failed to list networks');
                        return subcb(err);
                    }

                    log.debug({network: networks[0]}, 'found network %s', name);

                    return subcb(null, networks[0].uuid);
                });
            },
            function provisionNic(network_uuid, subcb) {
                napi.provisionNic(network_uuid, opts, function(err, nic) {
                    if (err) {
                        log.error(err, 'failed to provision NIC');
                        return cb(err);
                    }

                    log.info({nic: nic}, 'provisioned NIC');

                    return subcb(null, nic);
                });
            }
        ],
        cb
    );
}

// -- SAPI functions

function createInstance(deployer, app, svc, cb) {
    var sapi = deployer.SAPI;
    var log = deployer.log;

    assert.string(
        deployer.config.datacenter_name,
        'deployer.config.datacenter_name'
    );

    assert.object(app, 'app');
    assert.object(app.metadata, 'app.metadata');
    assert.string(app.metadata.REGION, 'app.metadata.REGION');
    assert.string(app.metadata.DNS_DOMAIN, 'app.metadata.DNS_DOMAIN');

    assert.object(svc, 'svc');
    assert.string(svc.name, 'svc.name');
    assert.string(svc.uuid, 'svc.uuid');

    var inst_uuid = deployer.zone_uuid ? deployer.zone_uuid : uuidv4();

    var params = {};

    /*
     * Traditionally we've used numeric shards (e.g. 1.moray, 2.moray, etc.)
     * but there's no reason they have to be numbers.  We could have
     * 1-marlin.moray, marlin.moray, or anything similar.
     */
    var shard = '1';
    if (deployer.options.shard) {
        shard = deployer.options.shard;
    }

    /*
     * The root of all service hostnames is formed from the application's
     * region and DNS domain.
     */
    var service_root = sprintf(
        '%s.%s',
        app.metadata.REGION,
        app.metadata.DNS_DOMAIN
    );
    var service_name = sprintf('%s.%s', deployer.svcname, service_root);

    params.alias = service_name + '-' + inst_uuid.substr(0, 8);

    /*
     * Prefix with the shard for things that are shardable...
     */
    if (services.serviceIsSharded(deployer.svcname)) {
        params.alias = shard + '.' + params.alias;
    }

    params.tags = {};
    params.tags.manta_role = svc.name;

    if (deployer.options.server_uuid) {
        params.server_uuid = deployer.options.server_uuid;
    }

    if (deployer.options.image_uuid) {
        params.image_uuid = deployer.options.image_uuid;
    }

    if (deployer.options.networks) {
        var networks = [];
        deployer.options.networks.forEach(function(token) {
            networks.push({uuid: token});
        });
        params.networks = networks;
    }

    var metadata = {};
    metadata.DATACENTER = deployer.config.datacenter_name;
    metadata.SERVICE_NAME = service_name;
    metadata.SHARD = shard;

    if (deployer.svcname === 'nameservice') {
        assert.number(deployer.zkId, 'deployer.zkId');
        metadata.ZK_ID = deployer.zkId;
    }

    if (deployer.svcname === 'postgres') {
        metadata.SERVICE_NAME = sprintf('%s.moray.%s', shard, service_root);
        metadata.MANATEE_SHARD_PATH = sprintf(
            '/manatee/%s',
            metadata.SERVICE_NAME
        );
    }

    if (deployer.svcname === 'moray') {
        metadata.SERVICE_NAME = sprintf('%s.moray.%s', shard, service_root);
    }

    if (deployer.svcname === 'buckets-postgres') {
        metadata.SERVICE_NAME = sprintf(
            '%s.buckets-mdapi.%s',
            shard,
            service_root
        );
        metadata.MANATEE_SHARD_PATH = sprintf(
            '/manatee/%s',
            metadata.SERVICE_NAME
        );
    }

    if (deployer.svcname === 'buckets-mdapi') {
        metadata.SERVICE_NAME = sprintf(
            '%s.buckets-mdapi.%s',
            shard,
            service_root
        );
    }

    if (deployer.svcname === 'storage') {
        metadata.SERVICE_NAME = sprintf('stor.%s', service_root);
    }

    if (deployer.svcname === 'webapi' || deployer.svcname === 'loadbalancer') {
        metadata.SERVICE_NAME = app.metadata['MANTA_SERVICE'];
    }

    /*
     * This zone should get its configuration the local (i.e. same
     * datacenter) SAPI instance, as well as use the local UFDS instance.
     */
    var config = deployer.config;
    metadata['SAPI_URL'] = config.sapi.url;
    metadata['UFDS_URL'] = config.ufds.url;
    metadata['UFDS_ROOT_DN'] = config.ufds.bindDN;
    metadata['UFDS_ROOT_PW'] = config.ufds.bindPassword;
    metadata['SDC_NAMESERVERS'] = deployer.sdc_app.metadata.ZK_SERVERS;

    var queuecb;

    async.waterfall(
        [
            function(subcb) {
                if (svc.name !== 'storage') {
                    subcb(null);
                    return;
                }

                log.debug(
                    'putting "storage" zone provision for ' +
                        '"%s" into the queue',
                    inst_uuid
                );
                dStorageQueue.push(function(_queuecb) {
                    /*
                     * When we reach here, we're the only "storage"
                     * zone deployment that's going on right now.
                     * Save the queue callback so that we can invoke
                     * it when we finish deploying to free up the
                     * queue for someone else.
                     */
                    queuecb = _queuecb;
                    log.debug(
                        'dequeueing "storage" zone provision for "%s"',
                        inst_uuid
                    );
                    subcb();
                });
            },
            function(subcb) {
                if (svc.name !== 'storage') {
                    subcb(null);
                    return;
                }

                /*
                 * The manta_storage_id should be the next available
                 * number.
                 */
                var opts = {};
                opts.service_uuid = svc.uuid;
                opts.include_master = true;

                log.info('finding next manta_storage_id');

                sapi.listInstances(opts, function(err, insts) {
                    if (err) {
                        log.error(err, 'failed to list storage instances');
                        subcb(err);
                        return;
                    }

                    /*
                     * Find the highest-numbered storage id and pick
                     * the next one.
                     */
                    var mStorageId = pickNextStorageId(
                        insts,
                        metadata.SERVICE_NAME
                    );
                    if (mStorageId instanceof Error) {
                        log.error(err);
                        subcb(err);
                        return;
                    }

                    metadata.MANTA_STORAGE_ID = mStorageId;
                    params.tags.manta_storage_id = mStorageId;
                    subcb();
                });
            },
            function(subcb) {
                log.info('locating user script');

                var file = sprintf(
                    '%s/../scripts/user-script.sh',
                    path.dirname(__filename)
                );
                file = path.resolve(file);

                fs.readFile(file, 'ascii', function(err, contents) {
                    if (err && err['code'] === 'ENOENT') {
                        log.debug('no user script');
                    } else if (err) {
                        log.error(err, 'failed to read user script');
                        subcb(err);
                        return;
                    } else {
                        metadata['user-script'] = contents;
                        log.debug('read user script from %s', file);
                    }

                    subcb(null);
                });
            },
            function(subcb) {
                var opts = {};
                opts.params = params;
                opts.metadata = metadata;
                opts.uuid = inst_uuid;
                opts.master = true;

                log.info({opts: opts}, 'creating instance');

                sapi.createInstance(svc.uuid, opts, function(err, inst) {
                    if (err) {
                        log.error(
                            {err: err, service: svc.uuid, createOpts: opts},
                            'failed to create instance'
                        );
                        subcb(err);
                        return;
                    }

                    log.info({inst: inst}, 'created instance');

                    subcb(null, inst);
                });
            }
        ],
        function() {
            if (queuecb) {
                log.debug(
                    'done with "storage" zone provision for "%s"',
                    inst_uuid
                );
                setTimeout(queuecb, 0);
            }

            cb.apply(null, Array.prototype.slice.call(arguments));
        }
    );
}

/*
 * Given a list of SAPI instances for storage nodes, return an unused Manta
 * storage id.  If we're at all unsure, we return an error rather than
 * potentially returning a conflicting name.
 */
function pickNextStorageId(instances, svcname) {
    var max, inst, instname, numpart;
    var i, p, n;
    var err = null;

    max = 0;
    for (i = 0; i < instances.length; i++) {
        inst = instances[i];
        instname = inst.metadata.MANTA_STORAGE_ID;

        if (typeof instname !== 'string') {
            err = new VError(
                'instance "%s": missing or ' +
                    'invalid MANTA_STORAGE_ID metadata',
                inst.uuid
            );
            break;
        }

        p = instname.indexOf('.' + svcname);
        if (p === -1 || p === 0) {
            err = new VError(
                'instance "%s": instance name ' +
                    '("%s") does not contain expected suffix (".%s")',
                inst.uuid,
                instname,
                svcname
            );
            break;
        }

        numpart = instname.substr(0, p);
        n = parseInt(numpart, 10);
        if (isNaN(n) || n < 1) {
            err = new VError(
                'instance "%s": instance name ' +
                    '("%s") does not start with a positive integer',
                inst.uuid,
                instname
            );
            break;
        }

        max = Math.max(max, n);
    }

    if (err !== null) {
        return new VError(err, 'failed to allocate MANTA_STORAGE_ID');
    }

    return sprintf('%d.%s', max + 1, svcname);
}

function pickNextZkId(servers) {
    var max, err, server;

    max = 0;
    err = null;

    for (var i = 0; i < servers.length; i++) {
        server = servers[i];
        if (
            typeof server.num !== 'number' ||
            server.num % 1 !== 0 ||
            server.num < 1
        ) {
            err = new VError(
                'ZK_SERVERS[%d].num ("%s") is not a positive integer',
                i,
                server.num
            );
            break;
        }
        max = Math.max(max, server.num);
    }

    if (err !== null) {
        return new VError(err, 'failed to allocate new ZK_ID');
    }

    return max + 1;
}
