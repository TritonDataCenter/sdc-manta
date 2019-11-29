#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2018, Joyent, Inc.
 */

/*
 * manta-factoryreset.js: remove a manta installation
 */

var assert = require('assert-plus');
var async = require('async');
var common = require('../lib/common');
var exec = require('child_process').exec;
var Logger = require('bunyan');
var optimist = require('optimist');
var once = require('once');
var sdc = require('sdc-clients');
var sprintf = require('util').format;
var stdin = process.openStdin();
var vasync = require('vasync');
var verror = require('verror');

var VError = verror.VError;

// -- Globals

optimist.usage('Usage:\tmanta-factoryreset');

var ARGV = optimist.options({
    l: {
        alias: 'log_file',
        describe: 'dump logs to this file (stdout to dump to console)',
        default: '/var/log/manta-factoryreset.log'
    },
    y: {
        alias: 'skip_confirmation',
        describe: 'skip the warning/confirmation'
    }
}).argv;

function usage() {
    optimist.showHelp();
}

// -- Helpers

/*
 * Callback method for common.updateNetworkUsers.
 */
function removeUserFromNetwork(owner_uuid, network_owners, callback) {
    var uuids = [];
    var foundUser = false;

    network_owners.forEach(function(uuid) {
        if (uuid === owner_uuid) {
            foundUser = true;
        } else {
            uuids.push(uuid);
        }
    });

    if (!foundUser) {
        var e = new VError(
            {
                name: 'UserNotFoundError',
                info: {errno: 'ENOENT'}
            },
            'User %s is not an owner of this network',
            owner_uuid
        );

        callback(e);
        return;
    }

    callback(null, uuids);
}

// -- Mainline

var self = this;

var bstreams = [
    {
        level: 'debug',
        path: ARGV.l
    }
];
if (ARGV.l === 'stdout') {
    bstreams = [
        {
            level: 'debug',
            stream: process.stdout
        }
    ];
} else {
    console.error('logs at ' + ARGV.l);
}
self.log = new Logger({
    name: 'manta-factoryreset',
    serializers: Logger.stdSerializers,
    streams: bstreams
});

var POSEIDON;

var warning = [
    'WARNING: This will completely undeploy all Manta services, including ',
    'all storage data.  Are you sure you want to proceed? (y/N): '
].join('\n');

async.waterfall(
    [
        function issueWarning(cb) {
            if (ARGV.y === true) {
                return cb();
            }

            common.confirm(warning, function(proceed) {
                if (!proceed) {
                    process.stdout.write('\n');
                    console.log('Manta factory reset aborted.');
                    process.exit(1);
                }
                cb();
            });
        },

        function initClients(cb) {
            common.initSdcClients.call(self, function(err) {
                if (err) {
                    return cb(err);
                }

                return cb(null);
            });
        },

        function getPoseidon(cb) {
            var log = self.log;

            self.UFDS.getUser('poseidon', function(err, user) {
                if (err && err.name === 'ResourceNotFoundError') {
                    console.log('No manta installation found');
                    process.exit(1);
                } else if (err) {
                    log.error(err, 'failed to get poseidon user');
                    return cb(err);
                }

                POSEIDON = user;
                return cb(null);
            });
        },

        function getApplication(cb) {
            var log = self.log;
            log.info('fetching manta application from sapi');

            common.getMantaApplication.call(self, POSEIDON.uuid, function(
                err,
                app
            ) {
                if (app) {
                    self.application = app;
                }
                cb(err);
            });
        },

        function denyProduction(cb) {
            var log = self.log;
            log.info('checking stage');

            if (!self.application) {
                return cb(null);
            }

            var app = self.application;

            if (app.metadata.SIZE === 'production') {
                var m =
                    'Attempting to factory reset in a ' +
                    'production stage.  Failing...';
                log.fatal(m);
                return cb(new Error(m));
            }
            return cb(null);
        },

        function getServices(cb) {
            if (!self.application) {
                return cb(null);
            }

            var sapi = self.SAPI;
            var log = self.log;
            var app = self.application;

            sapi.getApplicationObjects(app.uuid, function(err, ret) {
                if (err) {
                    log.error(err, 'failed to list application objects');
                    return cb(err);
                }

                self.services = ret.services;
                self.instances = ret.instances;

                log.info(
                    {
                        services: self.services,
                        instances: self.instances
                    },
                    'found manta services and instances'
                );

                cb();
            });
        },

        function loadServers(cb) {
            var cnapi = self.CNAPI;
            var log = self.log;

            log.info('fetching servers from cnapi');

            cnapi.listServers({}, function(err, res) {
                if (err) {
                    log.error(err, 'failed to list servers');
                    return cb(err);
                }

                self.servers = res;
                return cb(null);
            });
        },

        function getProbeGroups(cb) {
            var amon = self.AMON;
            var log = self.log;

            log.info('fetching probe groups from amon');

            amon.listProbeGroups(POSEIDON.uuid, function(err, res) {
                if (err) {
                    log.error(err, 'failed to get amon probe groups');
                    return cb(err);
                }

                self.probeGroups = res;
                return cb(null);
            });
        },

        function getProbes(cb) {
            var amon = self.AMON;
            var log = self.log;

            log.info('fetching probes from amon');

            amon.listProbes(POSEIDON.uuid, function(err, res) {
                if (err) {
                    log.error(err, 'failed to get amon probes');
                    return cb(err);
                }

                self.probes = res;
                return cb(null);
            });
        },

        function _undeployMarlinAgents(cb) {
            if (!self.application) {
                return cb(null);
            }

            var log = self.log;
            var script = sprintf(
                '[[ -d %s ]] || exit 0; ' + '%s/tools/mragentdestroy -f',
                common.MARLIN_DIR,
                common.MARLIN_DIR
            );
            common.runOnEachMarlinNode.call(self, script, function(err) {
                if (err) {
                    log.error(err, 'failed to undeploy marlin agents');
                    cb(err);
                    return;
                }
                log.info('done undeploying marlin agents');
                cb();
            });
        },

        function _deleteInstances(cb) {
            var sapi = self.SAPI;
            var log = self.log;

            if (!self.instances) {
                return cb(null);
            }

            var uuids = [];
            Object.keys(self.instances).forEach(function(key) {
                uuids = uuids.concat(self.instances[key]);
            });
            uuids = uuids.map(function(inst) {
                return inst.uuid;
            });

            /*
             * Delete 8 instances at a time since the workflow system can
             * run 16 jobs in parallel.
             */
            async.forEachLimit(
                uuids,
                8,
                function(uuid, subcb) {
                    log.info('deleting instance %s', uuid);

                    sapi.deleteInstance(uuid, function(err) {
                        if (err) {
                            log.error(
                                err,
                                'failed to ' + 'delete instance %s',
                                uuid
                            );
                        } else {
                            log.info('deleted instance %s', uuid);
                        }

                        subcb(err);
                    });
                },
                function(err) {
                    cb(err);
                }
            );
        },

        function _deleteServices(cb) {
            var sapi = self.SAPI;
            var log = self.log;

            if (!self.services) {
                return cb(null);
            }

            var uuids = Object.keys(self.services);

            async.forEach(
                uuids,
                function(uuid, subcb) {
                    sapi.deleteService(uuid, function(err) {
                        if (err) {
                            log.error(
                                err,
                                'failed to ' + 'delete service %s',
                                uuid
                            );
                        } else {
                            log.info('deleted service %s', uuid);
                        }

                        subcb(err);
                    });
                },
                function(err) {
                    cb(err);
                }
            );
        },

        function _deleteApplications(cb) {
            if (!self.application) {
                return cb(null);
            }

            var sapi = self.SAPI;
            var log = self.log;

            var app = self.application;
            assert.object(app, 'app');

            sapi.deleteApplication(app.uuid, function(err) {
                if (err) {
                    log.error(
                        err,
                        'failed to ' + 'delete application %s',
                        app.uuid
                    );
                } else {
                    log.info('deleted application %s', app.uuid);
                }

                cb(err);
            });
        },

        function _deleteProbes(cb) {
            var amon = self.AMON;
            var log = self.log;

            if (!self.probes) {
                return cb(null);
            }

            var uuids = [];
            self.probes.forEach(function(elem, idx, ary) {
                uuids = uuids.concat(elem.uuid);
            });

            async.forEachLimit(
                uuids,
                8,
                function(uuid, subcb) {
                    log.info('deleting probe %s', uuid);

                    amon.deleteProbe(POSEIDON.uuid, uuid, function(err) {
                        if (err) {
                            log.error(
                                err,
                                'failed to ' + 'delete probe %s',
                                uuid
                            );
                        } else {
                            log.info('deleted probe %s', uuid);
                        }

                        subcb(err);
                    });
                },
                function(err) {
                    cb(err);
                }
            );
        },

        function _deleteProbeGroups(cb) {
            var amon = self.AMON;
            var log = self.log;

            if (!self.probeGroups) {
                return cb(null);
            }

            var uuids = [];
            self.probeGroups.forEach(function(elem, idx, ary) {
                uuids = uuids.concat(elem.uuid);
            });

            async.forEachLimit(
                uuids,
                8,
                function(uuid, subcb) {
                    log.info('deleting probe group %s', uuid);

                    amon.deleteProbeGroup(POSEIDON.uuid, uuid, function(err) {
                        if (err) {
                            log.error(
                                err,
                                'failed to ' + 'delete probe group %s',
                                uuid
                            );
                        } else {
                            log.info('deleted probe group %s', uuid);
                        }

                        subcb(err);
                    });
                },
                function(err) {
                    cb(err);
                }
            );
        },

        function _removePoseidonFromNetworks(cb) {
            if (!POSEIDON) {
                return cb();
            }

            var networks = ['manta', 'mantanat', 'admin'];

            vasync.forEachParallel(
                {
                    func: function(network, subcb) {
                        common.updateNetworkUsers(
                            {
                                name: network,
                                owner_uuid: POSEIDON.uuid,
                                napi: self.NAPI,
                                log: self.log,
                                action: 'remove',
                                update_func: removeUserFromNetwork
                            },
                            subcb
                        );
                    },
                    inputs: networks
                },
                function(err, results) {
                    cb(err);
                }
            );
        },

        function _removePoseidonFromOperators(cb) {
            if (!POSEIDON) {
                return cb();
            }

            var ufds = self.UFDS;
            var log = self.log;

            assert.object(POSEIDON, 'POSEIDON');
            assert.string(POSEIDON.dn, 'POSEIDON.dn');

            var entry = {
                type: 'delete',
                modification: {
                    uniquemember: POSEIDON.dn
                }
            };

            var operatorsdn = 'cn=operators, ou=groups, o=smartdc';

            log.info(
                {entry: entry, operatorsdn: operatorsdn},
                'removing poseidon from operators group'
            );

            ufds.modify(operatorsdn, entry, function(err, res) {
                if (err) {
                    log.error(
                        err,
                        'failed to remove poseidon ' + 'from operators group'
                    );
                }

                return cb(err);
            });
        },

        function _deletePoseidonKeys(cb) {
            if (!POSEIDON) {
                return cb();
            }

            var ufds = self.UFDS;
            var log = self.log;

            function deleteKey(key, subcb) {
                ufds.deleteKey(POSEIDON, key, subcb);
            }

            POSEIDON.listKeys(function(err, keys) {
                if (err) {
                    return cb(err);
                }
                if (keys.length === 0) {
                    log.info('no keys for poseidon.');
                    return cb();
                }
                log.info(
                    sprintf('removing %s key(s) for poseidon', keys.length)
                );
                vasync.forEachParallel(
                    {
                        inputs: keys,
                        func: deleteKey
                    },
                    function(err2) {
                        return cb(err2);
                    }
                );
            });
        },

        function _deletePoseidon(cb) {
            if (!POSEIDON) {
                return cb();
            }

            var ufds = self.UFDS;
            var log = self.log;

            log.info('deleting poseidon user');
            ufds.deleteUser(POSEIDON, function(err) {
                if (err) {
                    log.error(err, 'Error deleting poseidon');
                    return cb(err);
                }
                log.info('poseidon deleted');
                cb();
            });
        },

        function _deleteHashRingImages(cb) {
            var imgapi = self.IMGAPI;
            vasync.pipeline(
                {
                    funcs: [
                        function _listImages(_, _cb) {
                            var filter = {
                                name: 'manta-hash-ring'
                            };

                            imgapi.listImages(filter, {}, function(
                                err,
                                images
                            ) {
                                _.images = images;
                                return _cb(err);
                            });
                        },
                        function _deleteImages(_, _cb) {
                            _cb = once(_cb);
                            if (!_.images || _.images.length === 0) {
                                return _cb();
                            }
                            var barrier = vasync.barrier();
                            barrier.on('drain', cb);
                            _.images.forEach(function(image) {
                                barrier.start(image.uuid);
                                imgapi.deleteImage(image.uuid, {}, function(
                                    err
                                ) {
                                    if (err) {
                                        return _cb(err);
                                    }
                                    barrier.done(image.uuid);
                                });
                            });
                        }
                    ],
                    arg: {}
                },
                function(err, results) {
                    process.exit();
                    return cb(err, results);
                }
            );
        }
    ],
    function(err) {
        if (err) {
            console.error('Error: ' + err.message);
            process.exit(1);
        }

        process.exit(0);
    }
);
