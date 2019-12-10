#!/usr/bin/env node
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * manta-marlin.js: deploy and configure the marlin agent
 */

var assert = require('assert-plus');
var async = require('async');
var optimist = require('optimist');

var Logger = require('bunyan');

var sprintf = require('util').format;

var common = require('../lib/common');

// -- Globals

optimist.usage('Usage:\tmanta-marlin');

var ARGV = optimist.options({
    l: {
        alias: 'log_file',
        describe: 'dump logs to this file (stdout to dump to console)',
        default: '/var/log/manta-marlin.log'
    },
    s: {
        alias: 'server_uuid',
        describe: 'server on which to deploy'
    }
}).argv;

function usage() {
    optimist.showHelp();
}

if (ARGV._.length !== 0) {
    usage();
    process.exit(1);
}

if (ARGV.s && !/^\w{8}-\w{4}-\w{4}-\w{4}-\w{12}$/.test(ARGV.s)) {
    console.error('not a valid server uuid: "%s"');
    process.exit(1);
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
    name: 'manta-marlin',
    serializers: Logger.stdSerializers,
    streams: bstreams
});

async.waterfall(
    [
        function initClients(cb) {
            common.initSdcClients.call(self, cb);
        },

        function getMantaApplication(cb) {
            var log = self.log;

            common.getMantaApplication.call(self, function(err, app) {
                if (err) {
                    log.error(err, 'failed to get manta app');
                    return cb(err);
                }

                self.application = app;
                log.debug({app: self.application}, 'found manta application');

                return cb(null);
            });
        },

        function findServerUuid(cb) {
            var cnapi = self.CNAPI;
            var log = self.log;

            if (ARGV.s) {
                cnapi.getServer(ARGV.s, function(err) {
                    if (err) {
                        log.error(
                            {
                                server_uuid: self.server_uuid,
                                error: err
                            },
                            'error fetching server info'
                        );
                        return cb(err);
                    }
                    self.server_uuid = ARGV.s;
                    return cb(null);
                });
            } else {
                common.findServerUuid.call(self, function(err, s) {
                    if (err) {
                        log.error(err, 'failed to find server uuid');
                        return cb(err);
                    }

                    self.server_uuid = s;
                    return cb(null);
                });
            }
        },

        function getOrCreateComputeId(cb) {
            assert.func(cb, 'cb');
            var log = self.log;

            common.getOrCreateComputeId.call(self, self.server_uuid, function(
                err,
                compute_id
            ) {
                if (err) {
                    log.error(err, 'unable to get or create compute_id');
                    return cb(err);
                }

                self.compute_id = compute_id;
                return cb(null);
            });
        },

        function configureAgent(cb) {
            assert.func(cb, 'cb');

            var app = self.application;
            var log = self.log;
            var message;
            var e;

            if (
                !self.compute_id ||
                !app.metadata['MANTA_SERVICE'] ||
                !app.metadata['MARLIN_MORAY_SHARD']
            ) {
                message = 'compute id, service or moray shard missing';
                log.error(
                    {
                        compute_id: self.compute_id,
                        service: app.metadata['MANTA_SERVICE'],
                        shard: app.metadata['MARLIN_MORAY_SHARD']
                    },
                    message
                );
                e = new Error(message);
                e.message = message;
                cb(e);
                return;
            }

            var cmd = sprintf('%s/tools/mragentconf', common.MARLIN_DIR);
            var argv = [];
            argv.push(cmd);
            argv.push(self.compute_id);
            argv.push(app.metadata['MANTA_SERVICE']);
            argv.push(app.metadata['MARLIN_MORAY_SHARD']);

            if (
                !app.metadata ||
                !app.metadata['ZK_SERVERS'] ||
                app.metadata['ZK_SERVERS'].length < 1
            ) {
                message =
                    'zk servers missing or empty in the manta application';
                log.error(
                    {
                        zkServers: app.metadata['ZK_SERVERS']
                    },
                    message
                );
                e = new Error(message);
                e.message = message;
                cb(e);
                return;
            }

            var zk_servers = app.metadata['ZK_SERVERS'].map(function(s) {
                return s.host;
            });
            argv = argv.concat(zk_servers);

            var script = argv.join(' ');

            common.commandExecute.call(self, self.server_uuid, script, cb);
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
