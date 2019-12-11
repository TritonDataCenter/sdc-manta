#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * manta-undeploy.js: undeploy a single manta zone
 */

var optimist = require('optimist');
var Logger = require('bunyan');
var deploy = require('../lib/deploy');

function usage() {
    optimist.showHelp();
}

function fatal(err) {
    console.error('Error: ' + err.message);
    process.exit(1);
}

var ARGV, bstreams, log, deployer;

optimist.usage('Usage:\tmanta-undeploy <instance>');
ARGV = optimist.options({
    l: {
        alias: 'log_file',
        describe: 'dump logs to this file (or "stdout")',
        default: '/var/log/manta-undeploy.log'
    }
}).argv;

if (ARGV._.length !== 1) {
    usage();
    process.exit(2);
}

if (ARGV.l === 'stdout') {
    bstreams = [
        {
            level: 'debug',
            stream: process.stdout
        }
    ];
} else {
    bstreams = [
        {
            level: 'debug',
            path: ARGV.l
        }
    ];
    console.error('logs at ' + ARGV.l);
}

log = new Logger({
    name: 'manta-undeploy',
    serializers: Logger.stdSerializers,
    streams: bstreams
});

deployer = deploy.createDeployer(log);
deployer.on('error', fatal);
deployer.on('ready', function() {
    deployer.undeploy(ARGV._[0], function(err) {
        if (err) {
            fatal(err);
        }
        deployer.close(function() {
            log.info('deployer cleaned up');
            /* normal Node exit */
        });
    });
});
