#!/usr/bin/env node
// -*- mode: js -*-
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2014, Joyent, Inc.
 */

/*
 * manta-deploy.js: deploy a single manta instance
 */

var optimist = require('optimist');
var Logger = require('bunyan');
var deploy = require('../lib/deploy');

function usage() {
    optimist.showHelp();
}

var ARGV, bstreams, options, log;

optimist.usage('Usage:\tmanta-deploy <service>');
ARGV = optimist.options({
    l: {
        alias: 'log_file',
        describe: 'dump logs to this file (or "stdout")',
        default: '/var/log/manta-deploy.log'
    },
    n: {
        alias: 'networks',
        describe: 'networks on which to deploy'
    },
    s: {
        alias: 'server_uuid',
        describe: 'server on which to deploy'
    },
    z: {
        alias: 'shard',
        describe: 'moray shard (for moray or postgres)'
    }
}).argv;

if (ARGV._.length !== 1) {
    usage();
    process.exit(2);
}

if (ARGV.s && !/^\w{8}\-\w{4}\-\w{4}\-\w{4}\-\w{12}$/.test(ARGV.s)) {
    console.error('not a valid server uuid: "%s"', ARGV.s);
    process.exit(1);
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

if (ARGV.z !== undefined && isNaN(parseInt(ARGV.z, 10))) {
    console.error(
        'not a valid shard: "%j" ' +
            '(you may need a space between -z and the arg)',
        ARGV.z
    );
    process.exit(1);
}

options = {
    shard: ARGV.z || null,
    server_uuid: ARGV.s || null,
    networks: ARGV.n ? ARGV.n.split(',') : null
};

log = new Logger({
    name: 'manta-deploy',
    serializers: Logger.stdSerializers,
    streams: bstreams
});

deploy.deploy(options, ARGV._[0], log, function(err) {
    if (err) {
        console.error('Error: ' + err.message);
        process.exit(1);
    }

    process.exit(0);
});
