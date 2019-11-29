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
 * manta-merge-config.js: shows what a merged config would look like.
 */

var assert = require('assert-plus');
var optimist = require('optimist');
var path = require('path');
var sprintf = require('util').format;
var sapi = require('sdc-clients').SAPI;

optimist.usage('Usage:\tmanta-merge-config <options> <service>');

var ARGV = optimist.options({
    s: {
        alias: 'size',
        describe: 'manta deployment size (coal, lab, or production)',
        demand: true
    }
}).argv;

var SERVICE = ARGV._[0];

function usage() {
    optimist.showHelp();
}

if (!SERVICE) {
    console.error('service is required');
    usage();
    process.exit(2);
}

if (['coal', 'lab', 'production'].indexOf(ARGV.s) === -1) {
    console.error(ARGV.s + ' is an invalid size');
    usage();
    process.exit(2);
}

// -- Mainline

var self = this;
var file = sprintf(
    '%s/../config/services/%s/service.json',
    path.dirname(__filename),
    SERVICE
);
file = path.resolve(file);
var override = file + '.' + ARGV.s;
var a = [file, override];
// Kinda weird reaching into a prototype...
sapi.prototype.readAndMergeFiles(a, function(err, o) {
    if (err && err.code === 'ENOENT') {
        console.log('ERROR: ' + file + " doesn't exist.");
        process.exit(1);
    }
    if (err) {
        console.log(err);
        process.exit(1);
    }
    console.log(JSON.stringify(o, null, 2));
});
