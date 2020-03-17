/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var util = require('util');

var assert = require('assert-plus');
var bunyan = require('bunyan');
var cmdln = require('cmdln');
var restifyClients = require('restify-clients');
var uuidv4 = require('uuid/v4');

var clicommon = require('./clicommon');
var Migrator = require('./migrator').Migrator;
var UI = require('./ui').UI;


// ---- globals

// Identify this invocation. Used in logging and `x-request-id`s.
var RUN_ID = uuidv4();

var NAME = 'mantav2-migrate';
var LOG = bunyan.createLogger({
    name: NAME,
    level: 'warn',
    stream: process.stderr,
    serializers: restifyClients.bunyan.serializers,
    // Extra fields:
    runId: RUN_ID
});

var OPTIONS = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Print this help and exit'
    },
    {
        name: 'version',
        type: 'bool',
        help: 'Print version and exit.'
    },
    {
        names: ['verbose', 'v'],
        type: 'arrayOfBool',
        help: 'Verbose trace logging.'
    }
];


// ---- other support stuff

// Add a 'commaSepString' dashdash option type.
function parseCommaSepStringNoEmpties(option, optstr, arg) {
    // JSSTYLED
    return arg.trim().split(/\s*,\s*/g)
        .filter(function (part) { return part; });
}

cmdln.dashdash.addOptionType({
    name: 'commaSepString',
    takesArg: true,
    helpArg: 'STRING',
    parseArg: parseCommaSepStringNoEmpties
});

cmdln.dashdash.addOptionType({
    name: 'arrayOfCommaSepString',
    takesArg: true,
    helpArg: 'STRING',
    parseArg: parseCommaSepStringNoEmpties,
    array: true,
    arrayFlatten: true
});


function loadPackageInfo() {
    return JSON.parse(fs.readFileSync(__dirname + '/../../package.json'));
}

// ---- CLI

function Mantav2MigrateCli() {
    cmdln.Cmdln.call(this, {
        name: NAME,
        desc: 'Migrate a Manta from mantav1 to mantav2.',
        options: OPTIONS,
        helpOpts: {
            includeEnv: true
        },
        helpSubcmds: [
            'help',
            //'status',
            {group: ''},
            'snaplink-cleanup'
        ]
    });
}
util.inherits(Mantav2MigrateCli, cmdln.Cmdln);

Mantav2MigrateCli.prototype.init = function init(opts, args, callback) {
    this.log = LOG;
    var packageInfo = loadPackageInfo();
    var userAgent = util.format('%s/%s node/%s', NAME, packageInfo.version,
        process.versions.node);

    if (opts.verbose) {
        this.log.level('trace');
        this.log.src = true;
        this.showErrStack = true;
    }
    if (this.log.trace()) {
        var optsToLog = clicommon.objCopy(opts);
        delete optsToLog._order;
        delete optsToLog._args;
        this.log.trace({opts: optsToLog, args: args}, 'cli init');
    }

    if (opts.version) {
        console.log('%s %s', NAME, packageInfo.version);
        callback();
        return;
    }

    this.ui = new UI({log: this.log});
    this.migrator = new Migrator({
        log: this.log,
        runId: RUN_ID,
        userAgent: userAgent
    });

    cmdln.Cmdln.prototype.init.apply(this, arguments);
};


Mantav2MigrateCli.prototype.fini = function fini(subcmd, err, cb) {
    this.log.trace({err: err, subcmd: subcmd}, 'cli fini');
    if (this.migrator) {
        this.migrator.close();
    }
    cb();
};

Mantav2MigrateCli.prototype.do_completion = require('./do_completion');

//Mantav2MigrateCli.prototype.do_status = require('./do_status');
Mantav2MigrateCli.prototype.do_snaplink_cleanup = require('./do_snaplink_cleanup');


//---- mainline

function main(argv) {
    if (!argv) {
        argv = process.argv;
    }

    var cli = new Mantav2MigrateCli();
    cmdln.main(cli, {
        showCode: true,
        showNoCommandErr: true
    });
}

//---- exports

module.exports = {
    main: main
};
