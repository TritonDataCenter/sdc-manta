/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

var fs = require('fs');
var util = require('util');

var bunyan = require('bunyan');
var cmdln = require('cmdln');
var restifyClients = require('restify-clients');
var uuidv4 = require('uuid/v4');

// XXX imports from mantav2-migrate should move up a dir
var clicommon = require('../mantav2-migrate/clicommon');
var Hotpatcher = require('./hotpatcher').Hotpatcher;
var UI = require('../mantav2-migrate/ui').UI;

// ---- globals

// Identify this invocation. Used in logging and `x-request-id`s.
var RUN_ID = uuidv4();

var NAME = 'manta-hotpatch-rebalancer-agent';
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
        type: 'bool',
        help: 'Verbose trace logging.'
    }
];

// ---- other support stuff

// Add a 'commaSepString' dashdash option type.
function parseCommaSepStringNoEmpties(_option, _optstr, arg) {
    // JSSTYLED
    return arg
        .trim()
        .split(/\s*,\s*/g)
        .filter(function(part) {
            return part;
        });
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

function HotpatchRebalAgentCli() {
    cmdln.Cmdln.call(this, {
        name: NAME,
        desc: 'Hotpatch rebalancer-agent in deployed "storage" instances.',
        options: OPTIONS,
        helpOpts: {
            includeEnv: true
        },
        helpSubcmds: ['help', {group: ''}, 'list']
    });
}
util.inherits(HotpatchRebalAgentCli, cmdln.Cmdln);

HotpatchRebalAgentCli.prototype.init = function init(opts, args, callback) {
    this.log = LOG;
    var packageInfo = loadPackageInfo();
    var userAgent = util.format(
        '%s/%s node/%s',
        NAME,
        packageInfo.version,
        process.versions.node
    );

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
        callback(false); // `false` is the signal to Cmdln that we are done.
        return;
    }

    this.ui = new UI({log: this.log});
    this.hotpatcher = new Hotpatcher({
        log: this.log,
        runId: RUN_ID,
        userAgent: userAgent
    });

    cmdln.Cmdln.prototype.init.apply(this, arguments);
};

HotpatchRebalAgentCli.prototype.fini = function fini(subcmd, err, cb) {
    this.log.trace({err: err, subcmd: subcmd}, 'cli fini');
    if (this.hotpatcher) {
        this.hotpatcher.close();
    }
    cb();
};

HotpatchRebalAgentCli.prototype.do_completion = require('./do_completion');

HotpatchRebalAgentCli.prototype.do_list = require('./do_list');

// ---- mainline

function main(argv) {
    if (!argv) {
        argv = process.argv;
    }

    var cli = new HotpatchRebalAgentCli();
    cmdln.main(cli, {
        showCode: true,
        showNoCommandErr: true
    });
}

// ---- exports

module.exports = {
    main: main
};