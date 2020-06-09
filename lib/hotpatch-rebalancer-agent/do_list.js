/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

/*
 * `manta-hotpatch-rebalancer-agent list ...`
 *
 * Command to list the current version of rebalancer-agent on storage instances.
 */

var format = require('util').format;
var fs = require('fs');
var path = require('path');

var assert = require('assert-plus');
var cmdln = require('cmdln');
var tabula = require('tabula');
var vasync = require('vasync');
var VError = require('verror').VError;

// XXX move import path
var clicommon = require('../mantav2-migrate/clicommon');


//
// Given an array return a string with each element JSON-stringifed separated by
// newlines.
//
function jsonStream(arr, stream) {
    stream = stream || process.stdout;

    arr.forEach(function (elem) {
        stream.write(JSON.stringify(elem) + '\n');
    });
}

function do_list(subcmd, opts, args, cb) {
    var log = this.log;
    var hotpatcher = this.hotpatcher;
    var ui = this.ui;

    if (opts.help) {
        this.do_help('help', {}, [subcmd], cb);
        return;
    } else if (args.length !== 0) {
        cb(new cmdln.UsageError('unexpected arguments'));
        return;
    }


    vasync.pipeline(
        {
            arg: {},
            funcs: [
                function getAllStorInsts(ctx, next) {
                    hotpatcher.listStorInsts(function onInst(err, insts) {
                        if (err) {
                            next(err);
                        } else if (insts.length === 0) {
                            ui.error('No rebalancer-agents are deployed.');
                            next(true);
                        } else {
                            ctx.storInsts = insts;
                            next();
                        }
                    });
                },

                function getRebalAgentInfo(ctx, next) {
                    hotpatcher.listRebalAgents(ctx.storInsts, function onInfo(err, rebalAgents) {
                        if (err) {
                            next(err);
                        } else {
                            ctx.rebalAgents = rebalAgents
                            next();
                        }
                    });
                },

                function printTable(ctx, next) {
                    // XXX perhaps include this in --long output:  MANTA_STORAGE_ID: '1.stor.nightly.joyent.us',
                    // XXX perhaps get rebalancer-agent.orig/bin/rebalancer-agent --version and show that in long output? if useful
                    // XXX want storage node image?
                    // XXX want storage node image.version (talk to IMGAPI)?
                    var rows = [];
                    for (let a of ctx.rebalAgents) {
                        let row = {

                        }
                        rows.push(row);
                    }

                    // XXX sort for json
                    if (opts.json) {
                        jsonStream(ctx.rebalAgents);
                    } else {
                        // XXX options for these, --long output, etc.
                        let columns = [
                            {
                                lookup: 'storInst.uuid',
                                name: 'STORAGE NODE'
                            },
                            {
                                lookup: 'version'
                            },
                            {
                                lookup: 'hotpatched'
                            }
                        ];
                        let sort = [];
                        tabula(ctx.rebalAgents, {
                            dottedLookup: true,
                            skipHeader: opts.H,
                            columns: columns,
                            sort: sort
                        });

                    }
                }
            ]
        },
        function finish(err) {
            if (err === true) {
                // Early abort signal.
                err = null;
            }
// XXX printing errors, this sucks:
//  [root@headnode (nightly-2) ~]# manta-hotpatch-rebalancer-agent list
//  manta-hotpatch-rebalancer-agent list: error: first of 3 errors: error gathering rebalancer-agent info on storage inst 00000000-0000-0000-0000-002590c0933c
// I think cmdln update would handle this... but might be a pain for other
// tools. Think about it.
            cb(err);
        }
    );
}

do_list.options = [
    {
        names: ['help', 'h'],
        type: 'bool',
        help: 'Show this help.'
    }
];

do_list.help = [
    'List running rebalancer-agent versions.',
    '',
    '{{usage}}',
    '',
    '{{options}}'
].join('\n');

do_list.synopses = ['{{name}} {{cmd}} [OPTIONS]'];
do_list.completionArgtypes = ['none'];

module.exports = do_list;
